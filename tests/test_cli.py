from scyllabackup.cli import parse_args
import os
import pytest
from datetime import datetime
import json
import sh
from conftest import is_open


@pytest.fixture(scope='module')
def cli_common_args(scylla_data_dir, sql_db_file):
    return ['--prefix', 'testscyllabackup',
            '--path', scylla_data_dir,
            '--db', sql_db_file,
            '--nodetool-path', '/bin/ls',
            '--cqlsh-path', '/bin/ls',
            '--provider', 'wabs',
            '--wabs-container-name', 'test',
            '--wabs-account-name', 'test',
            '--wabs-sas', ' test']


def test_cli_take_snapshot(storage_client, snapshotter, cli_common_args):
    cql = snapshotter.cqlsh.bake("-e")
    cql("CREATE KEYSPACE Excelsior WITH replication"
        " = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    cql("CREATE TABLE Excelsior.test "
        "(pk int, t int, v text, s text static, PRIMARY KEY (pk, t));")
    cql("INSERT INTO Excelsior.test(pk, t, v, s) "
        "VALUES (0, 0, 'val0', 'static0');")
    cql("INSERT INTO Excelsior.test(pk, t, v, s) "
        "VALUES (1, 0, 'val1', 'static1');")
    cli = parse_args(['take'] + cli_common_args)
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)


def test_cli_list_snapshot(storage_client, snapshotter, cli_common_args,
                           capsys):
    cli = parse_args(['list'] + cli_common_args)
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)
    out = capsys.readouterr().out
    for line in out.splitlines():
        assert line.startswith('Found Snapshot') is True
    assert len(out.splitlines()) == 1


def test_cli_download_snapshot(storage_client, snapshotter, cli_common_args,
                               capsys, scylla_restore_dir, cql_restore_file):
    cli = parse_args(['download'] + cli_common_args +
                     ['--download-dir', scylla_restore_dir,
                      '--keyspace', 'excelsior',
                      '--schema', cql_restore_file,
                      '--latest-before', datetime.now().strftime("%s")])
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)

    excelsior_dir = os.path.join(scylla_restore_dir, 'excelsior')
    assert os.path.isdir(excelsior_dir) is True
    assert os.path.isfile(cql_restore_file) is True
    assert os.path.getsize(cql_restore_file) > 0


def test_cli_download_db(storage_client, snapshotter, cli_common_args,
                         sql_restore_db_file):
    cli = parse_args(['download_db'] + cli_common_args + [sql_restore_db_file])
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)
    assert os.path.isfile(sql_restore_db_file) is True


def test_cli_list_restored_db(storage_client, snapshotter, cli_common_args,
                              sql_restore_db_file, capsys):
    cli = parse_args(['list'] + cli_common_args +
                     ['--db', sql_restore_db_file])
    cli.func(cli)
    out = capsys.readouterr().out
    for line in out.splitlines():
        assert line.startswith('Found Snapshot') is True
    assert len(out.splitlines()) == 1


def test_cli_restore_schema(storage_client, restore_snapshotter,
                            cli_common_args, cql_restore_file_in_docker):
    cli = parse_args(['restore_schema'] + cli_common_args +
                     ['-f', cql_restore_file_in_docker])
    cli.storage = storage_client
    cli.snapshotter = restore_snapshotter
    cli.func(cli)
    cli.snapshotter.cqlsh('-e', 'SELECT * FROM excelsior.test;')


def test_cli_get_restore_mapping(storage_client, restore_snapshotter,
                                 cli_common_args, scylla_restore_dir,
                                 cql_restore_file_in_docker,
                                 cli_restore_mapping_file):
    cli = parse_args(['get_restore_mapping'] + cli_common_args +
                     ['--restore-path', scylla_restore_dir,
                      '--keyspace', 'excelsior',
                      cli_restore_mapping_file])
    cli.storage = storage_client
    cli.snapshotter = restore_snapshotter
    cli.func(cli)
    with open(cli_restore_mapping_file, 'r') as f:
        mapping = json.load(f)

    assert len(mapping) is 1


def test_cli_restore_snapshot(docker_compose_file, docker_compose_project_name,
                              restore_snapshotter, cli_common_args,
                              scylla_restore_dir, cli_restore_mapping_file):

    cli = parse_args(['restore'] + cli_common_args +
                     ['--restore-path', scylla_restore_dir,
                      '--restore-mapping-file', cli_restore_mapping_file])
    cli.snapshotter = restore_snapshotter

    docker_compose_args = ['-f', docker_compose_file, '-p',
                           docker_compose_project_name]
    docker_compose = sh.Command('docker-compose').bake(*docker_compose_args)
    docker_compose('stop', 'scylla_restore')
    cli.func(cli)
    docker_compose('start', 'scylla_restore')
    wait_for_port = (docker_compose('port', 'scylla_restore', '9042').
                     split(":")[1])
    # Wait for the scylla restore container to start
    for i in range(30):
        if is_open('127.0.0.1', wait_for_port):
            break

    out = docker_compose('exec', '-T', 'scylla_restore', 'cqlsh',
                         '-e', 'select * from excelsior.test;')
    data_lines = [line for line in out.splitlines() if 'static' in line]
    assert len(data_lines) is 2


def test_cli_delete_snapshot(storage_client, snapshotter, cli_common_args,
                             capsys, scylla_restore_dir):
    cli = parse_args(['delete_older_than'] + cli_common_args + ["0"])
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)
    cli = parse_args(['list'] + cli_common_args)
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)
    out = capsys.readouterr().out
    assert len(out.splitlines()) == 0
