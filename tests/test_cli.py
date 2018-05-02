from scyllabackup.cli import parse_args
import os
import pytest


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
        "VALUES (0, 0, 'val1', 'static1');")
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
                               capsys, scylla_restore_dir):
    cli = parse_args(['list'] + cli_common_args)
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)
    found_snapshot = capsys.readouterr().out.split("'")[1]
    cql_restore_file = os.path.join(scylla_restore_dir, 'restore.cql')
    cli = parse_args(['download'] + cli_common_args +
                     ['--download-dir', scylla_restore_dir,
                      '--keyspace', 'excelsior',
                      '--schema', cql_restore_file,
                      '--snapshot', found_snapshot])
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)

    excelsior_dir = os.path.join(scylla_restore_dir, 'excelsior')
    assert os.path.isdir(excelsior_dir) is True
    assert os.path.isfile(cql_restore_file) is True
    assert os.path.getsize(cql_restore_file) > 0


def test_cli_download_db(storage_client, snapshotter, cli_common_args,
                         capsys, scylla_restore_dir):
    db_restore_file = os.path.join(scylla_restore_dir,
                                   'db_restore.db')
    cli = parse_args(['download_db'] + cli_common_args + [db_restore_file])
    cli.storage = storage_client
    cli.snapshotter = snapshotter
    cli.func(cli)
    assert os.path.isfile(db_restore_file) is True


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


def test_list_restored_db(storage_client, snapshotter, cli_common_args,
                          scylla_restore_dir, capsys):
    db_restore_file = os.path.join(scylla_restore_dir,
                                   'db_restore.db')
    cli = parse_args(['list'] + cli_common_args +
                     ['--db', db_restore_file])
    cli.func(cli)
    out = capsys.readouterr().out
    for line in out.splitlines():
        assert line.startswith('Found Snapshot') is True
    assert len(out.splitlines()) == 1
