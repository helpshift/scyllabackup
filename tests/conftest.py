import pytest
import socket
import os
from time import sleep
import logging
import datetime

import sh
import boto3
from azure.storage.blob import BlockBlobService

from spongeblob.retriable_storage import RetriableStorage
from scyllabackup.snapshot import Snapshot

logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption("--no-docker", action="store_true", default=False,
                     help=("Don't test with docker. Requires setting "
                           "up storage credential env variables"))
    parser.addoption("--provider", action="store", default="s3",
                     help="list of providers to test against")


@pytest.fixture(scope="session")
def test_with_docker(request):
    return not request.config.getoption("--no-docker")


@pytest.fixture(scope="session")
def test_provider(request):
    return request.config.getoption("--provider")


@pytest.fixture(scope="session")
def test_data(test_with_docker, test_provider):
    test_data = {
        'providers': [test_provider],
        'provider': test_provider,
        'prefix': 'pytest_spongeblob',
        'env_keys': ['WABS_ACCOUNT_NAME',
                     'WABS_CONTAINER_NAME',
                     'WABS_SAS_TOKEN',
                     'S3_AWS_KEY',
                     'S3_AWS_SECRET',
                     'S3_BUCKET_NAME']
    }
    test_creds = {}
    if test_with_docker:
        test_creds['s3'] = {'aws_key': 'test',
                            'aws_secret': 'test',
                            'bucket_name': 'test'}
        test_creds['wabs'] = {'account_name': 'devstoreaccount1',
                              'container_name': 'test',
                              'sas_token': 'test'}
    else:
        for env_key in test_data['env_keys']:
            provider, key = (v.lower() for v in env_key.split('_', 1))
            if provider in test_data['providers']:
                try:
                    test_creds[provider][key] = os.environ[env_key]
                except KeyError:
                    raise KeyError('Define Environment Key {0} for testing'
                                   .format(env_key))
    test_data['creds'] = test_creds
    return test_data


def is_open(ip, port):
    "Utility function to check if a port is up"
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip, int(port)))
        sleep(1)
        s.send('test')
        s.shutdown(2)
        return True
    except (socket.error, socket.timeout):
        return False


@pytest.fixture(scope='session')
def scylla_test_dir(tmpdir_factory):
    return tmpdir_factory.mktemp('scylla_test_dir')


@pytest.fixture(scope='session')
def scylla_data_symlink(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir),
                        'tests', './.scylla_data_dir')


@pytest.fixture(scope='session')
def scylla_data_symlink2(pytestconfig):
    return os.path.join(str(pytestconfig.rootdir),
                        'tests', './.scylla_data_dir2')


@pytest.fixture(scope='session')
def scylla_data_dir(scylla_test_dir):
    f = scylla_test_dir.mkdir('scylla_data_dir')
    return str(f)


@pytest.fixture(scope='session')
def scylla_data_dir2(scylla_test_dir):
    f = scylla_test_dir.mkdir('scylla_data_dir2')
    return str(f)


@pytest.fixture(scope='session')
def docker_compose_file(pytestconfig, scylla_data_dir, scylla_data_symlink,
                        scylla_data_dir2, scylla_data_symlink2):
    sh.ln('-snf', scylla_data_dir, scylla_data_symlink)
    sh.ln('-snf', scylla_data_dir2, scylla_data_symlink2)
    return os.path.join(
        str(pytestconfig.rootdir),
        'tests',
        'docker-compose.yml'
    )


@pytest.fixture(scope='session')
def blob_services(docker_ip, docker_services):
    service_ports = {provider: docker_services.port_for(provider, port)
                     for provider, port in (('s3', 8000),
                                            ('wabs', 10000),
                                            ('scylla', 9042))}

    for provider, port in service_ports.items():
        docker_services.wait_until_responsive(
            timeout=30.0, pause=0.1,
            check=lambda: is_open(docker_ip, port))

    urls = {provider: "http://{0}:{1}".format(docker_ip, port)
            for provider, port in service_ports.items()}
    return urls


@pytest.fixture(scope='session')
def storage_client(blob_services, request, test_with_docker, test_data, test_provider):
    test_creds = test_data['creds']
    client = RetriableStorage(test_provider, **test_creds[test_provider])

    if test_with_docker:
        if test_provider == 's3':
            client._storage.client = boto3.client('s3',
                                                  aws_access_key_id=test_creds['s3']['aws_key'],
                                                  aws_secret_access_key=test_creds['s3']['aws_secret'],
                                                  endpoint_url=blob_services['s3'])

            client._storage.client.create_bucket(Bucket=test_creds['s3']['bucket_name'])
        if test_provider == 'wabs':
            client._storage.client = BlockBlobService(account_name=test_creds['wabs']['account_name'],
                                                      sas_token=test_creds['wabs']['sas_token'],
                                                      is_emulated=True)
            client._storage.client.create_container(test_creds['wabs']['container_name'])
    return client


@pytest.fixture(scope='session')
def sql_db_file(scylla_test_dir):
    f = scylla_test_dir.join('scylla_data_dir', 'test.db')
    return str(f)


@pytest.fixture(scope='session')
def sql_restore_db_file(scylla_test_dir):
    f = scylla_test_dir.join('scylla_data_dir2', 'restore.db')
    return str(f)


@pytest.fixture(scope='session')
def scylla_restore_dir(scylla_test_dir):
    f = scylla_test_dir.mkdir('scylla_data_dir2/restore_scylla')
    return str(f)


@pytest.fixture(scope='session')
def cql_restore_file(scylla_restore_dir):
    return os.path.join(scylla_restore_dir, 'restore.cql')


@pytest.fixture(scope='session')
def cql_restore_file_in_docker(scylla_restore_dir, cql_restore_file):
    return os.path.join('/var/lib/scylla/',
                        os.path.basename(scylla_restore_dir),
                        os.path.basename(cql_restore_file))


@pytest.fixture(scope='session')
def snapshot_one():
    return datetime.datetime.now().strftime('%s')


@pytest.fixture(scope='session')
def snapshot_two():
    sleep(5)
    return datetime.datetime.now().strftime('%s')


@pytest.fixture(scope='session')
def snapshot_three():
    sleep(5)
    return datetime.datetime.now().strftime('%s')


def snapshotter_object(docker_compose_file, docker_compose_project_name,
                       scylla_data_symlink, sql_db_file, storage_client,
                       docker_service_name):

    scylla_docker_cmd = ['-f', docker_compose_file, '-p',
                         docker_compose_project_name, 'exec',
                         '-T', docker_service_name]

    nodetool_cmd = scylla_docker_cmd + ['nodetool']
    cqlsh_cmd = scylla_docker_cmd + ['cqlsh']

    obj = Snapshot(scylla_data_symlink + '/data/',
                   sql_db_file,
                   storage_obj=storage_client,
                   nodetool_path='docker-compose',
                   cqlsh_path='docker-compose')

    # override cli tools for docker
    obj.nodetool = sh.Command('docker-compose').bake(*nodetool_cmd)
    obj.cqlsh = sh.Command('docker-compose').bake(*cqlsh_cmd)

    return obj


@pytest.fixture(scope='session')
def snapshotter(docker_compose_file, docker_compose_project_name,
                scylla_data_symlink, sql_db_file, storage_client):
    return snapshotter_object(docker_compose_file, docker_compose_project_name,
                              scylla_data_symlink, sql_db_file, storage_client,
                              'scylla')


@pytest.fixture(scope='session')
def restore_snapshotter(docker_compose_file, docker_compose_project_name,
                        scylla_data_symlink2, sql_restore_db_file, storage_client):
    return snapshotter_object(docker_compose_file, docker_compose_project_name,
                              scylla_data_symlink2, sql_restore_db_file,
                              storage_client, 'scylla_restore')
