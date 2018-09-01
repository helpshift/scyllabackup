import os
import sys
from datetime import datetime, timedelta
import logging
import configargparse
from . import logger
from .snapshot import Snapshot
from spongeblob.retriable_storage import RetriableStorage
from botocore.client import Config
import filelock
import json


def make_storage(args):
    storage_args = {key.split('_', 1)[1]: value
                    for key, value in args.__dict__.items()
                    if key.startswith(args.provider)}

    if args.provider == 's3':
        # modify storage client max_pool_connections
        storage_args['boto_config'] = Config(connect_timeout=60,
                                             read_timeout=60,
                                             max_pool_connections=100)

    return RetriableStorage(args.provider, **storage_args)


def make_snapshotter(args):
    return Snapshot(scylla_data_dir=args.path,
                    db_path=args.db,
                    storage_obj=args.storage,
                    nodetool_path=args.nodetool_path,
                    cqlsh_path=args.cqlsh_path,
                    cqlsh_host=args.cqlsh_host,
                    cqlsh_port=args.cqlsh_port,
                    prefix=args.prefix,
                    max_workers=args.max_workers)


def take_snapshot(args):
    snapshot_name = datetime.now().strftime('%s')
    logger.info("Starting snapshot {0}".format(snapshot_name))
    args.snapshotter.nodetool_take_snapshot(snapshot_name)
    args.snapshotter.upload_snapshot(snapshot_name)
    args.snapshotter.nodetool_delete_snapshot(snapshot_name)


def list_snapshots(args):
    logger.info("Finding snapshots from database")
    if args.latest_before:
        snapshot_list = (args.snapshotter.db.
                         find_snapshots_before_epoch(args.latest_before,
                                                     count=5))
    else:
        snapshot_list = args.snapshotter.db.find_snapshots()
    for snapshot in snapshot_list:
        print("Found Snapshot '{0}' "
              "taken at {1}".format(snapshot,
                                    datetime.fromtimestamp(
                                             int(snapshot)).isoformat()))


def download_snapshot(args):
    if len(os.listdir(args.download_dir)) > 0:
        error_msg = ("Download directory {0} "
                     "not empty".format(args.download_dir))
        error_code = 66
    elif os.path.exists(args.schema):
        error_msg = "Schema file {0} already exists".format(args.schema)
        error_code = 17
    else:
        error_msg = None

    if error_msg:
        logger.error(error_msg + ", can't download")
        raise OSError(error_code, error_msg)

    if args.latest_before:
        download_snapshot = next(args.snapshotter.db.
                                 find_snapshots_before_epoch(
                                     args.latest_before, count=1))
    else:
        download_snapshot = args.snapshot

    logger.info("Downloading files for snapshot {0}".format(download_snapshot))
    args.snapshotter.download_snapshot(path=args.download_dir,
                                       snapshot_name=download_snapshot,
                                       keyspace_name=args.keyspace)

    schema = args.snapshotter.db.find_snapshot_schema(download_snapshot)
    if schema:
        logger.info("Downloading schema file for snapshot "
                    "{0} at {1}".format(download_snapshot, args.schema))
        with open(args.schema, 'w+') as f:
            f.write(schema)


def verify_snapshot(args):
    logger.info("Verifying snapshot {0}".format(args.snapshot))
    success = args.snapshotter.verify_snapshot(snapshot_name=args.snapshot)
    if success:
        logger.info("All files exist remotely")
    else:
        logger.error("Some remote files don't exist")
        sys.exit(2)


def delete_older_than_snapshot(args):
    ts = (datetime.now() - timedelta(days=args.days)).strftime("%s")
    logger.info("Deleting snapshot files for "
                "snapshot older than {0}".format(args.days))
    args.snapshotter.delete_snapshot(ts)


def download_db(args):
    if os.path.exists(args.db_download_path):
        error_msg = ("DB file already exists "
                     "at path {0}".format(args.db_download_path))
        logger.error(error_msg + ", can't download")
        raise OSError(17, error_msg)
    args.snapshotter.download_db(args.db_download_path)


def restore_schema_in_scylladb(args):
    if (args.force or
        raw_input('Do you want to restore schema file {0} in scylladb [y/n]?'.
                  format(args.schema_file))[0].lower() == 'y'):
        args.snapshotter.restore_schema(args.schema_file)
    else:
        logger.error("Not restoring schema file")
        sys.exit(2)


def get_restore_mapping(args):
    if not os.path.isdir(os.path.join(args.restore_path, args.keyspace)):
        logger.error("Restore data dir specified doesn't exist for "
                     "specified keyspace: {0}".format(args.keyspace))
        sys.exit(2)

    if os.path.isfile(args.restore_mapping_file):
        logger.error("Restore mapping file {0} already "
                     "exists".format(args.restore_mapping_file))
        sys.exit(2)
    with open(args.restore_mapping_file, 'w+') as f:
        f.write(json.dumps(args.snapshotter.
                           restore_snapshot_mapping(args.restore_path,
                                                    args.keyspace)))


def restore_snapshot(args):
    if not os.path.isfile(args.restore_mapping_file):
        logger.error("Restore mapping file {0} doesn't "
                     "exists".format(args.restore_mapping_file))
        sys.exit(2)
    if not os.path.isdir(args.restore_path):
        logger.error("Restore data dir {0} doesn't "
                     "exist".format(args.restore_path))
        sys.exit(2)

    with open(args.restore_mapping_file, 'r') as f:
        restore_mapping = json.load(f)

    args.snapshotter.restore_snapshot(args.restore_path, restore_mapping)


def common_parser():
    parser = configargparse.ArgParser(add_help=False)
    parser.add('-c', '--conf-file', is_config_file=True,
               help='Config file for scyllabackup')

    parser.add('-l', '--log-level', default='WARNING',
               choices=['DEBUG', 'INFO', 'WARNING',
                        'ERROR', 'CRITICAL'],
               help='Log level for scyllabackup')
    parser.add('--path', required=True, help='Path of scylla data directory')
    parser.add('--db', required=True,
               help='Path of scyllabackup db file. The backup metadata is '
               'stored in this file.')
    parser.add('--provider', required=True, choices=['s3', 'wabs'],
               help='Cloud provider used for storage. It should be one of `s3` '
               'or `wabs`')
    parser.add('--nodetool-path', default='/usr/bin/nodetool',
               help='Path of nodetool utility on filesystem.')
    parser.add('--cqlsh-path', default='/usr/bin/cqlsh',
               help='Path of cqlsh utility on filesystem')
    parser.add('--cqlsh-host', default='127.0.0.1',
               help='Host to use for connecting cqlsh service')
    parser.add('--cqlsh-port', default='9042',
               help='Port to use for connecting cqlsh service')

    s3 = parser.add_argument_group("Required arguments if using "
                                   "'s3' provider")
    s3.add('--s3-bucket-name', metavar='BUCKET_NAME',
           help='Mandatory if provider is s3')
    s3.add('--s3-aws-key', metavar='AWS_KEY',
           help='Mandatory if provider is s3')
    s3.add('--s3-aws-secret', metavar='AWS_SECRET',
           help='Mandatory if provider is s3')
    wabs = parser.add_argument_group("Required arguments if using "
                                     "'wabs' provider")
    wabs.add('--wabs-container-name', help='Mandatory if provider is wabs')
    wabs.add('--wabs-account-name', help='Mandatory if provider is wabs')
    wabs.add('--wabs-sas-token', help='Mandatory if provider is wabs')

    parser.add('--prefix', required=True,
               help='Mandatory prefix to store backups in cloud storage')

    parser.add('--lock', default='/var/run/lock/scyllabackup.lock',
               help='Lock file for scyllabackup.')

    parser.add('--lock-timeout', type=int, default=10,
               help='Timeout for taking lock.')

    parser.add('--max-workers', type=int, default=4,
               help='Sets max workers for parallelizing storage api calls')

    return parser


def validate_storage_args(args, parser):
    missing_args = ", ".join(["--{0}".format(key.replace('_', '-'))
                              for key, value in args.__dict__.items()
                              if key.startswith(args.provider) and
                              value is None])
    if missing_args:
        parser.error("All arguments not passed for provider '{0}'."
                     " Require args: {1}".format(args.provider,
                                                 missing_args))


def parse_args(cli_args):
    parent_parser = common_parser()
    parser = configargparse.ArgParser(
        default_config_files=[],
        description="Tool to manage scylla backups")

    subparsers = parser.add_subparsers(help='sub-command help')

    take = subparsers.add_parser('take',
                                 help='Take a scylla snapshot and upload '
                                 'it to cloud storage',
                                 parents=[parent_parser],
                                 formatter_class=configargparse.
                                 DefaultsFormatter)

    take.set_defaults(func=take_snapshot)

    ls = subparsers.add_parser('list', help='List scylla snapshots',
                               parents=[parent_parser],
                               formatter_class=configargparse.
                               DefaultsFormatter)
    ls.add('--latest-before', help='If specified, last 5 snapshot '
           'before specified timestamp will be listed.')
    ls.set_defaults(func=list_snapshots)

    download = subparsers.add_parser('download',
                                     help='Download a scylla snapshot',
                                     parents=[parent_parser],
                                     formatter_class=configargparse.
                                     DefaultsFormatter)

    download.add('--keyspace', default=None,
                 help='If specified, operation will be performed on '
                 'specified keyspace only')
    download.add('--download-dir', required=True,
                 help='Specify directory path to download snapshot files')
    download_group = download.add_mutually_exclusive_group(required=True)
    download_group.add('--snapshot', help='Specify snapshot to be downloaded')
    download_group.add('--latest-before', help='If specified, latest snapshot '
                       'before or equal to specified unix timestamp will be fetched.')
    download.add('--schema', required=True,
                 help='Specify file path to download scylla database '
                 'schema file before applying it to db ')
    download.set_defaults(func=download_snapshot)

    db = subparsers.add_parser('download_db',
                               help='Download a scyllabackup backup db',
                               parents=[parent_parser],
                               formatter_class=configargparse.
                               DefaultsFormatter)

    db.add('db_download_path', type=str,
           help='Delete all snapshots older '
           'than specified days')
    db.set_defaults(func=download_db)

    delete = subparsers.add_parser('delete_older_than',
                                   help='Delete scylla snapshots',
                                   parents=[parent_parser],
                                   formatter_class=configargparse.
                                   DefaultsFormatter)
    delete.add('days', type=int,
               help='Delete all snapshots older '
               'than specified days')
    delete.set_defaults(func=delete_older_than_snapshot)

    verify = subparsers.add_parser('verify',
                                   help='Verify all files of a snapshot',
                                   parents=[parent_parser],
                                   formatter_class=configargparse.
                                   DefaultsFormatter)

    verify.add('snapshot', type=int,
               help='Snapshot Name for which files will be verified')
    verify.set_defaults(func=verify_snapshot)

    restore_schema = subparsers.add_parser('restore_schema', help='Restore '
                                           'schema from specified file in '
                                           'scylladb', parents=[parent_parser],
                                           formatter_class=configargparse.
                                           DefaultsFormatter)
    restore_schema.add('schema_file',
                       help="Path of the cql file to restore db schema")
    restore_schema.add('-f', '--force', action='store_true',
                       help="Skip confirmation if specified.")
    restore_schema.set_defaults(func=restore_schema_in_scylladb)

    restore_mapping = (subparsers.
                       add_parser('get_restore_mapping', help='Get restore '
                                  'mapping for moving files from old database '
                                  'folders to new database folders. This '
                                  'command outputs a json output on stdout '
                                  'which can then be used by the restore '
                                  'command', parents=[parent_parser],
                                  formatter_class=configargparse.
                                  DefaultsFormatter))
    restore_mapping.add('--keyspace', required=True, help='Keyspace for which '
                        'you want to get restore mappings')
    restore_mapping.add('--restore-path', required=True, help='Path where the '
                        'restore files were downloaded via `download` command')
    restore_mapping.add('restore_mapping_file', help='Path where restore '
                        'mapping file will be stored')
    restore_mapping.set_defaults(func=get_restore_mapping)

    restore = (subparsers.
               add_parser('restore', help='Get restore mapping for moving '
                          'files from old database folders to new database '
                          'folders', parents=[parent_parser],
                          formatter_class=configargparse.DefaultsFormatter))

    restore.add('--restore-path', help='Path where the restore files are '
                'downloaded')
    restore.add('--restore-mapping-file', help='JSON mapping of folders to move '
                'file from one folder to another folder')
    restore.set_defaults(func=restore_snapshot)

    args = parser.parse_args(cli_args)
    validate_storage_args(args, parser)
    args.storage = make_storage(args)
    args.snapshotter = make_snapshotter(args)
    return args


def cli_run_with_lock(args=sys.argv[1:]):
    cli = parse_args(args)
    lock = filelock.FileLock(cli.lock)
    log_level = getattr(logging, cli.log_level.upper())
    logger.setLevel(log_level)
    try:
        with lock.acquire(timeout=cli.lock_timeout):
            cli.func(cli)
    except filelock.Timeout:
        logger.info("Another Instance of application already running")
        sys.exit(2)

if __name__ == '__main__':
    cli_run_with_lock()
