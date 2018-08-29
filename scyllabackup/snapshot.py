import glob
import fnmatch
import logging
import sys
import os
import errno
from shutil import move

from sh import ErrorReturnCode, Command
from .db import DB

import gevent
import gevent.pool
import gevent.queue
import gevent.monkey
from azure.common import AzureMissingResourceHttpError

gevent.monkey.patch_all()

logger = logging.getLogger(__name__)


def log_shell_exception_and_exit(e):
    """Utility function to log exception for a non zero sh.Command exit and
    exit with error code 2

    :param e: A sh.Command exception for non zero exit code
    :returns: Nothing
    :rtype: None


    """
    logger.error("Command failed with exit code '{0}': {1}".
                 format(e.exit_code, e.__dict__))
    sys.exit(2)


class Snapshot:
    "Take scylla snapshot and upload it using provided uploader"

    def __init__(self, scylla_data_dir, db_path, storage_obj,
                 nodetool_path='/usr/bin/nodetool',
                 cqlsh_path='/usr/bin/cqlsh',
                 cqlsh_host='127.0.0.1',
                 cqlsh_port='9042',
                 prefix='scyllabackup',
                 max_workers=4):
        self.scylla_data_dir = scylla_data_dir
        self.db = DB(db_path)
        self.db_path = db_path
        self.nodetool = Command(nodetool_path)
        self.cqlsh = Command(cqlsh_path).bake(cqlsh_host, cqlsh_port)
        self._upload_queue = gevent.queue.JoinableQueue()
        self._download_queue = gevent.queue.JoinableQueue()
        self._delete_queue = gevent.queue.JoinableQueue()
        self._verify_queue = gevent.queue.JoinableQueue()
        self._storage = storage_obj
        self._prefix = prefix
        self.db_key = self._prefix + '/' + os.path.basename(self.db_path)
        self.max_workers = max_workers

    @staticmethod
    def mkdir_p(path):
        """Function to handle recursive directory creation like `mkdir -p`.
        It does not fail if directory already exists at path. If the path
        is some other file type then reraise exception

        :param path: Path where directory needs to be created
        :returns: Nothing
        :rtype: None

        """
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def snapshot_file_glob(self, snapshot_name, keyspace_name='*'):
        """Function to return a glob iterator for given snapshot name.
        Restrict output to a specific keyspace if specified

        :param snapshot_name: Name of the snapshot whose files you want a
                              globbing iterator for.
        :param keyspace_name: Name of the keyspace within the specified
                              snapshot, you want the globbing to restrict to.
        :returns: An generator for looping over files in a snapshot
        :rtype: Iterator[str]

        """
        logger.debug("Gathering snapshot files from "
                     "data dir {0}".format(self.scylla_data_dir))
        snapshot_path = os.path.join(self.scylla_data_dir,
                                     keyspace_name,
                                     '*', 'snapshots',
                                     snapshot_name, '*')

        return glob.iglob(snapshot_path)

    def _nodetool_snapshot_op(self, snapshot_name, keyspace_name=None,
                              op='snapshot'):
        """Wrap nodetool utility for strictly taking or deleting snapshots.
        The function takes a snapshot name and optionally keyspace name.
        Taking/deleting is controlled by `op` parameter

        :param snapshot_name: Tag name of the snapshot to be taken or deleted
        :param keyspace_name: Restrict snapshot operation to a keyspace if
                              specified
        :param op: Snapshot operation keyword. Valid values are 'snapshot' for
                   taking a snapshot or 'clearsnapshot' for deleting snapshot
        :returns: Nothing
        :rtype: None

        """
        snapshot_log = "snapshot {0}".format(snapshot_name)
        if keyspace_name:
            snapshot_log += " for keyspace_name {1}".format(keyspace_name)

        if op == 'snapshot':
            debug_message = "Taking " + snapshot_log
            error_message = "Failed while taking " + snapshot_log
        elif op == 'clearsnapshot':
            debug_message = "Deleting " + snapshot_log
            error_message = "Failed while deleting " + snapshot_log
        else:
            raise ValueError("Snapshot operation can be "
                             "one of snapshot or clearsnapshot")

        logger.debug(debug_message)

        try:

            cmd = self.nodetool.bake(op, '-t', snapshot_name)
            if keyspace_name:
                cmd.bake(keyspace_name)
            cmd()

        except ErrorReturnCode as e:
            logger.error(error_message)
            log_shell_exception_and_exit(e)

    def nodetool_take_snapshot(self, snapshot_name, keyspace_name=None):
        """Take snapshot with specified snapshot name tag, and optionally
        restrict the operation to keyspace name if provided

        :param snapshot_name: Tag name of the snapshot to be taken
        :param keyspace_name: Restrict snapshot operation to a keyspace if
                              specified
        :returns: Nothing
        :rtype: None

        """
        self._nodetool_snapshot_op(snapshot_name, keyspace_name, op='snapshot')

    def snapshot_schema(self, snapshot_name):
        logger.debug("Trying to take snapshot of schema")
        try:
            schema = self.cqlsh('-e', 'DESC SCHEMA;')
            self.db.add_snapshot(snapshot_name, schema.stdout)
        except ErrorReturnCode as e:
            logger.error("Failed to take schema backup for snapshot {0}".
                         format(snapshot_name))
            log_shell_exception_and_exit(e)

    def nodetool_delete_snapshot(self, snapshot_name, keyspace_name=None):
        """Delete snapshot with specified snapshot name tag, and optionally
        restrict the operation to keyspace name if provided

        :param snapshot_name: Tag name of the snapshot to be deleted
        :param keyspace_name: Restrict snapshot operation to a keyspace if
                              specified
        :returns: Nothing
        :rtype: None

        """
        self._nodetool_snapshot_op(snapshot_name, keyspace_name,
                                   op='clearsnapshot')

    def upload_snapshot(self, snapshot_name, keyspace_name='*'):
        """Take and upload a scylladb snapshot to cloud storage

        :param snapshot_name: Tag name of the snapshot to be taken
        :param keyspace_name: Restrict snapshot operation to a keyspace if
                              specified
        :returns: Nothing
        :rtype: None

        """
        self.snapshot_schema(snapshot_name)

        # The path for scylla db snapshot files has following prefix. We want
        # first five parts for backup. Example path:
        # <scylla_data_dir>/<keyspace>/<table>/snapshots/<snapshot>/<file>.db
        def split_path(path):
            for i in range(5):
                path, basename = os.path.split(path)
                yield basename

        for i in range(self.max_workers):
            gevent.spawn(self.file_upload_worker)

        file_list = []
        for file_name in self.snapshot_file_glob(snapshot_name, keyspace_name):
            # Skipping upload of manifest.json files which contains the list of
            # backup files for a snapshot. We do not need this info is present
            # in database backup.
            if not fnmatch.fnmatch(file_name, '*/manifest.json'):
                (file_base_name, _, _,
                 table_name, keyspace_name) = split_path(file_name)
                logger.info("Adding file {0}, file: {1}, "
                            "table: {2}, keyspace: {3} "
                            "in upload_queue".format(file_name,
                                                     file_base_name,
                                                     table_name,
                                                     keyspace_name))

                file_list.append((keyspace_name, table_name, file_base_name))
                self._upload_queue.put((file_name,
                                        file_base_name,
                                        keyspace_name,
                                        table_name))

        logger.info("Add file list in database for "
                    "snapshot {0}".format(snapshot_name))
        self.db.add_snapshot_files(snapshot_name, file_list)
        self._storage.upload_file(self.db_key, self.db_path)

        self._upload_queue.join()

    def download_db(self, path):
        """Download scyllabackup metadata db file to specified path

        :param path: File path to download scyllabackup metadata db
        :returns: Nothing
        :rtype: None

        """
        self._storage.download_file(self.db_key, path)

    def download_snapshot(self, path, snapshot_name, keyspace_name=None):
        """Download snapshot from cloud storage reading the scyllabackup
        metadata db

        :param path: Directory to download a scyllabackup snapshot
        :param snapshot_name: Tag name of the snapshot to be downloaded
        :param keyspace_name: Restrict download operation to a keyspace if
                              specified
        :returns: Nothing
        :rtype: None

        """
        snapshot_id = self.db.find_snapshot_id(snapshot_name)
        if snapshot_id is None:
            logger.error("Specified snapshot doesn't exist, please specify a valid snapshot.")
            sys.exit(2)

        for file_tuple in self.db.find_snapshot_files(snapshot_name,
                                                      keyspace_name):
            # file_tuple = tuple(keyspace,tablename,file)
            self._download_queue.put(file_tuple)
        for i in range(self.max_workers):
            gevent.spawn(self.file_download_worker, path)
        self._download_queue.join()

    def file_download_worker(self, path):
        """Worker for downloading snapshot files. This worker is mapped to
        gevent threads for concurrency. Number of threads is configured by
        `self.max_workers`

        :param path: Directory path where snapshot files will be downloaded
        :returns: Nothing
        :rtype: None

        """
        while True:
            try:
                # file_tuple = tuple(keyspace,tablename,file)
                file_tuple = self._download_queue.get()
                storage_key = '/'.join((self._prefix,) + file_tuple)
                Snapshot.mkdir_p(os.path.join(path, *file_tuple[:-1]))
                self._storage.download_file(storage_key,
                                            os.path.join(path, *file_tuple))

            except Exception as e:
                logger.exception("Unexpected exception encountered")
                sys.exit(4)
            finally:
                self._download_queue.task_done()

    def verify_snapshot(self, snapshot_name):
        """Verifies that all files for a given snapshot name are present in
        the cloud storage. Useful for a consistency check before downloading
        snapshot

        :param snapshot_name: Tag name of the snapshot to be verified
        :returns: True if all files for given snapshot are present in cloud
                  storage, else False
        :rtype: bool

        """
        self.verify_success = True

        for i in range(self.max_workers):
            gevent.spawn(self.file_verify_worker)

        for file_tuple in self.db.find_snapshot_files(snapshot_name):
            # file_tuple = tuple(keyspace,tablename,file)
            self._verify_queue.put(file_tuple)

        self._verify_queue.join()

        return self.verify_success

    def file_verify_worker(self):
        """Worker for verifying snapshot files. This worker is mapped to
        gevent threads for concurrency. Number of threads is configured by
        `self.max_workers`

        :returns: Nothing
        :rtype: None

        """
        while True:
            try:
                # file_tuple = tuple(keyspace,tablename,file)
                file_tuple = self._verify_queue.get()
                storage_key = '/'.join((self._prefix,) + file_tuple)
                remote_file = self._storage.get_object_properties(storage_key)
                if remote_file is None:
                    logger.error("Remote file {0} "
                                 "doesn't exist".format(storage_key))
                    self.verify_success = False
                else:
                    logger.debug("Remote file {0} "
                                 "is present in storage".format(storage_key))
            except Exception as e:
                logger.exception("Unexpected exception encountered")
                sys.exit(4)
            finally:
                self._verify_queue.task_done()

    def file_upload_worker(self):
        """Worker for uploading files. This worker is mapped to gevent threads
        for concurrency. Number of threads is configured by `self.max_workers`

        :returns: Nothing
        :rtype: None

        """

        while True:
            try:
                (file_name, file_base_name,
                 keyspace_name, table_name) = self._upload_queue.get()

                key = '/'.join((self._prefix, keyspace_name,
                                table_name, file_base_name))

                remote_file = (self._storage.
                               get_object_properties(key,
                                                     metadata=True))

                file_stat = os.stat(file_name)
                file_size = file_stat.st_size
                file_mtime = str(int(file_stat.st_mtime))
                if (remote_file and remote_file['size'] == file_size and
                    remote_file['metadata']['mtime'] == file_mtime):
                    logger.info('Remote file size/mtime matches for "{0}".'
                                ' No reupload required'.format(key))
                else:
                    if remote_file:
                        logger.warn('Remote file size/mtime mismatch for "{0}"'
                                    '. Reupload required'.format(key))

                    logger.info('Uploading file "{0}"'.format(key))
                    self._storage.upload_file(key,
                                              file_name,
                                              metadata={'mtime': file_mtime})
            except Exception as e:
                logger.exception("Unexpected exception encountered")
                sys.exit(4)
            finally:
                self._upload_queue.task_done()

    def delete_snapshot(self, snapshot):
        """Delete all files older than a given snapshot from cloud storage and
        cleanup scyllabackup metadata db

        :param snapshot_name: Tag name of the snapshot before which all files
                              are to be deleted
        :returns: Nothing
        :rtype: None

        """
        for file_tuple in self.db.find_deletable_files(snapshot):
            self._delete_queue.put(file_tuple)

        for i in range(self.max_workers):
            gevent.spawn(self.file_delete_worker)
        self._delete_queue.join()
        self.db.delete_snapshots_files_older_than(snapshot)
        if self.db.cleanup_files_db():
            self.db.vacuum()

    def file_delete_worker(self):
        """Worker for deleting files. This worker is mapped to gevent threads
        for concurrency. Number of threads is configured by `self.max_workers`

        :returns: Nothing
        :rtype: None

        """

        while True:
            try:
                # file_tuple = tuple(keyspace,tablename,file)
                file_tuple = self._delete_queue.get()
                storage_key = '/'.join((self._prefix, ) + file_tuple)
                self._storage.delete_key(storage_key)
            except AzureMissingResourceHttpError as e:
                logger.error("Deletion of blob {0} failed. It's already deleted or missing.".format(storage_key))
            except Exception as e:
                logger.exception("Unexpected exception encountered")
                sys.exit(4)
            finally:
                self._delete_queue.task_done()

    def find_new_table_path(self, keyspace_name, table_name):
        """This function returns the on-disk directory of a table where
        sstables are stored for scylladb given the keyspace and table name.

        This utility is required for creating this restore mapping of table
        directory between a downloaded snapshot and a freshly created scylladb
        cluster for restore. The mapping is returned as a dictionary.

        The mapping is required because scylladb generates a UUID for each
        table in keyspace and store sstable files in a dir named `tablename +
        '-' + <UUID without any hyphens>`. This UUID is freshly generated when
        a keyspace is created in scylla. When restoring a snapshot, the UUID of
        table path of a newly created scylladb instance where schema is
        restored will mismatch the table path of a downloaded snapshot. This
        happens as the snapshot was created on different cluster and will have
        different uuid for each table. By creating a mapping, we can automate
        the restore process.


        :param keyspace_name: Name of the keyspace you want restore mapping for
        :param table_name: Name of the table in keyspace you want restore
                           mapping for
        :returns: Path of directory where sstables are stored for table in
                  given keyspace
        :rtype: str

        """
        cql = ("EXPAND ON; "
               "SELECT id FROM system_schema.tables "
               "WHERE keyspace_name = '{0}' "
               "AND table_name= '{1}';").format(keyspace_name, table_name)
        cql_cmd = self.cqlsh.bake('--no-color', '-e', cql)
        # Sample output of above command (ignore indentation, includes blank lines)
        # """
        # Now Expanded output is enabled
        #
        # @ Row 1
        # ----+--------------------------------------
        #  id | 08ae880a-52e9-43ec-9ed1-55afc2e8e7c6
        #
        # (1 rows)
        # """
        uuid_lines = [line for line in cql_cmd().splitlines()
                      if line.startswith(' id | ')]
        if len(uuid_lines) != 1:
            raise ValueError(('Matching id found for given keyspace and '
                              'table not equal to 1'))
        uuid = uuid_lines[0].split()[-1].replace('-', '')
        table_path_name = "{0}-{1}".format(table_name, uuid)
        return os.path.join(self.scylla_data_dir, keyspace_name,
                            table_path_name)

    def restore_schema(self, restore_schema_path):
        """Function to restore schema in scylladb from a cql file. This can be
        done manually also directly via cqlsh. This just abstracts the
        interface and is only expected to run on a new/clean cluster.

        :param restore_schema_path: The path of the cql file to be imported in
                                    scylladb
        :returns: Nothing
        :rtype: None

        """
        try:
            self.cqlsh.bake('-f')(restore_schema_path)
        except ErrorReturnCode as e:
            logger.error("Error while restoring schema")
            log_shell_exception_and_exit(e)

    def restore_snapshot_mapping(self, restore_path, keyspace_name):
        """Returns a dictionary which represents a path mapping from an already
        downloaded snapshot tables to the freshly created tables for a given
        keyspace. This is required as the directory path of table in snapshot
        mismatches directory path of table in a keyspace for a newly created
        cluster. Refer documentation of `find_new_table_path` function for more
        details why this happens.

        :param restore_path: The directory path where the snapshot files have
                             been downloaded
        :param keyspace_name: Name of the keyspace to be restored, from
                              download path
        :returns: Dictionary with path of downloaded snapshot table dir as key
                  and path of new table dir as value
        :rtype: dict[str, str]

        """
        tables = (os.path.basename(table_path) for table_path in
                  glob.iglob(os.path.join(restore_path, keyspace_name, '*'))
                  if os.path.isdir(table_path))

        restore_mapping = {}
        for table in tables:
            old_table_path = os.path.join(restore_path, keyspace_name, table)

            # NOTE: Following line removes 33 chars from table_name removes the
            # '-<UUID>' from table_path, which is the required arg for
            # `find_new_table_path`. Example: If the scylla has generated uuid
            # "08ae880a-52e9-43ec-9ed1-55afc2e8e7c6" for a table 'table1' in
            # keyspace 'keyspace1' then it will be stored on disk in directory:
            # <scylla_data_dir>/keyspace1/table1-08ae880a52e943ec9ed155afc2e8e7c6.
            # Notice that while appending uuid to tablename for directory,
            # scylladb removes all the hyphens. The UUID is present in the
            # directory of table when downloaded from cloud storage. Following
            # function will remove the 33 chars of uuid from that path name
            new_table_path = self.find_new_table_path(keyspace_name,
                                                      table[:-33])

            restore_mapping[old_table_path] = new_table_path

        return restore_mapping

    def restore_snapshot(self, restore_path, restore_mapping):
        """This function takes the mapping generated via
        `restore_snapshot_mapping` function for table mapping from snapshot to
        table data directory for scylladb. It then moves all the sstable files
        for each table from snapshot download dir to scylladb data dir.

        :param restore_mapping: Dictionary with path of downloaded snapshot
                                table dir as key and path of new table dir as
                                value
        :returns: Nothing
        :rtype: None

        """
        # Ensure that target restore dirs do not have any existing files
        target_dir_empty = True
        for new_table_path in restore_mapping.values():
            files_in_new_table_path = filter(lambda f:
                                             os.path.isfile(os.path.join
                                                            (new_table_path,
                                                             f)),
                                             os.listdir(new_table_path))
            if len(files_in_new_table_path) > 0:
                target_dir_empty = False
                table_name = os.path.basename(new_table_path)[:-33]
                logger.error("Newly created table {0} has some existing files "
                             "in {1}".format(table_name, new_table_path))

        if not target_dir_empty:
            sys.exit(2)

        for old_table_path, new_table_path in restore_mapping.items():
            for file_path in glob.iglob(os.path.join(old_table_path, '*')):
                move(file_path, new_table_path)
