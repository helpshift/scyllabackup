import glob
import fnmatch
import logging
import sys
import os
import errno

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
    logger.error("Command failed with exit code '{0}': {1}".
                 format(e.exit_code, e.__dict__))
    sys.exit(e.exit_code)


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
        self.db_key = self._prefix + '/' + os.path.split(self.db_path)[1]
        self.max_workers = max_workers

    @staticmethod
    def mkdir_p(path):
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def snapshot_file_glob(self, snapshot_name, keyspace_name='*'):
        logger.debug("Gathering snapshot files from "
                     "data dir {0}".format(self.scylla_data_dir))
        snapshot_path = os.path.join(self.scylla_data_dir,
                                     keyspace_name,
                                     '*', 'snapshots',
                                     snapshot_name, '*')

        return glob.iglob(snapshot_path)

    def _nodetool_snapshot_op(self, snapshot_name, keyspace_name=None,
                              op='snapshot'):
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
        self._nodetool_snapshot_op(snapshot_name, keyspace_name,
                                   op='clearsnapshot')

    def upload_snapshot(self, snapshot_name, keyspace_name='*'):
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
        self._storage.download_file(self.db_key, path)

    def download_snapshot(self, path, snapshot_name, keyspace_name=None):
        for file_tuple in self.db.find_snapshot_files(snapshot_name,
                                                      keyspace_name):
            # file_tuple = tuple(keyspace,tablename,file)
            self._download_queue.put(file_tuple)
        for i in range(self.max_workers):
            gevent.spawn(self.file_download_worker, path)
        self._download_queue.join()

    def file_download_worker(self, path):
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
        self.verify_success = True

        for i in range(self.max_workers):
            gevent.spawn(self.file_verify_worker)

        for file_tuple in self.db.find_snapshot_files(snapshot_name):
            # file_tuple = tuple(keyspace,tablename,file)
            self._verify_queue.put(file_tuple)

        self._verify_queue.join()

        return self.verify_success

    def file_verify_worker(self):
        while True:
            try:
                # file_tuple = tuple(keyspace,tablename,file)
                file_tuple = self._verify_queue.get()
                storage_key = '/'.join((self._prefix,) + file_tuple)
                remote_file = next(self._storage.list_object_keys(storage_key),
                                   None)
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
        while True:
            try:
                (file_name, file_base_name,
                 keyspace_name, table_name) = self._upload_queue.get()

                key = '/'.join((self._prefix, keyspace_name,
                                table_name, file_base_name))

                remote_file = next(self._storage.
                                   list_object_keys(key,
                                                    metadata=True),
                                   None)

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
        for file_tuple in self.db.find_deletable_files(snapshot):
            self._delete_queue.put(file_tuple)

        for i in range(self.max_workers):
            gevent.spawn(self.file_delete_worker)
        self._delete_queue.join()
        self.db.delete_snapshots_files_older_than(snapshot)
        if self.db.cleanup_files_db():
            self.db.vacuum()

    def file_delete_worker(self):
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
