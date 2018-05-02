import sqlite3 as sql
import datetime
from numbers import Number


class DB:
    def __init__(self, path):
        self._conn = sql.connect(path,
                                 detect_types=sql.PARSE_DECLTYPES |
                                 sql.PARSE_COLNAMES)

        self._conn.cursor().execute("PRAGMA foreign_keys = ON")

        # TODO: Rename epoch field to snapshot if it makes sense
        self._conn.executescript("""
CREATE TABLE IF NOT EXISTS snapshots_schemas (
  epoch         timestamp,
  schema        TEXT,
  PRIMARY KEY (epoch)
);

CREATE TABLE IF NOT EXISTS snapshots_files (
  epoch         timestamp,
  file          TEXT,
  keyspace      TEXT,
  tablename     TEXT,
  PRIMARY KEY (epoch, file, keyspace, tablename),
  FOREIGN KEY (epoch) REFERENCES snapshots_schemas(epoch)
);
        """)

    @staticmethod
    def normalize_epoch(epoch):
        if isinstance(epoch, str) or isinstance(epoch, unicode):
            epoch = int(epoch)
        if isinstance(epoch, Number):
            epoch = datetime.datetime.fromtimestamp(epoch)
        if isinstance(epoch, datetime.datetime):
            epoch = epoch.replace(microsecond=0)
            return epoch
        else:
            raise ValueError("Cannot convert given "
                             "instance {0} to epoch".format(epoch))

    def add_schema(self, epoch, schema):
        with self._conn as c:
            epoch = DB.normalize_epoch(epoch)
            c.execute('INSERT INTO snapshots_schemas VALUES(?,?)', (epoch,
                                                                    schema))

    def add_snapshot_files(self, snapshot, files):
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            for keyspace, tablename, filename in files:
                c.execute('INSERT INTO snapshots_files'
                          ' VALUES(?,?,?,?)', (snapshot, filename, keyspace,
                          tablename))

    def find_snapshots(self):
        sql = 'SELECT epoch from snapshots_schemas;'
        with self._conn as c:
                return (ts.strftime('%s') for (ts,) in c.execute(sql))

    def find_snapshot_files(self, snapshot, keyspace=None):
        sql = ('SELECT keyspace, tablename, file '
               'FROM snapshots_files WHERE epoch = ?')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            if keyspace:
                return c.execute(sql + ' AND keyspace = ?',
                                 (snapshot, keyspace))
            else:
                return c.execute(sql, (snapshot,))

    def find_snapshot_schema(self, snapshot):
        sql = ('SELECT schema file '
               'FROM snapshots_schemas WHERE epoch = ?')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
                return next(c.execute(sql, (snapshot,)), (None,))[0]

    def find_deletable_files(self, snapshot):
        sql = ('SELECT DISTINCT keyspace, tablename, file '
               'FROM snapshots_files WHERE epoch <= ? '
               'EXCEPT '
               'SELECT DISTINCT keyspace, tablename, file '
               'FROM snapshots_files WHERE epoch > ?')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            return c.execute(sql, (snapshot, snapshot))

    def delete_snapshots_files_older_than(self, snapshot):
        sql = 'DELETE FROM snapshots_files WHERE epoch <= ?'
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            return (c.execute(sql, (snapshot,)).rowcount > 0)

    def cleanup_snapshots_schemas_db(self):
        sql = ('DELETE FROM snapshots_schemas WHERE epoch not in '
               '(SELECT DISTINCT epoch FROM snapshots_files);')
        with self._conn as c:
            return (c.execute(sql).rowcount > 0)
