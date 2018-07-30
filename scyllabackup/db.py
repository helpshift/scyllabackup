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
CREATE TABLE IF NOT EXISTS schemas (
  schema_id     INTEGER PRIMARY KEY ASC,
  schema        TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id   INTEGER PRIMARY KEY ASC,
  epoch         timestamp,
  schema_id     INTEGER,
  UNIQUE(epoch),
  FOREIGN KEY (schema_id) REFERENCES schemas(schema_id)
);

CREATE TABLE IF NOT EXISTS tablenames (
  table_id      INTEGER PRIMARY KEY ASC,
  keyspace      TEXT,
  tablename     TEXT,
  UNIQUE (keyspace, tablename)
);

CREATE TABLE IF NOT EXISTS files (
  file_id       INTEGER PRIMARY KEY ASC,
  file          TEXT,
  table_id      INTEGER,
  CONSTRAINT unique_file_constraint UNIQUE(file, table_id),
  FOREIGN KEY (table_id) REFERENCES tablenames(table_id)
);

CREATE TABLE IF NOT EXISTS snapshots_files (
  id            INTEGER PRIMARY KEY ASC,
  snapshot_id   INTEGER,
  file_id       INTEGER DEFAULT 0,
  UNIQUE(snapshot_id, file_id),
  FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
  FOREIGN KEY (file_id) REFERENCES files(file_id) ON DELETE RESTRICT
);

/* Following index is required for making DELETE statements faster for `files` table */
CREATE INDEX IF NOT EXISTS snapshots_files_file_id_idx ON snapshots_files(file_id);
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
            try:
                c.execute('INSERT INTO schemas VALUES(?,?)', (None, schema))
            except sql.IntegrityError as e:
                pass

            find_schema_sql = ('SELECT schema_id FROM schemas '
                               'WHERE schema = ?')
            schema_id = next(c.execute(find_schema_sql, (schema,)))[0]
            c.execute('INSERT INTO snapshots VALUES(?,?,?)',
                      (None, epoch, schema_id))

    def add_snapshot_files(self, snapshot, files):
        snapshot = DB.normalize_epoch(snapshot)
        find_snapshot_id_sql = ('SELECT snapshot_id FROM snapshots '
                                'WHERE epoch = ?')
        find_table_id_sql = ('SELECT table_id FROM tablenames '
                             'WHERE keyspace = ? AND tablename = ?')
        find_file_id_sql = ('SELECT file_id FROM files '
                            'WHERE file = ? AND table_id = ?')

        unique_tables = set()
        for keyspace, tablename, _ in files:
            unique_tables.add((keyspace, tablename))
        with self._conn as c:
            snapshot_id = next(c.execute(find_snapshot_id_sql, (snapshot,)))[0]
            for keyspace, tablename in unique_tables:
                try:
                    c.execute('INSERT INTO tablenames VALUES(?,?,?)',
                              (None, keyspace, tablename))
                except sql.IntegrityError:
                    pass

            for keyspace, tablename, filename in files:
                try:
                    table_id = next(c.execute(find_table_id_sql,
                                              (keyspace, tablename)))[0]
                    c.execute('INSERT INTO files '
                              'VALUES(?,?,?)',
                              (None, filename, table_id))
                except sql.IntegrityError:
                    pass
                finally:
                    file_id = next(c.execute(find_file_id_sql,
                                             (filename, table_id)))[0]
                    c.execute('INSERT INTO snapshots_files VALUES(?,?,?)',
                              (None, snapshot_id, file_id))

    def find_snapshots(self):
        sql = 'SELECT epoch from snapshots;'
        with self._conn as c:
            return (ts.strftime('%s') for (ts,) in c.execute(sql))

    def find_snapshot_files(self, snapshot, keyspace=None):
        sql = ('SELECT t.keyspace, t.tablename, f.file '
               'FROM files as f, tablenames as t '
               'WHERE f.table_id = t.table_id AND '
               'f.file_id IN '
               '(SELECT file_id FROM snapshots_files WHERE snapshot_id IN '
               '(SELECT snapshot_id from snapshots where epoch = ?))')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            if keyspace:
                return c.execute(sql + ' AND t.keyspace = ?',
                                 (snapshot, keyspace))
            else:
                return c.execute(sql, (snapshot,))

    def find_snapshot_schema(self, snapshot):
        sql = ('SELECT schema '
               'FROM schemas WHERE schema_id IN '
               '(SELECT schema_id from snapshots WHERE epoch = ?)')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
                return next(c.execute(sql, (snapshot,)), (None,))[0]

    def find_deletable_files(self, snapshot):
        sql = ('SELECT t.keyspace, t.tablename, f.file '
               'FROM files as f, tablenames as t '
               'WHERE f.table_id = t.table_id AND '
               'f.file_id IN '
               '(SELECT DISTINCT file_id '
               'FROM snapshots_files WHERE snapshot_id '
               'IN (SELECT snapshot_id from snapshots WHERE epoch <= ?) '
               'EXCEPT '
               'SELECT DISTINCT file_id '
               'FROM snapshots_files WHERE snapshot_id '
               'IN (SELECT snapshot_id from snapshots WHERE epoch > ?))')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            return c.execute(sql, (snapshot, snapshot))

    def delete_snapshots_files_older_than(self, snapshot):
        sql = ('DELETE FROM snapshots WHERE epoch <= ?')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
            return (c.execute(sql, (snapshot,)).rowcount > 0)

    def cleanup_files_db(self):
        sql = ('DELETE FROM files WHERE file_id not in '
               '(SELECT DISTINCT file_id FROM snapshots_files);')
        with self._conn as c:
            return (c.execute(sql).rowcount > 0)

    def vaccum(self):
        with self._conn as c:
            c.execute("VACUUM")
