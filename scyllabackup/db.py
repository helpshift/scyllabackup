import sqlite3 as sql
import datetime
from numbers import Number


class DB:
    """This class handles the database model for scyllabackup.

    For each snapshot taken, a timestamp epoch is stored in `snapshots` table
    against a snapshot_id and a reference to a schema for the snapshot. Schemas
    are stored in a separately in another table for normalization. All file
    references for all snapshots are stored in a single `files` table for
    normalization and references for files for each snapshot are stored in
    `snapshot_files` table.

    The reference for snapshot is a datetime object. Most functions accept a
    unix timestamp(int or str) or a datetime object for handling
    snapshot/epoch. This makes interpretation of a snapshot easier as a unix
    timestamp. SQL management of this object is taken care of by datetime
    adapter for sqlite3 db-api interface in use.

    NOTE: Please note the difference between `epoch` and `snapshot` function
    parameter for functions in this class which might seem confusing. All
    functions which take `epoch` as parameter can work with any timestamp.
    Functions which take `snapshot` as parameter expect an epoch which is
    stored in the snapshots table
    """

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
        """Normalize epoch from various different inputs to datetime and remove
        microseconds

        :param epoch: An object representing unix timestamp
        :returns: A datetime object representing given epoch
        :rtype: datetime.datetime

        """
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

    def add_schema(self, snapshot, schema):
        """Add a schema against a snapshot epoch in database

        :param snapshot: An object representing unix timestamp
        :param schema: An str representing scylladb schema
        """
        with self._conn as c:
            snapshot = DB.normalize_epoch(snapshot)
            try:
                c.execute('INSERT INTO schemas VALUES(?,?)', (None, schema))
            except sql.IntegrityError as e:
                pass

            find_schema_sql = ('SELECT schema_id FROM schemas '
                               'WHERE schema = ?')
            schema_id = next(c.execute(find_schema_sql, (schema,)))[0]
            c.execute('INSERT INTO snapshots VALUES(?,?,?)',
                      (None, snapshot, schema_id))

    def add_snapshot_files(self, snapshot, files):
        """Add snapshot files in database

        :param snapshot: An object representing unix timestamp
        :param files: A list of tuple representing a file uploaded
        """
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
        """Return an iterator of snapshots currently available in DB

        :returns: An iterator of unix timestamps represented as strings
        :rtype: Iterator(tuple)

        """
        sql = 'SELECT epoch from snapshots;'
        with self._conn as c:
            return (ts.strftime('%s') for (ts,) in c.execute(sql))

    def find_snapshot_files(self, snapshot, keyspace=None):
        """Return an iterator of files present for current snapshot

        :param snapshot: An object representing unix timestamp when snapshot was taken
        :param keyspace: Optional for restricting results for a specific keyspace
        :returns: An iterator of files matching a given snapshot and optionally a keyspace
        :rtype: Iterator(tuple)

        """
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
        """Return the schema of database when the snapshot was taken

        :param snapshot: An object representing unix timestamp when snapshot was taken
        :returns: A string representing schema of database when snapshot was taken
        :rtype: str

        """
        sql = ('SELECT schema '
               'FROM schemas WHERE schema_id IN '
               '(SELECT schema_id from snapshots WHERE epoch = ?)')
        snapshot = DB.normalize_epoch(snapshot)
        with self._conn as c:
                return next(c.execute(sql, (snapshot,)), (None,))[0]

    def find_deletable_files(self, epoch):
        """Returns an iterator of files not used by any snapshot newer than
        a given epoch. This list can be used for cleaning up old files.

        :param epoch: An object representing unix timestamp.
        :returns: A iterator of tuple representing files
        :rtype: Iterator[tuple]

        """
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
        epoch = DB.normalize_epoch(epoch)
        with self._conn as c:
            return c.execute(sql, (epoch, epoch))

    def delete_snapshots_files_older_than(self, epoch):
        """Delete entries from snapshots older than given epoch

        :param epoch: An object representing unix timestamp.
        :returns: True if any deletion occurs else false
        :rtype: bool

        """
        sql = ('DELETE FROM snapshots WHERE epoch <= ?')
        epoch = DB.normalize_epoch(epoch)
        with self._conn as c:
            return (c.execute(sql, (epoch,)).rowcount > 0)

    def cleanup_files_db(self):
        """Cleanup files db for files not present in any snapshot

        :returns: True if any deletion occurs else false
        :rtype: bool

        """
        sql = ('DELETE FROM files WHERE file_id not in '
               '(SELECT DISTINCT file_id FROM snapshots_files);')
        with self._conn as c:
            return (c.execute(sql).rowcount > 0)

    def vaccum(self):
        """Run vaccum function on database
        """
        with self._conn as c:
            c.execute("VACUUM")
