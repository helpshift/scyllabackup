import sqlite3 as sql
from .db import DB
import os
from datetime import datetime
from argparse import ArgumentParser


def migrate_db_v1_to_v2(db_path):
    old_db_path = db_path + datetime.now().strftime("%s") + ".bkp_db_v1"
    os.rename(db_path, old_db_path)

    old_db = sql.connect(old_db_path,
                         detect_types=sql.PARSE_DECLTYPES |
                         sql.PARSE_COLNAMES)
    new_db = DB(db_path)
    old_db.cursor().execute("PRAGMA foreign_keys = ON")

    epoch_list = []
    for epoch, schema in old_db.execute('SELECT epoch, schema '
                                        'FROM snapshots_schemas'):
        new_db.add_schema(epoch, schema)
        epoch_list.append(epoch)

    for epoch in epoch_list:
        file_list = list(old_db.execute("SELECT keyspace, tablename, file "
                                        "FROM snapshots_files WHERE epoch = ?",
                                        (epoch,)))
        new_db.add_snapshot_files(epoch, file_list)


def main():
    parser = ArgumentParser(description='Migrate database to new Schema')
    parser.add_argument('db_path', help='Database Path to be migrated')
    args = parser.parse_args()
    migrate_db_v1_to_v2(args.db_path)


if __name__ == '__main__':
    main()
