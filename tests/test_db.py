from scyllabackup.db import DB
import pytest

from sqlite3 import IntegrityError
import datetime


@pytest.fixture(scope='module')
def sql_db():
    return DB(':memory:')


def test_init_db(sql_db):
    sql_db._conn.cursor()

def test_insert_schema(sql_db, snapshot_one):
    sql_db.add_schema(snapshot_one, 'DUMMY SCHEMA')


def test_insert_schema2(sql_db, snapshot_two):
    sql_db.add_schema(snapshot_two, 'DUMMY SCHEMA2')


def test_insert_snapshot_one(sql_db, snapshot_one):
    file_list = (('keyspace1', 'table1', 'file{0}'.format(i))
                 for i in range(1, 5))
    sql_db.add_snapshot_files(snapshot_one, file_list)


def test_insert_snapshot_two(sql_db, snapshot_two):
    file_list = (('keyspace1', 'table1', 'file{0}'.format(i))
                 for i in range(2, 4))
    sql_db.add_snapshot_files(snapshot_two, file_list)


@pytest.mark.xfail(raises=IntegrityError)
def test_insert_snapshot_fail(sql_db, snapshot_one):
    sql_db.add_snapshot_files(snapshot_one, [('keyspace1', 'table1', 'file1')])


def test_time_class(sql_db, snapshot_one):
    get_a_row = sql_db._conn.execute('select epoch from '
                                     'snapshots').fetchone()
    epoch = get_a_row[0]
    assert isinstance(epoch, datetime.datetime)


def test_find_snapshot_files(sql_db, snapshot_one, snapshot_two):
    snapshots_list = list(sql_db.find_snapshots())
    assert snapshot_one in snapshots_list
    assert snapshot_two in snapshots_list


def test_find_deletable_files(sql_db, snapshot_one):
    deletable_file_list = [f[2] for f in
                           sql_db.find_deletable_files(snapshot_one)]
    assert 'file1' in deletable_file_list
    assert 'file4' in deletable_file_list


def test_delete_snapshots(sql_db, snapshot_one):
    assert sql_db.delete_snapshots_files_older_than(snapshot_one) is True


def test_find_deletable_files_again(sql_db, snapshot_one):
    deletable_file_list = [f[2] for f in
                           sql_db.find_deletable_files(snapshot_one)]
    assert len(deletable_file_list) is 0


def test_cleanup_files_db(sql_db):
    assert sql_db.cleanup_files_db() is True


def test_find_snapshot_files_again(sql_db, snapshot_one, snapshot_two):
    snapshots_list = list(sql_db.find_snapshots())
    assert snapshot_one not in snapshots_list
    assert snapshot_two in snapshots_list
