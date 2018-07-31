import logging
import os

logger = logging.getLogger(__name__)


def test_setup_data_snapshot_one(snapshotter):
    cql = snapshotter.cqlsh.bake("-e")
    cql("CREATE KEYSPACE Excelsior WITH replication"
        " = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    cql("CREATE TABLE Excelsior.test "
        "(pk int, t int, v text, s text static, PRIMARY KEY (pk, t));")
    cql("INSERT INTO Excelsior.test(pk, t, v, s) "
        "VALUES (0, 0, 'val0', 'static0');")
    cql("INSERT INTO Excelsior.test(pk, t, v, s) "
        "VALUES (0, 0, 'val1', 'static1');")


def test_nodetool_take_snapshot_one(snapshotter, snapshot_one):
    snapshotter.nodetool_take_snapshot(snapshot_one)
    # This should not raise an exception
    snapshotter.snapshot_file_glob(snapshot_one, 'excelsior').next()


def test_add_snapshot_one_db(snapshotter, snapshot_one):
    snapshotter.upload_snapshot(snapshot_one)
    assert snapshotter.verify_snapshot(snapshot_one) is True


def test_setup_data_snapshot_two(snapshotter):
    cql = snapshotter.cqlsh.bake("-e")
    cql("INSERT INTO Excelsior.test(pk, t, v, s) "
        "VALUES (1, 1, 'val2', 'static2');")
    cql("INSERT INTO Excelsior.test(pk, t, v, s) "
        "VALUES (1, 2, 'val3', 'static3');")


def test_nodetool_take_snapshot_two(snapshotter, snapshot_two):
    snapshotter.nodetool_take_snapshot(snapshot_two)
    # This should not raise an exception
    snapshotter.snapshot_file_glob(snapshot_two, 'excelsior').next()


def test_add_snapshot_two_db(snapshotter, snapshot_two):
    snapshotter.upload_snapshot(snapshot_two)
    assert snapshotter.verify_snapshot(snapshot_two) is True


def test_nodetool_delete_snapshot(snapshotter, snapshot_one):
    snapshotter.nodetool_delete_snapshot(snapshot_one)
    try:
        snapshotter.snapshot_file_glob(snapshot_one).next()
    except Exception as e:
        assert isinstance(e, StopIteration)


def test_download_snapshot(scylla_restore_dir, snapshotter, snapshot_two):
    snapshotter.download_snapshot(scylla_restore_dir,
                                  snapshot_two, 'excelsior')


def test_delete_data_snapshot_three(snapshotter):
    cql = snapshotter.cqlsh.bake("-e")
    cql("DROP TABLE Excelsior.test;")
    cql("DROP KEYSPACE Excelsior;")
    cql("CREATE KEYSPACE Excelsior2 WITH replication"
        " = {'class': 'SimpleStrategy', 'replication_factor' : 3};")
    cql("CREATE TABLE Excelsior2.test "
        "(pk int, t int, v text, s text static, PRIMARY KEY (pk, t));")
    cql("INSERT INTO Excelsior2.test(pk, t, v, s) "
        "VALUES (0, 0, 'val0', 'static0');")
    cql("INSERT INTO Excelsior2.test(pk, t, v, s) "
        "VALUES (0, 0, 'val1', 'static1');")


def test_nodetool_take_snapshot_three(snapshotter, snapshot_three):
    snapshotter.nodetool_take_snapshot(snapshot_three)
    # This should not raise an exception
    snapshotter.snapshot_file_glob(snapshot_three).next()
    try:
        snapshotter.snapshot_file_glob(snapshot_three, 'excelsior').next()
    except StopIteration:
        pass
    snapshotter.snapshot_file_glob(snapshot_three, 'excelsior2').next()



def test_add_snapshot_three_db(snapshotter, snapshot_three):
    snapshotter.upload_snapshot(snapshot_three)
    assert snapshotter.verify_snapshot(snapshot_three) is True


def test_delete_snapshot(snapshotter, snapshot_two, snapshot_three):
    snapshotter.delete_snapshot(snapshot_two)
    assert len(list(snapshotter.db.find_snapshot_files(snapshot_two))) == 0
    assert len(list(snapshotter.db.find_snapshot_files(snapshot_three,
                                                       'excelsior'))) == 0
    assert len(list(snapshotter.db.find_snapshot_files(snapshot_three))) != 0
    assert len(list(snapshotter.db.find_snapshots())) == 1
    assert len(list(snapshotter._storage.list_object_keys(prefix='scyllabackup/excelsior/test'))) == 0
    assert len(list(snapshotter._storage.list_object_keys(prefix='scyllabackup/excelsior2/test'))) > 0


def test_download_db(snapshotter, scylla_restore_dir):
    snapshotter.download_db(os.path.join(scylla_restore_dir, 'backup_test.db'))
