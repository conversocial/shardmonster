from __future__ import absolute_import

from .mock import Mock
import bson
import six

from pymongo.operations import UpdateOne
from shardmonster import api, sharder
from shardmonster.tests.base import ShardingTestCase, MongoTestCase
from shardmonster.sharder import batch_of_upsert_ops
import test_settings


if six.PY3:
    long = int


class TestBatchingOfInsertsDuringCopyPhase(MongoTestCase):
    def test_can_make_batches_of_upsert_queries_from_id(self):
        batch = batch_of_upsert_ops(
            [{'_id': 1, 'n': 100}, {'_id': 2, 'n': 200}],
            ('_id',))
        self.assertEqual(
            batch,
            [UpdateOne({'_id': 1}, {'$set': {'n': 100}}, upsert=True),
             UpdateOne({'_id': 2}, {'$set': {'n': 200}}, upsert=True)])

    def test_will_create_upserts_with_full_shard_key(self):
        batch = batch_of_upsert_ops(
            [{'_id': 1, 'd': 10, 'n': 100}, {'_id': 2, 'd': 20, 'n': 200}],
            ('d', '_id',))
        self.assertEqual(
            batch,
            [UpdateOne({'_id': 1, 'd': 10},
                       {'$set': {'n': 100, 'd': 10}},
                       upsert=True),
             UpdateOne({'_id': 2, 'd': 20},
                       {'$set': {'n': 200, 'd': 20}},
                       upsert=True)])


class TestOplogInsertsDuringSyncPhase(MongoTestCase):
    def setUp(self):
        self.source = self._connect(test_settings.CONN1['uri'],
                                    test_settings.CONN1['db_name'])
        self.target = self._connect(test_settings.CONN2['uri'],
                                    test_settings.CONN2['db_name'])

    def test_copies_a_document_not_already_there(self):
        self.source.stuff.insert({'_id': 99, 'sh': 1})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'i',
             'ns': self.source.name + '.stuff',
             'o': {'_id': 99, 'sh': 1}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()),
                         [{'_id': 99, 'sh': 1}])

    def test_skips_copying_if_target_document_already_there(self):
        self.source.stuff.insert({'_id': 99, 'sh': 1, 'v': 'current'})
        self.target.stuff.insert({'_id': 99, 'sh': 1, 'v': 'current'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'i',
             'ns': self.source.name + '.stuff',
             'o': {'_id': 99, 'sh': 1, 'v': 'earlier'}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()),
                         [{'_id': 99, 'sh': 1, 'v': 'current'}])

    def test_skips_copying_if_source_document_no_longer_there(self):
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'i',
             'ns': self.source.name + '.stuff',
             'o': {'_id': 99, 'sh': 1, 'v': 'earlier'}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()), [])

    def test_skip_insert_if_not_part_of_the_shard(self):
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'i',
             'ns': self.source.name + '.stuff',
             'o': {'_id': 99, 'sh': 1, 'v': 'earlier'}},
            {'sh': 2},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()), [])

    def test_skip_insert_if_not_even_the_same_database(self):
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'i',
             'ns': 'somwhere_else.stuff',
             'o': {'_id': 99, 'sh': 1, 'v': 'earlier'}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()), [])


class TestOplogUpdatesDuringSyncPhase(MongoTestCase):
    def setUp(self):
        self.source = self._connect(test_settings.CONN1['uri'],
                                    test_settings.CONN1['db_name'])
        self.target = self._connect(test_settings.CONN2['uri'],
                                    test_settings.CONN2['db_name'])

    def test_patch_missing_copied_rows_by_upserting_from_source(self):
        self.source.stuff.insert({'_id': 99, 'sh': 1, 'v': 'current'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'u',
             'ns': self.source.name + '.stuff',
             'o2': {'_id': 99},
             'o': {'v': 'somewhen'}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()),
                         [{'_id': 99, 'sh': 1, 'v': 'current'}])

    def test_avoid_double_updates_by_recopying_direct_from_the_source(self):
        self.source.stuff.insert({'_id': 99, 'sh': 1, 'v': 'current'})
        self.target.stuff.insert({'_id': 99, 'sh': 1, 'v': 'earlier'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'u',
             'ns': self.source.name + '.stuff',
             'o2': {'_id': 99, 'sh': 1},
             'o': {'sh': 1, 'v': 'somewhen'}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()),
                         [{'_id': 99, 'sh': 1, 'v': 'current'}])

    def test_skip_update_if_source_is_missing_as_must_be_deleted_later(self):
        self.target.stuff.insert({'_id': 99, 'sh': 1, 'v': 'earlier'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'u',
             'ns': self.source.name + '.stuff',
             'o2': {'_id': 99, 'sh': 1},
             'o': {'sh': 1, 'v': 'somewhen'}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()),
                         [{'_id': 99, 'sh': 1, 'v': 'earlier'}])

    def test_skip_update_if_not_part_of_the_shard(self):
        self.source.stuff.insert({'_id': 99, 'sh': 1, 'v': 'current'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'u',
             'ns': self.source.name + '.stuff',
             'o2': {'_id': 99},
             'o': {'v': 'somewhen'}},
            {'sh': 2},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()), [])


class TestOplogDeletesDuringSyncPhase(MongoTestCase):
    def setUp(self):
        self.source = self._connect(test_settings.CONN1['uri'],
                                    test_settings.CONN1['db_name'])
        self.target = self._connect(test_settings.CONN2['uri'],
                                    test_settings.CONN2['db_name'])

    def test_deletion_will_delete_document_when_still_in_target(self):
        self.target.stuff.insert({'_id': 99, 'sh': 1, 'v': 'earlier'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'd',
             'ns': self.source.name + '.stuff',
             'o': {'_id': 99}},
            {'sh': 1},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()), [])

    def test_skip_delete_if_not_part_of_the_shard(self):
        self.target.stuff.insert({'_id': 99, 'sh': 1, 'v': 'earlier'})
        sharder.replay_oplog_entry(
            {'ts': bson.timestamp.Timestamp(1510573671, 1),
             'h': long(999),
             'v': 2,
             'op': 'd',
             'ns': self.source.name + '.stuff',
             'o': {'_id': 99}},
            {'sh': 2},
            self.source.stuff,
            self.target.stuff)
        self.assertEqual(list(self.target.stuff.find()),
                         [{'_id': 99, 'sh': 1, 'v': 'earlier'}])


class TestSharder(ShardingTestCase):
    def setUp(self):
        api.activate_caching(0.5)
        super(TestSharder, self).setUp()

    def tearDown(self):
        # Deactivate caching by setting a 0 timeout
        api.activate_caching(0)
        super(TestSharder, self).tearDown()

    def test_basic_copy(self):
        api.set_shard_at_rest('dummy', 1, "dest1/test_sharding")
        doc1 = {'x': 1, 'y': 1}
        doc1['_id'] = self.db1.dummy.insert(doc1)

        api.start_migration('dummy', 1, "dest2/test_sharding")

        manager = Mock(insert_throttle=None, insert_batch_size=1000)
        sharder._do_copy('dummy', 1, manager)

        # The data should now be on the second database
        doc2, = self.db2.dummy.find({})
        self.assertEqual(doc1, doc2)

    def test_sync_after_copy(self):
        api.set_shard_at_rest('dummy', 1, "dest1/test_sharding")
        api.start_migration('dummy', 1, "dest2/test_sharding")

        # Mimic the state the shard would be in after a document was copied
        # from one location to another
        doc1 = {'x': 1, 'y': 1}
        doc1['_id'] = self.db1.dummy.insert(doc1)
        self.db2.dummy.insert(doc1)

        # Get the initial oplog position, do an update and then sync from the
        # initial position
        initial_oplog_pos = sharder._get_oplog_pos('dummy', 1)
        self.db1.dummy.update({'x': 1}, {'$inc': {'y': 1}})
        api.set_shard_to_migration_status(
            'dummy', 1, api.ShardStatus.MIGRATING_SYNC)
        sharder._sync_from_oplog('dummy', 1, initial_oplog_pos)

        # The data on the second database should now reflect the update that
        # went through
        doc2, = self.db2.dummy.find({})
        self.assertEqual(2, doc2['y'])

    def test_delete_after_migration(self):
        api.set_shard_at_rest('dummy', 1, "dest1/test_sharding")
        api.start_migration('dummy', 1, "dest2/test_sharding")

        # Mimic the state the shard would be in after a document was copied
        # from one location to another
        doc1 = {'x': 1, 'y': 1}
        doc1['_id'] = self.db1.dummy.insert(doc1)
        self.db2.dummy.insert(doc1)

        api.set_shard_to_migration_status(
            'dummy', 1, api.ShardStatus.POST_MIGRATION_DELETE)
        manager = Mock(delete_throttle=None, delete_batch_size=1000)
        sharder._delete_source_data('dummy', 1, manager)

        # The data on the first database should now be gone and the data
        # on the second database should be ok.
        self.assertEqual(0, self.db1.dummy.find({}).count())
        doc1_actual, = self.db2.dummy.find({})
        self.assertEqual(doc1, doc1_actual)

    def test_sync_ignores_other_collection(self):
        api.set_shard_at_rest('dummy', 1, "dest1/test_sharding")
        api.start_migration('dummy', 1, "dest2/test_sharding")

        # Mimic the state the shard would be in after a document was copied
        # from one location to another
        doc1 = {'x': 1, 'y': 1}
        doc1['_id'] = self.db1.dummy.insert(doc1)
        self.db2.dummy.insert(doc1)

        # Get the initial oplog position, do an update to a different collection
        # and then sync from the initial position
        initial_oplog_pos = sharder._get_oplog_pos('dummy', 1)
        self.db1.other_coll.insert(doc1)
        self.db1.other_coll.update({'x': 1}, {'$inc': {'y': 1}})
        api.set_shard_to_migration_status(
            'dummy', 1, api.ShardStatus.MIGRATING_SYNC)
        sharder._sync_from_oplog('dummy', 1, initial_oplog_pos)

        # The data on the second database should be in the same state as it
        # was before
        doc2, = self.db2.dummy.find({})
        self.assertEqual(1, doc2['y'])

    def test_sync_uses_correct_connection(self):
        """This tests for a bug found during a rollout. The connection for the
        metadata was assumed to be the same connection as the source data was
        going to be coming from. This is *not* always the case.
        """
        # To test this a migration from new to old will expose the bug
        api.set_shard_at_rest('dummy', 1, "dest2/test_sharding")
        api.start_migration('dummy', 1, "dest1/test_sharding")

        # Mimic the state the shard would be in after a document was copied
        # from one location to another
        doc1 = {'x': 1, 'y': 1}
        doc1['_id'] = self.db1.dummy.insert(doc1)
        self.db2.dummy.insert(doc1)

        # Get the initial oplog position, do an update and then sync from the
        # initial position
        initial_oplog_pos = sharder._get_oplog_pos('dummy', 1)
        self.db2.dummy.update({'x': 1}, {'$inc': {'y': 1}})
        api.set_shard_to_migration_status(
            'dummy', 1, api.ShardStatus.MIGRATING_SYNC)
        sharder._sync_from_oplog('dummy', 1, initial_oplog_pos)

        # The data on the first database should now reflect the update that
        # went through
        doc2, = self.db1.dummy.find({})
        self.assertEqual(2, doc2['y'])
