"""
This is an integration test suite that attempts to move data between two shards
a few times whilst also doing inserts and updates. It then verifies that the
data makes sense at the very end.

This works by creating documents of the form:

    {
        account_id: 1,
        some_counter: 1,
        some_key: 1,
    }

It then performs sharding on account ID whilst doing changes to some_counter.
"""
import sys
import time


import test_settings
from shardmonster import api, sharder
from shardmonster.tests.base import ShardingTestCase


class TestMovedDuringCopy(ShardingTestCase):
    """Tests for a specific scenario where data is moved *during* a copy and due
    to the use of an index for iteration the data is missed by the copy.
    """
    def setUp(self):
        super(TestMovedDuringCopy, self).setUp()
        self._create_indices()
        self._make_collections_shard_aware()
        api.activate_caching(0.5)


    def tearDown(self):
        super(TestMovedDuringCopy, self).tearDown()
        # Deactivate caching by setting a 0 timeout
        api.activate_caching(0)


    def _make_collections_shard_aware(self):
        self.unwrapped_dummy_1 = self.db1.dummy
        self.unwrapped_dummy_2 = self.db2.dummy
        self.db1.dummy = api.make_collection_shard_aware('dummy')
        self.db2.dummy = api.make_collection_shard_aware('dummy')
        self.sharded_coll = self.db1.dummy


    def _create_indices(self):
        self.db1.dummy.ensure_index(
            [('account_id', 1), ('some_key', 1)],
            unique=True)
        self.db2.dummy.ensure_index(
            [('account_id', 1), ('some_key', 1)],
            unique=True)


    def _prepare_account_data(self, db, account_id, key_range):
        records = [{
                'account_id': account_id,
                'some_key': i,
                'counter': 1,
            } for i in key_range]

        for record in records[:-1]:
            db.dummy.insert(record, w=0)
        db.dummy.insert(records[-1])
        return records


    def _prepare_realms(self):
        api.create_realm('dummy', 'account_id', 'dummy', 'dest1/test_sharding')

    def _modify_data(self, account_1):
        # Invert all the data (starting at the end) on the some_key field to
        # effectively invert the index
        i = 0
        for record in reversed(account_1):
            i += 1
            self.sharded_coll.update(
                {'account_id': 1, 'some_key': record['some_key']},
                {'$set': {'some_key': record['some_key'] * -1}},
            )
            record['some_key'] *= -1

    def _verify_end_state(self, account_1, original_1, original_2):
        # Fetch the data from the second server and check all the records match
        account_1_actual = list(original_2.find({'account_id': 1}))
        account_1_actual = list(sorted(
            account_1_actual, key=lambda r: -r['some_key']))

        self.assertEquals(account_1, account_1_actual)

        # There should be no data for the 1st account at the source
        self.assertEquals(0, original_1.find({'account_id': 1}).count())

    def test_index_inversion(self):
        num_records = 200
        api.set_shard_at_rest('dummy', 1, "dest1/test_sharding")

        account_1 = self._prepare_account_data(
            self.db1, 1, xrange(0, num_records))

        shard_manager = sharder._begin_migration(
            'dummy', 1, "dest2/test_sharding")
        self._modify_data(account_1)
        while not shard_manager.is_finished():
            time.sleep(0.01)
        self._verify_end_state(
            account_1, self.unwrapped_dummy_1, self.unwrapped_dummy_2)


class TestWholeThing(ShardingTestCase):
    def setUp(self):
        super(TestWholeThing, self).setUp()
        self._create_indices()
        self._make_collections_shard_aware()
        api.activate_caching(0.5)


    def tearDown(self):
        super(TestWholeThing, self).tearDown()
        # Deactivate caching by setting a 0 timeout
        api.activate_caching(0)

 
    def _make_collections_shard_aware(self):
        self.unwrapped_dummy_1 = self.db1.dummy
        self.unwrapped_dummy_2 = self.db2.dummy
        self.db1.dummy = api.make_collection_shard_aware('dummy')
        self.db2.dummy = api.make_collection_shard_aware('dummy')
        self.sharded_coll = self.db1.dummy


    def _create_indices(self):
        self.db1.dummy.ensure_index(
            [('account_id', 1), ('some_key', 1)],
            unique=True)
        self.db2.dummy.ensure_index(
            [('account_id', 1), ('some_key', 1)],
            unique=True)


    def _prepare_account_data(self, db, account_id, key_range):
        records = []
        for i in key_range:
            record = {
                'account_id': account_id,
                'some_key': i,
                'counter': 1
            }
            record['_id'] = db.dummy.insert(record)
            records.append(record)
        return records


    def _prepare_realms(self):
        api.create_realm('dummy', 'account_id', 'dummy', 'dest1/test_sharding')
    
    def _modify_data(
            self, account_1, account_2, start_insert_id, number_inserts):
        # Increment all the counters a few times
        for cnt in range(3):
            for record in account_1:
                self.sharded_coll.update(
                    {'account_id': 1, 'some_key': record['some_key']},
                    {'$inc': {'counter': 1}},
                )
                record['counter'] += 1

        # Delete all the counters that are divisible by 97 (including 0)
        to_delete = []
        for record in account_1:
            if record['some_key'] % 97 == 0:
                to_delete.append(record)
        for record in to_delete:
            account_1.remove(record)
            self.sharded_coll.remove(
                {'account_id': 1, 'some_key': record['some_key']})

        # Add some additional records
        account_1 += self._prepare_account_data(
            self.db1, 1,
            xrange(start_insert_id, start_insert_id + number_inserts))
        account_2 += self._prepare_account_data(
            self.db1, 2,
            xrange(start_insert_id, start_insert_id + number_inserts))

    def _verify_end_state(self, account_1, account_2, original_1, original_2):
        # Fetch the data from the second server and check all the records match
        account_1_actual = list(original_2.find({'account_id': 1}))
        account_1_actual = list(sorted(
            account_1_actual, key=lambda r: r['some_key']))

        if account_1 != account_1_actual:
            print 'Account 1 Expected'
            print '------------------'
            for doc in account_1:
                print '(%s, %s) -> %d' % (
                    doc['account_id'], doc['some_key'], doc['counter'])
            print 'Account 1 Actual'
            print '----------------'
            for doc in account_1_actual:
                print '(%s, %s) -> %d    (%s)' % (
                    doc['account_id'], doc['some_key'], doc['counter'], doc['_id'])
        self.assertEquals(account_1, account_1_actual)

        # There should be no data for the 1st account at the source
        self.assertEquals(0, original_1.find({'account_id': 1}).count())

    def _attempt_migration(self, num_records):
        api.set_shard_at_rest('dummy', 1, "dest1/test_sharding")
        api.set_shard_at_rest('dummy', 2, "dest1/test_sharding")

        account_1 = self._prepare_account_data(
            self.db1, 1, xrange(0, num_records))
        account_2 = self._prepare_account_data(
            self.db1, 2, xrange(0, num_records))

        shard_manager = sharder._begin_migration(
            'dummy', 1, "dest2/test_sharding")
        self._modify_data(account_1, account_2, num_records, num_records)
        while not shard_manager.is_finished():
            time.sleep(0.01)
        self._verify_end_state(
            account_1, account_2, self.unwrapped_dummy_1, self.unwrapped_dummy_2)

        # Check that the data for the other account has remained intact and in
        # the same place
        account_2_actual = list(self.unwrapped_dummy_1.find({'account_id': 2}))
        account_2_actual = list(sorted(
            account_2_actual, key=lambda r: r['some_key']))

        self.assertEquals(account_2, account_2_actual)

        # Now migrate back to the source
        print 'Now migrate backwards...'
        shard_manager = sharder._begin_migration(
            'dummy', 1, "dest1/test_sharding")
        self._modify_data(account_1, account_2, num_records * 2, num_records)
        while not shard_manager.is_finished():
            time.sleep(0.01)
        self._verify_end_state(
            account_1, account_2, self.unwrapped_dummy_2, self.unwrapped_dummy_1)


# Make the tests dynamically so that test runners break it up :)
for i, records in enumerate(test_settings.INTEGRATION_TEST_RUNS):
    def _make_test(run, run_records):
        def test_method(self):
            start_time = time.time()
            msg = '[%d/%d] Testing sharding with %d initial documents\n' % (
                run, len(test_settings.INTEGRATION_TEST_RUNS), run_records
            )
            sys.stderr.write(msg)

            self._attempt_migration(run_records)

            end_time = time.time()
            sys.stderr.write(
                '  Took %.2fs\n' % (end_time - start_time))
    
        name = 'test_%05d_%05d' % (run_records, i)
        test_method.__doc__ = 'Migration of %d docs, run #%d' % (run_records, i)
        return name, test_method
    name, tester = _make_test(i + 1, records)
    setattr(TestWholeThing, name, tester)

# Set tester to None at the end so that nose doesn't pick this up as a test to
# run
tester = None
