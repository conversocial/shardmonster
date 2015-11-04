from shardmonster.api import (
    ensure_realm_exists, set_shard_at_rest, start_migration)
from shardmonster.metadata import _get_realm_for_collection, _get_realm_coll
from shardmonster.tests.base import ShardingTestCase


class TestRealm(ShardingTestCase):
    def test_ensure_realm_exists(self):
        # Trying to get a none-existent realm should blow up
        with self.assertRaises(Exception) as catcher:
            realm = _get_realm_for_collection('some_collection')
        self.assertEquals(
            catcher.exception.message,
            'Realm for collection some_collection does not exist')

        ensure_realm_exists(
            'some_realm', 'some_field', 'some_collection', 'cluster-1/db')
        realm = _get_realm_for_collection('some_collection')
        self.assertEquals('some_realm', realm['name'])

        # Try creating the realm again, ensure it doesn't blow up or create a
        # duplicate
        ensure_realm_exists(
            'some_realm', 'some_field', 'some_collection', 'cluster-1/db')
        realm = _get_realm_for_collection('some_collection')
        self.assertEquals('some_realm', realm['name'])

        coll = _get_realm_coll()
        self.assertEquals(2, coll.count()) # One realm exists due to test base


    def test_ensure_changing_realm_breaks(self):
        ensure_realm_exists(
            'some_realm', 'some_field', 'some_collection', 'cluster-1/db')

        with self.assertRaises(Exception) as catcher:
            ensure_realm_exists(
                'some_realm', 'some_other_field', 'some_collection',
                'cluster-1/db')
        self.assertEquals(
            catcher.exception.message, 'Cannot change realm')


    def test_one_realm_per_collection(self):
        ensure_realm_exists(
            'some_realm', 'some_field', 'some_collection', 'cluster-1/db')

        with self.assertRaises(Exception) as catcher:
            ensure_realm_exists(
                'some_other_realm', 'some_other_field', 'some_collection',
                'cluster-1/db')
        self.assertEquals(
            catcher.exception.message,
            'Realm for collection some_collection already exists')


    def test_cannot_move_to_same_location(self):
        ensure_realm_exists(
            'some_realm', 'some_field', 'some_collection', 'cluster-1/db')

        set_shard_at_rest('some_realm', 1, 'cluster-1/db')

        with self.assertRaises(Exception) as catcher:
            start_migration('some_realm', 1, 'cluster-1/db')
        self.assertEquals(
            catcher.exception.message, 'Shard is already at cluster-1/db')
