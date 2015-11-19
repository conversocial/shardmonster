from shardmonster.api import (
    ensure_realm_exists, set_shard_at_rest, start_migration, where_is)
from shardmonster.tests.base import ShardingTestCase


class TestRealm(ShardingTestCase):
    def test_cannot_move_to_same_location(self):
        ensure_realm_exists('some_realm', 'some_field', 'dest1/db')

        set_shard_at_rest('some_realm', 1, 'dest1/db')

        with self.assertRaises(Exception) as catcher:
            start_migration('some_realm', 1, 'dest1/db')
        self.assertEquals(
            catcher.exception.message, 'Shard is already at dest1/db')


    def test_set_shard_at_rest_bad_location(self):
        ensure_realm_exists('some_realm', 'some_field', 'cluster-1/db')

        with self.assertRaises(Exception) as catcher:
            set_shard_at_rest('some_realm', 1, 'bad-cluster/db')
        self.assertEquals(
            catcher.exception.message,
            'Cluster bad-cluster has not been configured')


    def test_set_shard_at_rest_when_already_at_rest(self):
        ensure_realm_exists('some_realm', 'some_field', 'dest1/db')
        set_shard_at_rest('some_realm', 1, 'dest1/db')

        with self.assertRaises(Exception) as catcher:
            set_shard_at_rest('some_realm', 1, 'dest2/db')
        self.assertEquals(
            catcher.exception.message,
            'Shard with key 1 has already been placed. Use force=true if '
            'you really want to do this')

        # Forcing is should work
        set_shard_at_rest('some_realm', 1, 'dest2/db', force=True)


    def test_where_is(self):
        ensure_realm_exists('some_realm', 'some_field', 'dest1/db')
        set_shard_at_rest('some_realm', 1, 'dest2/db')

        # Specific location
        self.assertEquals('dest2/db', where_is('some_realm', 1))
        # Default location
        self.assertEquals('dest1/db', where_is('some_realm', 2))
