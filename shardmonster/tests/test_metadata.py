from __future__ import absolute_import

import time
from .mock import patch
from unittest import TestCase

import test_settings
from shardmonster import api, metadata
from shardmonster.tests.base import ShardingTestCase


class TestShardMetadataStore(ShardingTestCase):
    def setUp(self):
        super(TestShardMetadataStore, self).setUp()
        self._cache_length = 0.05
        api.activate_caching(self._cache_length)


    def tearDown(self):
        super(TestShardMetadataStore, self).tearDown()
        # Deactivate caching by setting a 0 timeout
        api.activate_caching(0)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_single_shard(self, mock_query):
        expected_shard_metadata = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 1}
        mock_query.return_value = [expected_shard_metadata.copy()]

        store = metadata.ShardMetadataStore('dummy-realm')
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_shard_metadata, actual_shard_metadata)
        self.assertEqual(1, mock_query.call_count)

        # Do another query and ensure we have used a cached value
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_shard_metadata, actual_shard_metadata)
        self.assertEqual(1, mock_query.call_count)

        # Now sleep for longer the cache timeout and ensure we do another call
        time.sleep(self._cache_length * 2)
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_shard_metadata, actual_shard_metadata)
        self.assertEqual(2, mock_query.call_count)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_single_shard_in_flux(self, mock_query):
        expected_shard_metadata = {
            'status': metadata.ShardStatus.MIGRATING_SYNC, 'shard_key': 1}
        mock_query.return_value = [expected_shard_metadata.copy()]

        store = metadata.ShardMetadataStore('dummy-realm')
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_shard_metadata, actual_shard_metadata)
        self.assertEqual(1, mock_query.call_count)

        # Do another query and ensure we have NOT used a cached value
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_shard_metadata, actual_shard_metadata)
        self.assertEqual(2, mock_query.call_count)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_all_shards(self, mock_query):
        expected_metadata_1 = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 1}
        expected_metadata_2 = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 2}
        mock_query.return_value = [
            expected_metadata_1.copy(), expected_metadata_2.copy()]

        store = metadata.ShardMetadataStore('dummy-realm')
        actual_metadata = store.get_all_shard_metadata()
        self.assertEqual(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEqual(1, mock_query.call_count)

        # Do another query and ensure we have used a cached value
        actual_metadata = store.get_all_shard_metadata()
        self.assertEqual(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEqual(1, mock_query.call_count)

        # Do a query for a single shard and ensure we get the cache value
        actual_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_metadata_1, actual_metadata)

        # Now sleep for longer the cache timeout and ensure we do another call
        time.sleep(self._cache_length * 2)
        mock_query.return_value = [expected_metadata_1.copy()]
        actual_metadata = store.get_single_shard_metadata(1)
        self.assertEqual(expected_metadata_1, actual_metadata)
        self.assertEqual(2, mock_query.call_count)

        # The previous call will have only refreshed a single shard. Therefore,
        # a global call will result in another query
        actual_metadata = store.get_all_shard_metadata()
        self.assertEqual(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEqual(3, mock_query.call_count)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_all_shards_in_flux(self, mock_query):
        expected_metadata_1 = {
            'status': metadata.ShardStatus.MIGRATING_SYNC, 'shard_key': 1}
        expected_metadata_2 = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 2}
        mock_query.return_value = [
            expected_metadata_1.copy(), expected_metadata_2.copy()]

        store = metadata.ShardMetadataStore('dummy-realm')
        actual_metadata = store.get_all_shard_metadata()
        self.assertEqual(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEqual(1, mock_query.call_count)

        # A query of the second shard should hit the cache
        actual_metadata = store.get_single_shard_metadata(2)
        self.assertEqual(expected_metadata_2, actual_metadata)
        self.assertEqual(1, mock_query.call_count)

        # Do another query and we should end up refrshing just a single shard
        mock_query.return_value = [expected_metadata_1.copy()]

        actual_metadata = store.get_all_shard_metadata()
        self.assertEqual(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEqual(2, mock_query.call_count)
        mock_query.assert_called_with(1)

        # Another full query should skip the cache
        actual_metadata = store.get_all_shard_metadata()
        self.assertEqual(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEqual(3, mock_query.call_count)

    def test_fetch_all_shards_from_metadata(self):
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection',
            'cluster-1/%s' % test_settings.CONN1['db_name'])
        api.set_shard_at_rest('dummy-realm', 1, 'dest1/some_db')

        entries = metadata.ShardMetadataStore('dummy-realm') \
                          ._query_shards_collection()
        self.assertEqual(entries[0]['shard_key'], 1)
        self.assertEqual(entries[0]['location'], 'dest1/some_db')
        self.assertEqual(entries[0]['realm'], 'dummy-realm')

    def test_empty_list_when_shard_is_missing(self):
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection',
            'cluster-1/%s' % test_settings.CONN1['db_name'])

        entries = list(metadata.ShardMetadataStore('dummy-realm') \
                               ._query_shards_collection(1))
        self.assertEqual(entries, [])

    def test_raise_when_collection_absent(self):
        with self.assertRaises(Exception):
            metadata.ShardMetadataStore('missing')._query_shards_collection()

    def test_default_location(self):
        default_dest = 'cluster-2/%s' % test_settings.CONN2['db_name']
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection', default_dest)
        api.set_shard_at_rest('dummy-realm', 1, 'dest2/some_db')

        entries = metadata.ShardMetadataStore('dummy-realm') \
                          ._query_shards_collection()
        self.assertEqual(entries[0]['location'], 'dest2/some_db')

    def test_raise_if_changing_shard_location_once_set(self):
        default_dest = 'cluster-2/%s' % test_settings.CONN2['db_name']
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection', default_dest)
        api.set_shard_at_rest('dummy-realm', 1, 'dest2/some_db')
        with self.assertRaises(Exception):
            api.set_shard_at_rest('dummy-realm', 1, 'dest1/some_db')

    def test_shard_location_does_not_change_even_when_forced(self):
        default_dest = 'cluster-2/%s' % test_settings.CONN2['db_name']
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection', default_dest)
        api.set_shard_at_rest('dummy-realm', 1, 'dest2/some_db')
        api.set_shard_at_rest('dummy-realm', 1, 'dest1/some_db', force=True)
        entries = metadata.ShardMetadataStore('dummy-realm') \
                          ._query_shards_collection()
        self.assertEqual(entries[0]['location'], 'dest1/some_db')

    def test_get_location_ordering(self):
        # Exposes a bug that was found in caching and default locations
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection',
            'cluster-1/some_db')
        api.set_shard_at_rest('dummy-realm', 1, 'dest2/some_db')
        realm = metadata._get_realm_for_collection('dummy_collection')
        meta = metadata._get_metadata_for_shard(realm, 2)
        expected_meta = {
            'status': metadata.ShardStatus.AT_REST,
            'realm': 'dummy-realm',
            'location': 'cluster-1/some_db'
        }
        self.assertEqual(meta, expected_meta)

        all_locations = metadata._get_all_locations_for_realm(realm)
        self.assertEqual([], all_locations['cluster-1/some_db'].contains)
        self.assertEqual([], all_locations['cluster-1/some_db'].excludes)
        self.assertEqual([1], all_locations['dest2/some_db'].contains)
        self.assertEqual([], all_locations['dest2/some_db'].excludes)


class TestGetRealm(ShardingTestCase):
    def setUp(self):
        super(TestGetRealm, self).setUp()
        self._cache_length = 0.05
        api.activate_caching(self._cache_length)


    def tearDown(self):
        super(TestGetRealm, self).tearDown()
        # Deactivate caching by setting a 0 timeout
        api.activate_caching(0)


    @patch('shardmonster.metadata._get_realm_coll')
    def test_caching(self, mock_get_realm_coll):
        shard_data = {'shard_field': 'domain'}
        mock_get_realm_coll.return_value.find.return_value = [shard_data]

        result = metadata._get_realm_for_collection('bob')
        self.assertEqual(shard_data, result)
        self.assertEqual(1, mock_get_realm_coll.call_count)

        result = metadata._get_realm_for_collection('bob')
        self.assertEqual(shard_data, result)
        self.assertEqual(1, mock_get_realm_coll.call_count)

        time.sleep(self._cache_length * 2)
        result = metadata._get_realm_for_collection('bob')
        self.assertEqual(shard_data, result)
        self.assertEqual(2, mock_get_realm_coll.call_count)


class TestLocationMetadata(TestCase):
    def test_repr(self):
        meta = metadata.LocationMetadata("somewhere/banana")
        self.assertEqual(
            "LocationMetadata(somewhere/banana, contains: [], excludes: [])",
            repr(meta))
        meta.contains += [1, 2, 3, 4, 5, 6]
        meta.excludes.append(9)
        self.assertEqual(
            "LocationMetadata(somewhere/banana, "\
            "contains: [1,2,3,4,5...], excludes: [9])",
            repr(meta))
