import time
from mock import patch
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

        store = metadata.ShardMetadataStore({'realm': 'dummy-realm'})
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_shard_metadata, actual_shard_metadata)
        self.assertEquals(1, mock_query.call_count)
    
        # Do another query and ensure we have used a cached value
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_shard_metadata, actual_shard_metadata)
        self.assertEquals(1, mock_query.call_count)

        # Now sleep for longer the cache timeout and ensure we do another call
        time.sleep(self._cache_length * 2)
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_shard_metadata, actual_shard_metadata)
        self.assertEquals(2, mock_query.call_count)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_single_shard_in_flux(self, mock_query):
        expected_shard_metadata = {
            'status': metadata.ShardStatus.MIGRATING_SYNC, 'shard_key': 1}
        mock_query.return_value = [expected_shard_metadata.copy()]

        store = metadata.ShardMetadataStore({'realm': 'dummy-realm'})
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_shard_metadata, actual_shard_metadata)
        self.assertEquals(1, mock_query.call_count)
    
        # Do another query and ensure we have NOT used a cached value
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_shard_metadata, actual_shard_metadata)
        self.assertEquals(2, mock_query.call_count)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_all_shards(self, mock_query):
        expected_metadata_1 = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 1}
        expected_metadata_2 = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 2}
        mock_query.return_value = [
            expected_metadata_1.copy(), expected_metadata_2.copy()]

        store = metadata.ShardMetadataStore({'realm': 'dummy-realm'})
        actual_metadata = store.get_all_shard_metadata()
        self.assertEquals(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEquals(1, mock_query.call_count)
    
        # Do another query and ensure we have used a cached value
        actual_metadata = store.get_all_shard_metadata()
        self.assertEquals(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEquals(1, mock_query.call_count)

        # Do a query for a single shard and ensure we get the cache value
        actual_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_metadata_1, actual_metadata)

        # Now sleep for longer the cache timeout and ensure we do another call
        time.sleep(self._cache_length * 2)
        mock_query.return_value = [expected_metadata_1.copy()]
        actual_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_metadata_1, actual_metadata)
        self.assertEquals(2, mock_query.call_count)

        # The previous call will have only refreshed a single shard. Therefore,
        # a global call will result in another query
        actual_metadata = store.get_all_shard_metadata()
        self.assertEquals(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEquals(3, mock_query.call_count)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_cache_all_shards_in_flux(self, mock_query):
        expected_metadata_1 = {
            'status': metadata.ShardStatus.MIGRATING_SYNC, 'shard_key': 1}
        expected_metadata_2 = {
            'status': metadata.ShardStatus.AT_REST, 'shard_key': 2}
        mock_query.return_value = [
            expected_metadata_1.copy(), expected_metadata_2.copy()]

        store = metadata.ShardMetadataStore({'realm': 'dummy-realm'})
        actual_metadata = store.get_all_shard_metadata()
        self.assertEquals(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEquals(1, mock_query.call_count)
        
        # A query of the second shard should hit the cache
        actual_metadata = store.get_single_shard_metadata(2)
        self.assertEquals(expected_metadata_2, actual_metadata)
        self.assertEquals(1, mock_query.call_count)
    
        # Do another query and we should end up refrshing just a single shard
        mock_query.return_value = [expected_metadata_1.copy()]

        actual_metadata = store.get_all_shard_metadata()
        self.assertEquals(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEquals(2, mock_query.call_count)
        mock_query.assert_called_with(1)

        # Another full query should skip the cache
        actual_metadata = store.get_all_shard_metadata()
        self.assertEquals(
            {1: expected_metadata_1, 2: expected_metadata_2},
            actual_metadata)
        self.assertEquals(3, mock_query.call_count)


    def test_query(self):
        api.create_realm(
            'dummy-realm', 'some_field', 'dummy_collection',
            'cluster-1/%s' % test_settings.CONN1['db_name'])
        api.set_shard_at_rest('dummy-realm', 1, 'dest1/some_db')
        expected_metadata = {
            'shard_key': 1,
            'location': 'dest1/some_db',
            'realm': 'dummy-realm'
        }
        
        def _trim_results(docs):
            return [{
                'shard_key': doc['shard_key'],
                'location': doc['location'],
                'realm': doc['realm']
            } for doc in docs]


        store = metadata.ShardMetadataStore({'name': 'dummy-realm'})

        results = _trim_results(store._query_shards_collection())
        self.assertEquals([expected_metadata], results)

        results = _trim_results(store._query_shards_collection(1))
        self.assertEquals([expected_metadata], results)

        results = _trim_results(store._query_shards_collection(2))
        self.assertEquals([], results)


        store = metadata.ShardMetadataStore({
            'name': 'some-other-realm'})
        results = _trim_results(store._query_shards_collection())
        self.assertEquals([], results)

        results = _trim_results(store._query_shards_collection(1))
        self.assertEquals([], results)


    @patch('shardmonster.metadata.ShardMetadataStore._query_shards_collection')
    def test_default_location(self, mock_query):
        default_dest = 'cluster-1/%s' % test_settings.CONN1['db_name']
        expected_shard_metadata = {
            'status': metadata.ShardStatus.AT_REST,
            'location': default_dest,
            'realm': 'dummy-realm',
        }
        mock_query.return_value = []

        store = metadata.ShardMetadataStore({
            'name': 'dummy-realm',
            'default_dest': default_dest})
        actual_shard_metadata = store.get_single_shard_metadata(1)
        self.assertEquals(expected_shard_metadata, actual_shard_metadata)
        self.assertEquals(1, mock_query.call_count)


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
        self.assertEquals(meta, expected_meta)

        all_locations = metadata._get_all_locations_for_realm(realm)
        self.assertEquals([], all_locations['cluster-1/some_db'].contains)
        self.assertEquals([], all_locations['cluster-1/some_db'].excludes)
        self.assertEquals([1], all_locations['dest2/some_db'].contains)
        self.assertEquals([], all_locations['dest2/some_db'].excludes)


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
        self.assertEquals(shard_data, result)
        self.assertEquals(1, mock_get_realm_coll.call_count)

        result = metadata._get_realm_for_collection('bob')
        self.assertEquals(shard_data, result)
        self.assertEquals(1, mock_get_realm_coll.call_count)
        
        time.sleep(self._cache_length * 2)
        result = metadata._get_realm_for_collection('bob')
        self.assertEquals(shard_data, result)
        self.assertEquals(2, mock_get_realm_coll.call_count)


class TestLocationMetadata(TestCase):
    def test_repr(self):
        meta = metadata.LocationMetadata("somewhere/banana")
        self.assertEquals(
            "LocationMetadata(somewhere/banana, contains: [], excludes: [])",
            repr(meta))
        meta.contains += [1, 2, 3, 4, 5, 6]
        meta.excludes.append(9)
        self.assertEquals(
            "LocationMetadata(somewhere/banana, "\
            "contains: [1,2,3,4,5...], excludes: [9])",
            repr(meta))
