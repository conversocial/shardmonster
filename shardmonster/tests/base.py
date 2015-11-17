import pymongo
import unittest

import test_settings
from shardmonster import api, connection as connection_module, metadata


def _is_same_mongo(conn1, conn2):
    uri_info1 = pymongo.uri_parser.parse_uri(conn1['uri'])
    uri_info2 = pymongo.uri_parser.parse_uri(conn2['uri'])
    return set(uri_info1['nodelist']) == set(uri_info2['nodelist'])


class ShardingTestCase(unittest.TestCase):
    def setUp(self):
        self._prepare_connections()
        self._clean_data_before_tests()
        self._prepare_clusters()
        self._prepare_realms()
        api.create_indices()


    def tearDown(self):
        self.conn1.close()
        self.conn2.close()


    def _create_connection(self, conn_settings):
        conn = connection_module._connect_to_mongo(conn_settings['uri'])
        db_name = conn_settings['db_name']
        db = conn[db_name]
        return conn, db


    def _prepare_connections(self):
        if _is_same_mongo(test_settings.CONN1, test_settings.CONN2):
            raise Exception('Must use two different Mongo servers for testing!')
        self.conn1, self.db1 = self._create_connection(
            test_settings.CONN1)
        self.conn2, self.db2 = self._create_connection(
            test_settings.CONN2)
        api.connect_to_controller(
            test_settings.CONTROLLER['uri'],
            test_settings.CONTROLLER['db_name'],
        )

        # Wipe the connections that are in the cache in shardmonster
        for connection in connection_module._connection_cache.values():
            connection.close()
        connection_module._connection_cache = {}
        connection_module._cluster_uri_cache = {}
        api._collection_cache = {}
        metadata._metadata_stores = {}


    def _clean_data_before_tests(self):
        self.db1.client.drop_database(self.db1.name)
        self.db2.client.drop_database(self.db2.name)
        api._reset_sharding_info()


    def _prepare_clusters(self):
        api.add_cluster('dest1', test_settings.CONN1['uri'])
        api.add_cluster('dest2', test_settings.CONN2['uri'])


    def _prepare_realms(self):
        api.create_realm(
            'dummy', 'x', 'dummy',
            'dest1/%s' % test_settings.CONN1['db_name'])
