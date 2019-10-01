from __future__ import absolute_import

import unittest

from pymongo import MongoClient

from .mock import Mock, call

import shardmonster.connection
from shardmonster.connection import (
    get_cluster_uri, _get_cluster_coll, ensure_cluster_exists,
    register_post_connect, connect_to_controller,
    configure_controller, get_controlling_db, get_hidden_secondary_connection
)
from shardmonster.tests import settings as test_settings
from shardmonster.tests.base import ShardingTestCase, wait_for_oplog_to_catch_up


class TestCallbacks(unittest.TestCase):
    """Test post-connect callbacks."""

    def test_post_connect(self):
        mock_callback = Mock()
        register_post_connect(mock_callback)
        connect_to_controller(
            test_settings.CONTROLLER['uri'],
            test_settings.CONTROLLER['db_name'],
        )
        self.assertEqual([call()], mock_callback.mock_calls)


class TestConfigureController(unittest.TestCase):

    def setUp(self):
        shardmonster.connection._controlling_db = None
        shardmonster.connection._controlling_db_config = None

    def test_not_configured(self):
        with self.assertRaises(Exception) as cm:
            get_controlling_db()
        self.assertEqual(
            'Call connect_to_controller or configure_controller '
            'before attempting to get a connection',
            str(cm.exception))

    def test_configured(self):
        configure_controller(
            test_settings.CONTROLLER['uri'],
            test_settings.CONTROLLER['db_name'],
        )
        self.assertIsNone(shardmonster.connection._controlling_db)
        get_controlling_db()
        self.assertIsNotNone(shardmonster.connection._controlling_db)


class TestCluster(ShardingTestCase):
    def test_ensure_cluster_exists(self):
        # Trying to get a none-existent realm should blow up
        with self.assertRaises(Exception) as catcher:
            uri = get_cluster_uri('best-cluster')
        self.assertEqual(
            str(catcher.exception),
            'Cluster best-cluster has not been configured')

        ensure_cluster_exists(
            'best-cluster', 'mongodb://localhost:27017')
        uri = get_cluster_uri('best-cluster')
        self.assertEqual('mongodb://localhost:27017', uri)

        # Try creating the cluster again, ensure it doesn't blow up or create a
        # duplicate
        ensure_cluster_exists(
            'best-cluster', 'mongodb://localhost:27017')
        uri = get_cluster_uri('best-cluster')
        self.assertEqual('mongodb://localhost:27017', uri)

        coll = _get_cluster_coll()
        # Two clusters exist due to the base class
        self.assertEqual(3, coll.count())

    # TODO Changing clusters


class HiddenSecondaryConnectionTestCase(ShardingTestCase):

    def test_successfully_gets_hidden_secondary_connection(self):
        hidden_secondary = get_hidden_secondary_connection(self.db1.client)
        self.assertEqual(
            hidden_secondary,
            MongoClient('mongodb://replica_1h:27017')
        )
        self.assertEqual(hidden_secondary.address, ('replica_1h', 27017))

    def test_successive_calls_return_cached_client(self):
        hidden_secondary = get_hidden_secondary_connection(self.db1.client)
        hidden_secondary_2 = get_hidden_secondary_connection(self.db1.client)
        self.assertIs(hidden_secondary, hidden_secondary_2)

    def test_replica_sets_without_a_hidden_secondary_return_none(self):
        hidden_secondary = get_hidden_secondary_connection(self.db2.client)
        self.assertIsNone(hidden_secondary)

    def test_can_query_with_hidden_secondary(self):
        inserted = {'hello': 'world'}
        self.db1.client[self.db1.name].dummy.insert_one(inserted)

        hidden_secondary = get_hidden_secondary_connection(self.db1.client)
        wait_for_oplog_to_catch_up(hidden_secondary, self.db1.client)

        found = hidden_secondary[self.db1.name].dummy.find_one()
        self.assertEqual(found, inserted)
