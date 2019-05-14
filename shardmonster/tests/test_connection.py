from __future__ import absolute_import

import unittest

from .mock import Mock, call

import test_settings
import shardmonster.connection
from shardmonster.connection import (
    get_cluster_uri, _get_cluster_coll, ensure_cluster_exists, \
    register_post_connect, connect_to_controller,
    configure_controller, get_controlling_db)
from shardmonster.tests.base import ShardingTestCase


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
