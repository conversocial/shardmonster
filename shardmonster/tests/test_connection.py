import unittest

from mock import call, Mock, patch

import test_settings
from shardmonster.connection import (
    configure_controller, get_cluster_uri, _get_cluster_coll,
    get_controlling_db, ensure_cluster_exists)
from shardmonster.tests.base import ShardingTestCase


class TestConfigureController(unittest.TestCase):

    @patch('shardmonster.connection._connect_to_mongo', autospec=True)
    def test_connects_on_demand_only(self, mock_connect):
        mock_db = Mock()
        mock_connect.return_value = {
            test_settings.CONTROLLER['db_name']: mock_db
        }
        configure_controller(
            test_settings.CONTROLLER['uri'],
            test_settings.CONTROLLER['db_name'],
        )
        # _connect_to_mongo() shouldn't be called yet
        self.assertFalse(mock_connect.called)
        # get_controlling_db() should result in a call to _connect_to_mongo()
        db = get_controlling_db()
        self.assertEqual(
            [call(test_settings.CONTROLLER['uri'])],
            mock_connect.mock_calls
        )
        self.assertIs(mock_db, db)


class TestCluster(ShardingTestCase):
    def test_ensure_cluster_exists(self):
        # Trying to get a none-existent realm should blow up
        with self.assertRaises(Exception) as catcher:
            uri = get_cluster_uri('best-cluster')
        self.assertEquals(
            catcher.exception.message,
            'Cluster best-cluster has not been configured')

        ensure_cluster_exists(
            'best-cluster', 'mongodb://localhost:27017')
        uri = get_cluster_uri('best-cluster')
        self.assertEquals('mongodb://localhost:27017', uri)

        # Try creating the cluster again, ensure it doesn't blow up or create a
        # duplicate
        ensure_cluster_exists(
            'best-cluster', 'mongodb://localhost:27017')
        uri = get_cluster_uri('best-cluster')
        self.assertEquals('mongodb://localhost:27017', uri)

        coll = _get_cluster_coll()
        # Two clusters exist due to the base class
        self.assertEquals(3, coll.count())

    # TODO Changing clusters
