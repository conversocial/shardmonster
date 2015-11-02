from shardmonster.connection import (
    get_cluster_uri, _get_cluster_coll, ensure_cluster_exists)
from shardmonster.tests.base import ShardingTestCase


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
