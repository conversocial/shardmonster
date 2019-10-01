from pymongo import MongoClient

from shardmonster import connection
from shardmonster.hidden_secondaries import (
    HiddenSecondaryError,
    configure_hidden_secondary,
    get_hidden_secondary_connection,
    close_connections_to_hidden_secondaries
)
from shardmonster.tests import mock
from shardmonster.tests.base import ShardingTestCase, wait_for_oplog_to_catch_up


class ConfigureHiddenSecondaryTestCase(ShardingTestCase):

    def assert_hidden_secondary(self, cluster_name, secondary_host):
        db = connection.get_controlling_db()
        cluster = db.clusters.find_one({'name': cluster_name})
        self.assertEqual(cluster['hidden_secondary_host'], secondary_host)

    def test_sets_host_when_unset(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')
        self.assert_hidden_secondary('dest1', 'replica_1h:27017')

    def test_updates_host_when_already_set(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')
        configure_hidden_secondary('dest1', 'foo:27017')
        self.assert_hidden_secondary('dest1', 'foo:27017')


class GetHiddenSecondaryConnectionTestCase(ShardingTestCase):

    def test_successful_when_hidden_secondary_is_configured_and_exists(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')
        hidden_secondary = get_hidden_secondary_connection('dest1')
        self.assertIsInstance(hidden_secondary, MongoClient)
        self.assertEqual(hidden_secondary.address, ('replica_1h', 27017))

    def test_raises_error_when_hidden_secondary_for_cluster_is_not_configured(self):
        with self.assertRaises(HiddenSecondaryError):
            get_hidden_secondary_connection('dest1')

    def test_raises_error_when_hidden_secondary_for_cluster_is_missing(self):
        configure_hidden_secondary('dest1', 'incorrect-host:27017')
        with self.assertRaises(HiddenSecondaryError):
            get_hidden_secondary_connection('dest1')

    def test_raises_error_when_hidden_secondary_for_cluster_is_not_hidden(self):
        configure_hidden_secondary('dest1', 'replica_1b:27017')
        with self.assertRaises(HiddenSecondaryError):
            get_hidden_secondary_connection('dest1')

    def test_successive_calls_return_cached_client(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')
        hidden_secondary = get_hidden_secondary_connection('dest1')
        hidden_secondary_2 = get_hidden_secondary_connection('dest1')
        self.assertIs(hidden_secondary, hidden_secondary_2)

    def test_can_query_with_hidden_secondary(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')

        inserted = {'hello': 'world'}
        self.db1.client[self.db1.name].dummy.insert_one(inserted)

        hidden_secondary = get_hidden_secondary_connection('dest1')
        wait_for_oplog_to_catch_up(hidden_secondary, self.db1.client)

        found = hidden_secondary[self.db1.name].dummy.find_one()
        self.assertEqual(found, inserted)


class CloseConnectionToHiddenSecondariesTestCase(ShardingTestCase):

    def test_closes_connections(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')
        hidden_secondary = get_hidden_secondary_connection('dest1')
        hidden_secondary.close = mock.Mock()

        close_connections_to_hidden_secondaries()

        hidden_secondary.close.assert_called_once_with()

    def test_removes_connection_from_cache(self):
        configure_hidden_secondary('dest1', 'replica_1h:27017')
        hidden_secondary = get_hidden_secondary_connection('dest1')

        close_connections_to_hidden_secondaries()
        hidden_secondary_2 = get_hidden_secondary_connection('dest1')

        self.assertIsNot(hidden_secondary, hidden_secondary_2)
