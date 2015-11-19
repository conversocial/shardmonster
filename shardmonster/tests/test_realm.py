import time
from mock import patch

from shardmonster.api import activate_caching, ensure_realm_exists
from shardmonster.tests.base import ShardingTestCase
from shardmonster.realm import _get_realm_coll, get_realm_by_name


class TestRealmCreation(ShardingTestCase):
    def test_ensure_realm_exists(self):
        # Trying to get a none-existent realm should blow up
        with self.assertRaises(Exception) as catcher:
            realm = get_realm_by_name('made-up-realm')
        self.assertEquals(
            catcher.exception.message,
            'Realm with name made-up-realm does not exist')

        ensure_realm_exists('some_realm', 'some_field', 'cluster-1/db')
        realm = get_realm_by_name('some_realm')
        self.assertEquals('some_realm', realm['name'])

        # Try creating the realm again, ensure it doesn't blow up or create a
        # duplicate
        ensure_realm_exists('some_realm', 'some_field', 'cluster-1/db')
        realm = get_realm_by_name('some_realm')
        self.assertEquals('some_realm', realm['name'])

        coll = _get_realm_coll()
        self.assertEquals(2, coll.count()) # One realm exists due to test base


    def test_ensure_changing_realm_breaks(self):
        ensure_realm_exists('some_realm', 'some_field', 'cluster-1/db')

        with self.assertRaises(Exception) as catcher:
            ensure_realm_exists(
                'some_realm', 'some_other_field',
                'cluster-1/db')
        self.assertEquals(
            catcher.exception.message, 'Cannot change realm')


class TestGetRealm(ShardingTestCase):
    def setUp(self):
        super(TestGetRealm, self).setUp()
        self._cache_length = 0.05
        activate_caching(self._cache_length)


    def tearDown(self):
        super(TestGetRealm, self).tearDown()
        # Deactivate caching by setting a 0 timeout
        activate_caching(0)


    @patch('shardmonster.realm._get_realm_coll')
    def test_caching(self, mock_get_realm_coll):
        shard_data = {'shard_field': 'domain'}
        mock_get_realm_coll.return_value.find.return_value = [shard_data]

        result = get_realm_by_name('bob')
        self.assertEquals(shard_data, result)
        self.assertEquals(1, mock_get_realm_coll.call_count)

        result = get_realm_by_name('bob')
        self.assertEquals(shard_data, result)
        self.assertEquals(1, mock_get_realm_coll.call_count)
        
        time.sleep(self._cache_length * 2)
        result = get_realm_by_name('bob')
        self.assertEquals(shard_data, result)
        self.assertEquals(2, mock_get_realm_coll.call_count)
