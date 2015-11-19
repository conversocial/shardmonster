import test_settings
from shardmonster import api, operations
from shardmonster.tests.base import ShardingTestCase

MAX_INT32 = pow(2, 32) - 1

class TestDistributorIntegration(ShardingTestCase):
    def test_multishard_find(self):
        return
        api.create_realm(
            'hashed', 'x', 'hashed',
            'dest1/%s' % test_settings.CONN1['db_name'],
            shard_type='hash_range')

        api.set_shard_at_rest(
            'hashed', [0, MAX_INT32 / 2], 'dest1/test_sharding')
        api.set_shard_at_rest(
            'hashed', [(MAX_INT32 / 2) + 1, MAX_INT32], 'dest2/test_sharding')

        # These hashes calculated via running xxhash on command line
        doc1 = {'x': 1, 'y': 1, '__hash': 3068971186}
        doc2 = {'x': 2, 'y': 1, '__hash': 205742900}
        self.db1.dummy.insert(doc1)
        self.db2.dummy.insert(doc2)

        c = operations.multishard_find('dummy', {'y': 1})
        results = sorted(list(c), key=lambda d: d['x'])
        print results
        self.assertEquals([doc1, doc2], results)

        results = operations.multishard_find('dummy', {'x': 1})
        self.assertEquals([doc1], results)

        results = operations.multishard_find('dummy', {'x': 2})
        self.assertEquals([doc2], results)
