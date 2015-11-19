import bson
import numbers
import xxhash

from shardmonster.metadata import _get_shard_metadata_for_realm

# TODO hook into upsert and insert
# TODO caching
class HashRangeDistributor(object):
    def _hash_key(self, shard_key):
        if isinstance(shard_key, bson.ObjectId):
            to_shard = shard_key.binary
        elif isinstance(shard_key, numbers.Integral):
            to_shard = str(shard_key)
        elif not isinstance(shard_key, basestring):
            assert False, 'Unsupported type for shard key %s' % (
                shard_key.__class__)
        return xxhash.xxh32(to_shard).intdigest()


    def get_location_for_shard(self, realm, shard_key):
        metadata = _get_shard_metadata_for_realm(realm)
        print metadata


        pass

