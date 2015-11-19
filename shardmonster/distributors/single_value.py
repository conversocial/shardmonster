import bson
import numbers

from shardmonster.metadata import (
    _get_metadata_for_shard, _get_realm_by_name, LocationMetadata,
    POST_MIGRATION_PHASES)


class SingleValueDistributor(object):
    def __init__(self, realm_name):
        self.realm_name = realm_name
        # TODO This is long lived :(
        self.realm = _get_realm_by_name(realm_name)


    def get_location_for_shard(self, shard_key):
        shard = _get_metadata_for_shard(self.realm, shard_key)
        status = shard['status']

        if status in POST_MIGRATION_PHASES:
            location = LocationMetadata(shard['new_location'])
            location.contains.append(shard_key)
        else:
            location = LocationMetadata(shard['location'])
            location.contains.append(shard_key)
        return location
