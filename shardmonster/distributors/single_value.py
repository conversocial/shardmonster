from shardmonster.metadata import (
    _get_metadata_for_shard, LocationMetadata, POST_MIGRATION_PHASES)
from shardmonster.realm import get_realm_by_name


class SingleValueDistributor(object):
    def __init__(self, realm_name):
        self.realm_name = realm_name


    def get_location_for_shard(self, shard_key):
        realm = get_realm_by_name(self.realm_name)
        shard = _get_metadata_for_shard(realm, shard_key)
        status = shard['status']

        if status in POST_MIGRATION_PHASES:
            location = LocationMetadata(shard['new_location'])
            location.contains.append(shard_key)
        else:
            location = LocationMetadata(shard['location'])
            location.contains.append(shard_key)
        return location

    def create_query_iterator(self, query):
        """Creates an iterator that returns locations for querying and the
        query that should be performed against them. This query may be different
        from the original query as it may need to exclude certain results during
        migrations.
        """
        from shardmonster.metadata import _get_all_locations_for_realm
        from shardmonster.operations import _get_query_target
        realm = get_realm_by_name(self.realm_name)
        shard_field = realm['shard_field']
        shard_key = _get_query_target(self.realm_name, query)
        if shard_key:
            location = self.get_location_for_shard(shard_key)
            locations = {location.location: location}
        else:
            locations = _get_all_locations_for_realm(realm)

        for location, location_meta in locations.iteritems():
            query_to_execute = query

            if location_meta.excludes:
                if len(location_meta.excludes) == 1:
                    exclusion = {shard_field: {'$ne': location_meta.excludes[0]}}
                    query_to_execute = {'$and': [query, exclusion]}
                else:
                    raise Exception('Multiple shards in transit. Aborting')
            yield location, query_to_execute
