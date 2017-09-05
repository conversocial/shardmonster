import time

from shardmonster.connection import (
    _cluster_uri_cache, _get_cluster_coll, get_controlling_db)

class ShardStatus(object):
    def __init__(self):
        raise Exception('Enum. Do not instantiate')

    AT_REST = "at-rest"
    MIGRATING_COPY = "migrating-copy"
    MIGRATING_SYNC = "migrating-sync"
    POST_MIGRATION_PAUSED_AT_SOURCE = "post-migration-paused-at-source"
    POST_MIGRATION_PAUSED_AT_DESTINATION = "post-migration-paused-destination"
    POST_MIGRATION_DELETE = "post-migration-delete"


# The phases during which caching should not be happening
SHORT_CACHE_PHASES = {
    ShardStatus.MIGRATING_SYNC,
    ShardStatus.POST_MIGRATION_PAUSED_AT_SOURCE,
    ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION,
}


MIGRATION_PHASES = {
    ShardStatus.MIGRATING_COPY,
    ShardStatus.MIGRATING_SYNC,
    ShardStatus.POST_MIGRATION_PAUSED_AT_SOURCE,
}
POST_MIGRATION_PHASES = {
    ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION,
    ShardStatus.POST_MIGRATION_DELETE,
}

_caching_timeout = 0
_metadata_stores = {}
_realm_cache = {}


def _get_realm_coll():
    return get_controlling_db().realms


def _get_shards_coll():
    return get_controlling_db().shards


class ShardMetadataStore(object):
    """A store of all the shard metadata for a particular realm.
    This is NOT thread safe.

    We want to cache as many lookups as possible
    We have generic shard information
     - This should be cached for as long as possible
     - If a single shard is being moved then this part needs to be refreshed
    We also have specific queries for specific shards
     - These should be cached unless they are actively migrating
    """
    def __init__(self, realm_getter_fn):
        self._cache = {}
        self.realm = realm_getter_fn
        self._global_timeout = 0
        self._in_flux = None


    def metadata_changed(self):
        """Call this when metadata is changed. This will flush the cache.
        """
        self._cache = {}
        self._global_timeout = 0


    def get_single_shard_metadata(self, shard_key):
        if not self._cache_entry_is_valid(shard_key):
            self._refresh_single_shard_metadata(shard_key)
        return self._cache[shard_key][0]


    def _cache_entry_is_valid(self, shard_key):
        now = time.time()
        return (
            self._in_flux != shard_key
            and shard_key in self._cache
            and self._cache[shard_key][1] > now)


    def get_all_shard_metadata(self):
        now = time.time()
        if self._global_timeout < now:
            self._refresh_all_shard_metadata()
        elif self._in_flux:
            self._refresh_single_shard_metadata(self._in_flux)
            
        return {
            shard_key: metadata for
            shard_key, (metadata, _) in self._cache.iteritems()
        }

    
    def _refresh_single_shard_metadata(self, shard_key):
        global _caching_timeout
        shards = list(self._query_shards_collection(shard_key))

        generic_expiry = time.time() + _caching_timeout
        if shards:
            shard, = shards
            if shard['status'] in SHORT_CACHE_PHASES:
                self._in_flux = shard['shard_key']
                expiry = 0
            else:
                expiry = generic_expiry

            self._cache[shard['shard_key']] = (shard, expiry)
        else:
            shard = {
                'location': self.realm()['default_dest'],
                'status': ShardStatus.AT_REST,
                'realm': self.realm()['name'],
            }
            self._cache[shard_key] = (shard, generic_expiry)

    
    def _refresh_all_shard_metadata(self):
        global _caching_timeout
        cursor = self._query_shards_collection()
        self._global_timeout = time.time() + _caching_timeout
        self._in_flux = None

        for shard in cursor:
            if shard['status'] in SHORT_CACHE_PHASES:
                assert not self._in_flux, "Multiple shards in motion"
                self._in_flux = shard['shard_key']
                expiry = 0
            else:
                expiry = self._global_timeout

            self._cache[shard['shard_key']] = (shard, expiry)


    def _query_shards_collection(self, shard_key=None):
        shards_coll = _get_shards_coll()
        query = {'realm': self.realm()['name']}
        if shard_key:
            query['shard_key'] = shard_key
        return shards_coll.find(query)


class LocationMetadata(object):
    def __init__(self, location):
        assert location.count('/') == 1, \
            "Location must be of the form cluster/db and not %s" % location
        self.location = location
        self.contains = []
        self.excludes = []


    def __repr__(self):
        contains = ",".join(str(c) for c in self.contains[:5])
        if len(self.contains) >= 5:
            contains += "..."
        return "LocationMetadata(%s, contains: [%s], excludes: %s)" % (
            self.location, contains, self.excludes)


def realm_changed(realm_getter_fn):
    _get_metadata_store(realm_getter_fn).metadata_changed()


def _get_location_for_shard(realm, shard_key):
    """Gets the locations for the given shard. The result will be a single
    LocationMetadata object.
    """
    shard = _get_metadata_for_shard(realm, shard_key)

    status = shard['status']
    if status in POST_MIGRATION_PHASES:
        location = LocationMetadata(shard['new_location'])
        location.contains.append(shard_key)
    else:
        location = LocationMetadata(shard['location'])
        location.contains.append(shard_key)
    return location


def _get_metadata_store(realm_getter_fn):
    global _metadata_stores
    realm_name = realm_getter_fn()['name']
    if realm_name not in _metadata_stores:
        _metadata_stores[realm_name] = ShardMetadataStore(realm_getter_fn)
    return _metadata_stores[realm_name]


def _get_shard_metadata_for_realm(realm):
    return _get_metadata_store(realm).get_all_shard_metadata()


def _get_metadata_for_shard(realm, shard_key):
    return _get_metadata_store(realm).get_single_shard_metadata(shard_key)


def _get_all_locations_for_realm(realm):
    """Gets all the locations for the given realm. The results will be of the
    form:
    
        { location: LocationMetadata(...) }

    The excludes is a list of keys that need to be excluded from any query
    performed against that location.
    """
    shards = _get_shard_metadata_for_realm(realm)
    locations = {}
    for shard in shards.itervalues():
        location = shard['location']
        if location not in locations:
            locations[location] = LocationMetadata(location)
        if 'new_location' in shard:
            new_location = shard['new_location']
            if new_location not in locations:
                locations[new_location] = LocationMetadata(new_location)

        status = shard['status']
        if status in MIGRATION_PHASES:
            locations[shard['new_location']].excludes.append(shard['shard_key'])
            locations[shard['location']].contains.append(shard['shard_key'])
        elif status in POST_MIGRATION_PHASES:
            locations[shard['location']].excludes.append(shard['shard_key'])
            locations[shard['new_location']].contains.append(shard['shard_key'])
        else:
            if 'shard_key' in shard:
                locations[shard['location']].contains.append(shard['shard_key'])

    if realm['default_dest'] not in locations:
        locations[realm['default_dest']] = LocationMetadata(
            realm['default_dest'])


    return dict(locations)


def _get_realm_for_collection(collection_name):
    global _realm_cache, _caching_timeout
    now = time.time()
    realm, expiry = _realm_cache.get(collection_name, (None, 0))
    if expiry <= now:
        realms_coll = _get_realm_coll()
        try:
            realm = realms_coll.find({'collection': collection_name})[0]
        except IndexError:
            raise Exception(
                'Realm for collection %s does not exist' % collection_name)
        expiry = now + _caching_timeout
        _realm_cache[collection_name] = realm, expiry
    return _realm_cache[collection_name][0]


def _get_realm_by_name(realm_name):
    realms_coll = _get_realm_coll()
    try:
        return realms_coll.find({'name': realm_name})[0]
    except IndexError:
        raise Exception(
            'Realm named %s does not exist' % realm_name)


def get_caching_duration():
    return _caching_timeout


def activate_caching(timeout):
    """Activates caching of metadata.

    :param int timeout: Number of seconds to cache metadata for.

    Caching is generally a good thing. However, during a migration there will be
    a pause equal to whatever the caching timeout. This is to avoid stale reads
    and writes when the source of truth for a shard changes location.
    """
    global _caching_timeout, _metadata_stores, _realm_cache
    _caching_timeout = timeout

    # Blank out the metadata stores as changing the timeout will really mess
    # up everything in them
    _metadata_stores = {}
    _realm_cache = {}


def wipe_metadata():
    """Wipes all metadata. Should only be used during testing. There is no undo.

    Wipes caches as well.
    """
    _get_realm_coll().remove()
    _get_shards_coll().remove()
    _get_cluster_coll().remove()

    _cluster_uri_cache.clear()
    _realm_cache.clear()
    _metadata_stores.clear()


def are_migrations_happening():
    """Returns True if any migrations are happening. Otherwise, False.
    """
    coll = _get_shards_coll()
    states = [
        ShardStatus.MIGRATING_COPY,
        ShardStatus.MIGRATING_SYNC,
        ShardStatus.POST_MIGRATION_PAUSED_AT_SOURCE,
        ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION,
        ShardStatus.POST_MIGRATION_DELETE,
    ]
    return coll.find({'status': {'$in': states}}).count() > 0
