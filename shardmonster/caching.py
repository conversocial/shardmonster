from shardmonster.shardstatus import ShardStatus

# The phases during which caching should not be happening
SHORT_CACHE_PHASES = {
    ShardStatus.MIGRATING_SYNC,
    ShardStatus.POST_MIGRATION_PAUSED_AT_SOURCE,
    ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION,
}


_caching_timeout = 0
_caches = []


def get_caching_duration():
    return _caching_timeout


def activate_caching(timeout):
    """Activates caching of metadata.

    :param int timeout: Number of seconds to cache metadata for.

    Caching is generally a good thing. However, during a migration there will be
    a pause equal to whatever the caching timeout. This is to avoid stale reads
    and writes when the source of truth for a shard changes location.
    """
    global _caching_timeout
    _caching_timeout = timeout

    # Changing the cache timeout will could cause bad things if the caches
    # are left intact.
    clear_caches()


def create_cache():
    global _caches
    cache = {}
    _caches.append(cache)
    return cache


def clear_caches():
    global _caches
    for cache in _caches:
        cache.clear()
