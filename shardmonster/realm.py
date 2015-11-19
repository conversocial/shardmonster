import time
from shardmonster.caching import create_cache, get_caching_duration
from shardmonster.connection import get_controlling_db


_realm_cache = create_cache()


def _get_realm_coll():
    return get_controlling_db().realms


def create_indices():
    realm_coll = _get_realm_coll()
    realm_coll.ensure_index([('name', 1)], unique=True)


def create_realm(name, shard_field, default_dest, shard_type='single_value'):
    _get_realm_coll().insert({
        'name': name,
        'shard_field': shard_field,
        'shard_type': shard_type,
        'default_dest': default_dest})


def ensure_realm_exists(
        name, shard_field, default_dest, shard_type='single_value'):
    """Ensures that a realm of the given name exists and matches the expected
    settings.

    :param str name: The name of the realm
    :param shard_field: The field in documents that should be used as the shard
        field. The only supported values that can go in this field are strings
        and integers.
    :param str default_dest: The default destination for any data that isn't
        explicitly sharded to a specific location.
    :param str shard_type: The type of sharding to perform. Options are:
        single_value, hash_range.
    :return: None
    """
    coll = _get_realm_coll()

    cursor = coll.find({'name': name})
    if cursor.count():
        # realm with this name already exists
        existing = cursor[0]
        if (existing['shard_field'] != shard_field
            or existing['default_dest'] != default_dest):
            raise Exception('Cannot change realm')
        else:
            return
        
    create_realm(name, shard_field, default_dest, shard_type)


def get_realm_by_name(name):
    global _realm_cache
    now = time.time()
    realm, expiry = _realm_cache.get(name, (None, 0))
    if expiry <= now:
        realms_coll = _get_realm_coll()
        try:
            realm = realms_coll.find({'name': name})[0]
            # TODO Remove this after everything has been updated with the new
            # field
            if 'shard_type' not in realm:
                realm['shard_type'] = 'single_value'
        except IndexError:
            raise Exception(
                'Realm with name %s does not exist' % name)
        expiry = now + get_caching_duration()
        _realm_cache[name] = realm, expiry
    return _realm_cache[name][0]
