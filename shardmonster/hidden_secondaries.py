import pymongo

from shardmonster import connection


_HIDDEN_SECONDARY_CONNECTION_CACHE = {}


class HiddenSecondaryError(Exception):
    pass


def configure_hidden_secondary(cluster_name, host):
    clusters = connection._get_cluster_coll()
    return clusters.update_one(
        {'name': cluster_name},
        {'$set': {'hidden_secondary_host': host}}
    )


def get_hidden_secondary_connection(cluster_name):
    """The connection client to a hidden secondary in the replica set.

    Connections are cached.
    Returns None if no hidden secondary exists."""
    expected_host = _get_expected_hidden_secondary_host(cluster_name)
    if _hidden_secondary_exists(cluster_name, expected_host):
        return _connect_to_hidden_secondary(expected_host)
    else:
        raise HiddenSecondaryError(
            'Configured hidden secondary {host} for {cluster} does not exist '
            'in replica set config'.format(
                host=expected_host,
                cluster=cluster_name
            )
        )


def _get_expected_hidden_secondary_host(cluster_name):
    clusters = connection._get_cluster_coll()
    cluster = clusters.find_one({'name': cluster_name})
    try:
        return cluster['hidden_secondary_host']
    except KeyError:
        raise HiddenSecondaryError(
            'No hidden secondary has been configured for {cluster}'.format(
                cluster=cluster_name
            )
        )


def _hidden_secondary_exists(cluster_name, expected_host):
    conn = connection.get_connection(cluster_name)
    replica_set_conf = conn.admin.command('replSetGetConfig')
    for member in replica_set_conf['config']['members']:
        if member['hidden'] and member['host'] == expected_host:
            return True
    return False


def _connect_to_hidden_secondary(host):
    if host not in _HIDDEN_SECONDARY_CONNECTION_CACHE:
        uri = _uri(host)
        _HIDDEN_SECONDARY_CONNECTION_CACHE[host] = pymongo.MongoClient(uri)
    return _HIDDEN_SECONDARY_CONNECTION_CACHE[host]


def _uri(host):
    return 'mongodb://{host}/'.format(host=host)


def close_connections_to_hidden_secondaries():
    for host, connection in list(_HIDDEN_SECONDARY_CONNECTION_CACHE.items()):
        connection.close()
        del _HIDDEN_SECONDARY_CONNECTION_CACHE[host]
