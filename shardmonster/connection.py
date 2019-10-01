from __future__ import absolute_import

import logging
import pymongo
import threading
import time

import six

logger = logging.getLogger("shardmonster")
CLUSTER_CACHE_LENGTH = 10 * 60  # Cache URI lookups for 10 minutes

_connection_cache = {}
_cluster_uri_cache = {}
_controlling_db = None
_controlling_db_config = None
_post_connect_callbacks = []


def _connect_to_mongo(uri):
    return pymongo.MongoClient(uri)


def register_post_connect(fn):
    _post_connect_callbacks.append(fn)
    return fn


def connect_to_controller(uri, db_name):
    """Connects to the controlling database. This contains information about
    the realms, shards and clusters.

    :param str uri: The Mongo URI to connect to. This should typically detail
        several replica members to ensure connectivity.
    :param str db_name: The name of the database to connect to on the given
        replica set.
    """
    global _controlling_db, _controlling_db_config
    _controlling_db = _connect_to_mongo(uri)[db_name]
    _controlling_db_config = ((uri, db_name), {})
    for fn in _post_connect_callbacks:
        fn()


def configure_controller(*args, **kwargs):
    """Configures the controlling database without making a connection.

    All args and kwargs will be relayed to connect_to_controller on demand.
    """
    global _controlling_db_config
    _controlling_db_config = (args, kwargs)


def _make_connection(cluster_name):
    uri = get_cluster_uri(cluster_name)
    return _connect_to_mongo(uri)


def get_controlling_db():
    """Gets a reference to the database that is the controller for sharding.
    """
    global _controlling_db
    if not _controlling_db:
        if not _controlling_db_config:
            raise Exception(
                'Call connect_to_controller or configure_controller '
                'before attempting to get a connection')
        (args, kwargs) = _controlling_db_config
        connect_to_controller(*args, **kwargs)
    return _controlling_db


def get_connection(cluster_name):
    global _connection_cache
    key = '%s:%s' % (threading.current_thread(), cluster_name)
    if key not in _connection_cache:
        connection = _make_connection(cluster_name)
        _connection_cache[key] = connection
        return connection

    connection = _connection_cache[key]
    return connection


def close_thread_connections(thread):
    """Closes all connections for the given thread.
    """
    global _connection_cache
    to_remove = set()
    for key in six.iterkeys(_connection_cache):
        if key.startswith('%s:' % thread):
            to_remove.add(key)
    for key in to_remove:
        _connection_cache[key].close()
        del _connection_cache[key]


def _get_cluster_coll():
    db = get_controlling_db()
    return db.clusters


def add_cluster(name, uri):
    """Adds a cluster with a specific name to the clusters shardmonster is aware
    of.
    """
    coll = _get_cluster_coll()
    coll.insert({'name': name, 'uri': uri})


def ensure_cluster_exists(name, uri):
    """Ensures that a cluster with the given name exists. If it doesn't exist,
    a new cluster definition will be created using name and uri. If it does
    exist then no changes will be made.

    :param str name: The name of the cluster
    :param str uri: The URI to use for the cluster
    """
    coll = _get_cluster_coll()
    cursor = coll.find({'name': name})
    if not cursor.count():
        add_cluster(name, uri)
    else:
        existing = cursor[0]
        if existing['uri'] != uri:
            logger.warn(
                "Cluster in database does not match cluster being configured. "
                "This is normally OK if clusters are being moved about."
            )


def get_cluster_uri(name):
    """Gets the URI of the cluster with the given name.

    Caches all lookups for ~10 minutes.
    """
    global _cluster_uri_cache
    now = time.time()
    if name not in _cluster_uri_cache or _cluster_uri_cache[name][1] <= now:
        coll = _get_cluster_coll()
        cluster = coll.find_one({'name': name})
        if not cluster:
            raise Exception('Cluster %s has not been configured' % name)
        uri = cluster['uri']
        expiry = now + CLUSTER_CACHE_LENGTH
        _cluster_uri_cache[name] = (uri, expiry)

    return _cluster_uri_cache[name][0]


def parse_location(location):
    """Parses a location of the form cluster/database into the two parts and
    returns them as a tuple

       >>> parse_location("cluster1/some_db")
       ("cluster1", "some_db")
    """
    if location.count('/') != 1:
        raise Exception('Invalid location %s' % location)
    cluster, db = location.split('/')
    return cluster, db
