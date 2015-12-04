import logging
import pymongo
import threading
import time

logger = logging.getLogger("shardmonster")

CLUSTER_CACHE_LENGTH = 10 * 60 # Cache URI lookups for 10 minutes

_connection_cache = {}
_cluster_uri_cache = {}
_controlling_db = None


def _connect_to_mongo(uri):
    return pymongo.MongoClient(uri)


def connect_to_controller(uri, db_name):
    """Connects to the controlling database. This contains information about
    the realms, shards and clusters.

    :param str uri: The Mongo URI to connect to. This should typically detail
        several replica members to ensure connectivity.
    :param str db_name: The name of the database to connect to on the given
        replica set.
    """
    global _controlling_db
    _controlling_db = _connect_to_mongo(uri)[db_name]


def _make_connection(cluster_name):
    uri = get_cluster_uri(cluster_name)
    return _connect_to_mongo(uri)


def get_controlling_db():
    """Gets a reference to the database that is the controller for sharding.
    """
    global _controlling_db
    if not _controlling_db:
        raise Exception(
            'Call connect_to_controller before attempting to get a connection')
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
