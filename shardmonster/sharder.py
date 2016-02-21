"""
The actual sharding process for moving data around between servers.

Running
-------

python shardmonster --server=localhost:27017 --db=shardmonster

The server/database is the location of the metadata.
"""
import pymongo
import sys
import threading
import time
from datetime import datetime

from shardmonster import api, metadata
from shardmonster.connection import (
    get_connection, get_controlling_db, parse_location)

STATUS_COPYING = 'copying'
STATUS_SYNCING = 'syncing'
STATUS_SYNCED = 'synced'
ALL_STATUSES = [STATUS_COPYING, STATUS_SYNCING, STATUS_SYNCED]
STATUS_MAP = {STATUS: STATUS for STATUS in ALL_STATUSES}

HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

def blue(s, *args):
    print OKBLUE + (s % args) + ENDC


def _detail_log(s):
    print '    ', datetime.now(), s


def _get_collection_from_location_string(location, collection_name):
    server_addr, database_name = parse_location(location)
    connection = get_connection(server_addr)
    return connection[database_name][collection_name]


def _do_copy(collection_name, shard_key, insert_throttle=None):
    realm = metadata._get_realm_for_collection(collection_name)
    shard_field = realm['shard_field']

    shards_coll = api._get_shards_coll()
    shard_metadata, = shards_coll.find(
        {'realm': realm['name'], 'shard_key': shard_key})
    if shard_metadata['status'] != metadata.ShardStatus.MIGRATING_COPY:
        raise Exception('Shard not in copy state (phase 1)')

    current_location = shard_metadata['location']
    new_location = shard_metadata['new_location']

    current_collection = _get_collection_from_location_string(
        current_location, collection_name)

    new_collection = _get_collection_from_location_string(
        new_location, collection_name)

    query = {shard_field: shard_key}
    cursor = current_collection.find(query, no_cursor_timeout=True)
    inserted = 0
    try:
        for record in cursor:
            new_collection.insert(record, w=0)
            if inserted % 50000 == 0:
                _detail_log('%d records inserted' % inserted)
            if insert_throttle:
                time.sleep(insert_throttle)
            inserted += 1
    finally:
        cursor.close()

    result = new_collection.database.command('getLastError')
    if result['err']:
        raise Exception('Failed to do copy! Mongo error: %s' % result['err'])


def _get_metadata_for_shard(realm_name, shard_key):
    shards_coll = api._get_shards_coll()
    shard_metadata, = shards_coll.find(
        {'realm': realm_name, 'shard_key': shard_key})
    return shard_metadata


def _get_oplog_pos(collection_name, shard_key):
    """Gets the oplog position for the given collection/shard key combination.
    This is necessary as the oplog will be very different on different clusters.
    """
    realm = metadata._get_realm_for_collection(collection_name)
    shard_metadata = _get_metadata_for_shard(realm['name'], shard_key)

    current_location = shard_metadata['location']
    current_collection = _get_collection_from_location_string(
        current_location, collection_name)
    current_conn = current_collection.database.client

    repl_coll = current_conn['local']['oplog.rs']
    most_recent_op = repl_coll.find({}, sort=[('$natural', -1)])[0]
    ts_from = most_recent_op['ts']
    return ts_from


def _sync_from_oplog(collection_name, shard_key, oplog_pos):
    """Syncs the oplog to within a reasonable timeframe of "now".
    """
    realm = metadata._get_realm_for_collection(collection_name)
    shard_field = realm['shard_field']

    shard_metadata = _get_metadata_for_shard(realm['name'], shard_key)

    current_location = shard_metadata['location']
    new_location = shard_metadata['new_location']

    current_collection = _get_collection_from_location_string(
        current_location, collection_name)

    new_collection = _get_collection_from_location_string(
        new_location, collection_name)

    # Get the connection used by the source collection and use that for the
    # oplog tailing
    conn = current_collection.database.client
    repl_coll = conn['local']['oplog.rs']
    cursor = repl_coll.find(
        {'ts': {'$gte': oplog_pos}},
        cursor_type=pymongo.CursorType.TAILABLE,
        oplog_replay=True)
    cursor = cursor.hint([('$natural', 1)])

    shard_query = {shard_field: shard_key}

    current_namespace = "%s.%s" % (
        current_collection.database.name, current_collection.name)

    for r in cursor:
        if r['ns'] != current_namespace:
            continue

        if r['op'] in ['u', 'i']:
            # Check that this doc is part of our query set
            oid = r.get('o2', r['o'])['_id']
            object_query = {'_id': oid}
            object_query.update(shard_query)
            match = bool(
                current_collection.find(object_query).count())
        elif r['op'] == 'd':
            oid = r.get('o2', r['o'])['_id']
            object_query = {'_id': oid}
            object_query.update(shard_query)
            match = bool(
                new_collection.find(object_query).count())
        else:
            # Notification ops can be ignored.
            continue

        if not match:
            continue

        if r['op'] == 'u':
            blue(' - Updating %s with %s' % (oid, r['o']))
            new_collection.update(
                {'_id': oid}, r['o'], w=1)

        elif r['op'] == 'i':
            try:
                new_collection.insert(r['o'], w=1)
            except pymongo.errors.DuplicateKeyError:
                pass
        elif r['op'] == 'd':
            blue(' - Removing %s' % oid)
            new_collection.remove({'_id': oid}, w=1)

        oplog_pos = r['ts']

    return oplog_pos


def _delete_source_data(collection_name, shard_key, delete_throttle=None):
    realm = metadata._get_realm_for_collection(collection_name)
    shard_field = realm['shard_field']

    shards_coll = api._get_shards_coll()
    shard_metadata, = shards_coll.find(
        {'realm': realm['name'], 'shard_key': shard_key})
    if shard_metadata['status'] != metadata.ShardStatus.POST_MIGRATION_DELETE:
        raise Exception('Shard not in delete state')

    current_location = shard_metadata['location']
    current_collection = _get_collection_from_location_string(
        current_location, collection_name)

    cursor = current_collection.find(
            {shard_field: shard_key}, {'_id': 1},
            no_cursor_timeout=True)
    deleted = 0
    try:
        for doc in cursor:
            current_collection.remove({'_id': doc['_id']})
            if delete_throttle:
                time.sleep(delete_throttle)
            if deleted % 10000 == 0:
                _detail_log('%d records deleted' % deleted)
            deleted += 1

    finally:
        cursor.close()


class ShardMovementThread(threading.Thread):
    def __init__(
            self, collection_name, shard_key, new_location,
            delete_throttle=None, insert_throttle=None):
        self.collection_name = collection_name
        self.shard_key = shard_key
        self.new_location = new_location
        self.exception = None
        self.delete_throttle = delete_throttle
        self.insert_throttle = insert_throttle
        super(ShardMovementThread, self).__init__()


    def run(self):
        try:
            blue('* Starting migration')
            api.start_migration(
                self.collection_name, self.shard_key, self.new_location)

            # Copy phase
            blue('* Doing copy')
            oplog_pos = _get_oplog_pos(self.collection_name, self.shard_key)
            _do_copy(self.collection_name, self.shard_key, self.insert_throttle)

            # Sync phase
            blue('* Initial oplog sync')
            start_sync_time = time.time()
            api.set_shard_to_migration_status(
                self.collection_name, self.shard_key, metadata.ShardStatus.MIGRATING_SYNC)
            oplog_pos = _sync_from_oplog(
                self.collection_name, self.shard_key, oplog_pos)

            # Ensure that the sync has taken at least as long as our caching time
            # to ensure that all writes will get paused at approximately the same
            # time.
            while time.time() < start_sync_time + api.get_caching_duration():
                time.sleep(0.05)
                oplog_pos = _sync_from_oplog(
                    self.collection_name, self.shard_key, oplog_pos)

            # Now all the caching of metadata should be stopped for this shard.
            # We can flip to being paused at destination and wait ~100ms for any
            # pending updates/inserts to be performed. If these are taking longer
            # than 100ms then you are in a bad place and should rethink sharding.
            blue('* Pausing at destination')
            api.set_shard_to_migration_status(
                self.collection_name, self.shard_key,
                metadata.ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION)
            time.sleep(0.1)
            
            blue('* Syncing oplog once more')
            _sync_from_oplog(
                self.collection_name, self.shard_key, oplog_pos)

            # Delete phase
            blue('* Doing deletion')
            api.set_shard_to_migration_status(
                self.collection_name, self.shard_key,
                metadata.ShardStatus.POST_MIGRATION_DELETE)
            _delete_source_data(
                self.collection_name, self.shard_key,
                delete_throttle=self.delete_throttle)

            api.set_shard_at_rest(
                self.collection_name, self.shard_key, self.new_location,
                force=True)

            blue('* Done')
        except:
            self.exception = sys.exc_info()
            raise


class ShardMovementManager(object):
    def __init__(
            self, collection_name, shard_key, new_location,
            delete_throttle=None, insert_throttle=None):
        self.collection_name = collection_name
        self.shard_key = shard_key
        self.new_location = new_location
        self.delete_throttle = delete_throttle
        self.insert_throttle = insert_throttle


    def start_migration(self):
        self._migration_thread = ShardMovementThread(
            self.collection_name, self.shard_key, self.new_location,
            delete_throttle=self.delete_throttle,
            insert_throttle=self.insert_throttle)
        self._migration_thread.start()


    def is_finished(self):
        if self._migration_thread.exception:
            raise Exception(
                'Migration failed %s' % (self._migration_thread.exception,))
        return not self._migration_thread.is_alive()


def _begin_migration(
        collection_name, shard_key, new_location, delete_throttle=None,
        insert_throttle=None):
    if metadata.are_migrations_happening():
        raise Exception(
            'Cannot start migration when another migration is in progress')
    manager = ShardMovementManager(
        collection_name, shard_key, new_location,
        delete_throttle=delete_throttle, insert_throttle=insert_throttle)
    manager.start_migration()
    return manager


def do_migration(
        collection_name, shard_key, new_location, delete_throttle=None,
        insert_throttle=None):
    """Migrates the data with the given shard key in the given collection to
    the new location. E.g.

        >>> do_migration('some_collection', 1, 'cluster-1/some_db')

    Would migrate everything from some_collection where the shard field is set
    to 1 to the database some_db on cluster-1.

    :param str collection_name: The name of the collection to migrate
    :param shard_key: The key of the shard that is to be moved
    :param str new_location: Location that the shard should be moved to in the
        format "cluster/database".
    :param float delete_throttle: This is the length of pause that will be
        applied after each deletion of a source document.
    :param float insert_throttle: This is the length of pause that will be
        applied after each insert of a new document to the destination.

    This method blocks until the migration is completed.
    """
    manager = _begin_migration(
        collection_name, shard_key, new_location,
        delete_throttle=delete_throttle, insert_throttle=insert_throttle)
    while not manager.is_finished():
        time.sleep(0.01)
