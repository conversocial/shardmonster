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

def log(s):
    print datetime.now(), s


def _get_collection_from_location_string(location, collection_name):
    server_addr, database_name = parse_location(location)
    connection = get_connection(server_addr)
    return connection[database_name][collection_name]


def _do_copy(collection_name, shard_key, manager):
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
    try:
        for record in cursor:
            new_collection.insert(record, w=0)

            # Get the insert throttle out of the manager. This allows for the
            # insert throttle to be changed by another thread whilst maintaining
            # thread safety.
            insert_throttle = manager.insert_throttle
            if insert_throttle:
                time.sleep(insert_throttle)

            manager.inc_inserted()
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
            # Verify that this object has been successfully copied from the old
            # collection before performing the update. If an object is moved in
            # the index during a migration then it *can* be missed and we pick
            # it up here instead.
            if not new_collection.find({'_id': oid}).count():
                doc = list(current_collection.find({'_id': oid}))
                if doc:
                    doc = doc[0]
                    new_collection.insert(doc, w=1)
            new_collection.update(
                {'_id': oid}, r['o'], w=1)

        elif r['op'] == 'i':
            try:
                new_collection.insert(r['o'], w=1)
            except pymongo.errors.DuplicateKeyError:
                pass
        elif r['op'] == 'd':
            new_collection.remove({'_id': oid}, w=1)

        oplog_pos = r['ts']

    return oplog_pos


def _delete_source_data(collection_name, shard_key, manager):
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
    try:
        for doc in cursor:
            current_collection.remove({'_id': doc['_id']})

            # Get the delete throttle out of the manager. This allows for the
            # insert throttle to be changed by another thread whilst maintaining
            # thread safety.
            delete_throttle = manager.delete_throttle
            if delete_throttle:
                time.sleep(delete_throttle)
            manager.inc_deleted()

    finally:
        cursor.close()


class ShardMovementThread(threading.Thread):
    def __init__(
            self, collection_name, shard_key, new_location, manager):
        self.collection_name = collection_name
        self.shard_key = shard_key
        self.new_location = new_location
        self.exception = None
        self.manager = manager
        super(ShardMovementThread, self).__init__()


    def run(self):
        try:
            # Copy phase
            self.manager.set_phase('copy')
            api.start_migration(
                self.collection_name, self.shard_key, self.new_location)

            oplog_pos = _get_oplog_pos(self.collection_name, self.shard_key)
            _do_copy(self.collection_name, self.shard_key, self.manager)

            # Sync phase
            self.manager.set_phase('sync')
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
            api.set_shard_to_migration_status(
                self.collection_name, self.shard_key,
                metadata.ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION)
            time.sleep(0.1)
            
            # Sync the oplog one final time to catch any writes that were
            # performed during the pause
            _sync_from_oplog(
                self.collection_name, self.shard_key, oplog_pos)

            # Delete phase
            self.manager.set_phase('delete')
            api.set_shard_to_migration_status(
                self.collection_name, self.shard_key,
                metadata.ShardStatus.POST_MIGRATION_DELETE)
            _delete_source_data(
                self.collection_name, self.shard_key, self.manager)

            api.set_shard_at_rest(
                self.collection_name, self.shard_key, self.new_location,
                force=True)

            self.manager.set_phase('complete')
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
        self.inserted = 0
        self.deleted = 0
        self.phase = None

    def inc_inserted(self):
        self.inserted += 1

    def inc_deleted(self):
        self.deleted += 1

    def start_migration(self):
        self._migration_thread = ShardMovementThread(
            self.collection_name, self.shard_key, self.new_location,
            manager=self)
        self._migration_thread.start()

    def is_finished(self):
        if self._migration_thread.exception:
            raise Exception(
                'Migration failed %s' % (self._migration_thread.exception,))
        return not self._migration_thread.is_alive()

    def block_until_finished(self, status_interval=60):
        """Blocks the current thread until the manager is finished.
        """
        last_status = time.time()
        while not self.is_finished():
            time.sleep(0.1)
            if time.time() - last_status > status_interval:
                self.print_status()
                last_status = time.time()

    def set_insert_throttle(self, insert_throttle):
        log('Changing insert throttle from %.4f to %.4f' % (
            self.insert_throttle, insert_throttle))
        self.insert_throttle = insert_throttle

    def set_delete_throttle(self, delete_throttle):
        log('Changing delete throttle from %.4f to %.4f' % (
            self.delete_throttle, delete_throttle))
        self.delete_throttle = delete_throttle

    def print_status(self):
        if not self.phase:
            log('Migration not started')
        elif self.phase == 'sync':
            log('Syncing oplog')
        elif self.phase == 'copy':
            log('Copying source data. %d documents copied' % self.inserted)
        elif self.phase == 'delete':
            log('Deleting source data. %d documents deleted' % self.deleted)
        elif self.phase == 'complete':
            log('Complete')

    def set_phase(self, phase):
        self.phase = phase


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

    This method returns a reference to the migration manager which can be
    queried for it's status or can be used to block for completion.
    """
    manager = _begin_migration(
        collection_name, shard_key, new_location,
        delete_throttle=delete_throttle, insert_throttle=insert_throttle)
    return manager
