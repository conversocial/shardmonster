"""
The actual sharding process for moving data around between servers.

Running
-------

python shardmonster --server=localhost:27017 --db=shardmonster

The server/database is the location of the metadata.
"""
from __future__ import absolute_import

import pymongo
import sys
import threading
import time
import traceback
from datetime import datetime
from itertools import chain
from pprint import pformat

import six
from pymongo.errors import BulkWriteError, OperationFailure

from shardmonster import api, metadata
from shardmonster.connection import (
    close_thread_connections, get_connection, parse_location)

STATUS_COPYING = 'copying'
STATUS_SYNCING = 'syncing'
STATUS_SYNCED = 'synced'
ALL_STATUSES = [STATUS_COPYING, STATUS_SYNCING, STATUS_SYNCED]
STATUS_MAP = {STATUS: STATUS for STATUS in ALL_STATUSES}


def log(s):
    print(datetime.now(), s)


def pretty_log(s):
    print(datetime.now(), pformat(s))


def _get_collection_from_location_string(location, collection_name):
    server_addr, database_name = parse_location(location)
    connection = get_connection(server_addr)
    return connection[database_name][collection_name]


def batched_cursor_iterator(cursor, batch_size_fn):
    batch = []
    for record in cursor:
        batch.append(record)
        if len(batch) >= batch_size_fn():
            yield batch
            batch = []

    if len(batch):
        yield batch


def _do_copy(collection_name, shard_key, manager):
    realm = metadata._get_realm_for_collection(collection_name)

    shard_metadata, = api._get_shards_coll().find(
        {'realm': realm['name'], 'shard_key': shard_key})
    if shard_metadata['status'] != metadata.ShardStatus.MIGRATING_COPY:
        raise Exception('Shard not in copy state (phase 1)')

    current_collection = _get_collection_from_location_string(
        shard_metadata['location'], collection_name)
    new_collection = _get_collection_from_location_string(
        shard_metadata['new_location'], collection_name)
    target_key = sniff_mongos_shard_key(new_collection) or ['_id']

    cursor = current_collection.find({realm['shard_field']: shard_key},
                                     no_cursor_timeout=True)
    try:
        # manager.insert_throttle and manager.insert_batch_size can change
        # in other thread so we reference them on each cycle
        for batch in batched_cursor_iterator(cursor,
                                             lambda: manager.insert_batch_size):
            try:
                result = new_collection.bulk_write(
                    batch_of_upsert_ops(batch, target_key),
                    ordered=True)
            except BulkWriteError as e:
                pretty_log(e.details)
                raise
            tum_ti_tum(manager.insert_throttle)
            manager.inc_inserted(by=result.bulk_api_result['nUpserted'])
    finally:
        cursor.close()


def sniff_mongos_shard_key(collection):
    connection = collection.database.client
    try:
        info = connection.config.collections.find_one({
            "_id": '%s.%s' % (collection.database.name, collection.name),
            "dropped": False})
        if not info:
            return None
        return list(info['key'].keys())
    except OperationFailure:
        return None


def batch_of_upsert_ops(batch, target_key):
    # Duplicates will be the results of seeing updates during the
    # read. These will be corrected by the oplog pass later.
    return [
        pymongo.operations.UpdateOne(pick(target_key, record),
                                     {'$set': without_id(record)},
                                     upsert=True)
        for record in batch]


def pick(target_key, record):
    return {key: value for key, value in six.iteritems(record)
            if key in target_key}


def without_id(record):
    new_record = record.copy()
    new_record.pop('_id', None)
    return new_record


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
    """Syncs the oplog to within a reasonable timeframe of "now"."""
    realm = metadata._get_realm_for_collection(collection_name)
    shard_metadata = _get_metadata_for_shard(realm['name'], shard_key)

    source = _get_collection_from_location_string(
        shard_metadata['location'], collection_name)
    target = _get_collection_from_location_string(
        shard_metadata['new_location'], collection_name)

    cursor = tail_oplog(source.database.client, oplog_pos)
    try:
        for entry in cursor:
            replay_oplog_entry(entry, {realm['shard_field']: shard_key},
                               source, target)
            oplog_pos = entry['ts']
    finally:
        cursor.close()
    return oplog_pos


def tail_oplog(connection, oplog_pos):
    cursor = connection['local']['oplog.rs'].find(
        {'ts': {'$gte': oplog_pos}},
        cursor_type=pymongo.CursorType.TAILABLE,
        oplog_replay=True)
    cursor = cursor.hint([('$natural', 1)])
    return cursor


def replay_oplog_entry(entry, shard_selector, source, target):
    if entry['ns'] != source.database.name + '.' + source.name:
        return
    if entry['op'] == 'i':
        query = merge(shard_selector, {'_id': entry['o']['_id']})
        if fetch_one(source.find(query)):
            try:
                target.insert(entry['o'], w=1)
            except pymongo.errors.DuplicateKeyError:
                pass
    elif entry['op'] == 'u':
        query = merge(shard_selector, {'_id': entry['o2']['_id']})
        source_document = fetch_one(source.find(query))
        if source_document:
            if not source_document == entry['o']:
                target.update({'_id': source_document['_id']},
                              source_document, w=0, upsert=True)
    elif entry['op'] == 'd':
        query = merge(shard_selector, {'_id': entry['o']['_id']})
        if fetch_one(target.find(query)):
            target.remove({'_id': entry['o']['_id']}, w=1)


def merge(*dicts):
    return dict(chain(*(a_dict.items() for a_dict in dicts)))


def fetch_one(cursor):
    try:
        return cursor.next()
    except StopIteration:
        return None


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

    cursor = current_collection.find({shard_field: shard_key}, {'_id': 1},
                                     no_cursor_timeout=True)
    try:
        # manager.insert_throttle and manager.insert_batch_size can change
        # in other thread so we reference them on each cycle
        for batch in batched_cursor_iterator(cursor,
                                             lambda: manager.delete_batch_size):
            _ids = [record['_id'] for record in batch]
            result = current_collection.delete_many({'_id': {'$in': _ids}})
            tum_ti_tum(manager.delete_throttle)
            manager.inc_deleted(by=result.raw_result['n'])
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
                self.collection_name,
                self.shard_key, metadata.ShardStatus.MIGRATING_SYNC)
            oplog_pos = _sync_from_oplog(
                self.collection_name, self.shard_key, oplog_pos)

            # Ensure that the sync has taken at least as long as our caching
            # time to ensure that all writes will get paused at approximately
            # the same time.
            while time.time() < start_sync_time + api.get_caching_duration():
                time.sleep(0.05)
                oplog_pos = _sync_from_oplog(
                    self.collection_name, self.shard_key, oplog_pos)

            # Now all the caching of metadata should be stopped for this shard.
            # We can flip to being paused at destination and wait ~100ms for any
            # pending updates/inserts to be performed. If these are taking
            # longer than 100ms then you are in a bad place and should rethink
            # sharding.
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
        finally:
            close_thread_connections(threading.current_thread())


class ShardMovementManager(object):
    def __init__(
            self, collection_name, shard_key, new_location,
            delete_throttle, insert_throttle,
            delete_batch_size, insert_batch_size):
        self.collection_name = collection_name
        self.shard_key = shard_key
        self.new_location = new_location
        self.delete_throttle = delete_throttle
        self.insert_throttle = insert_throttle
        self.delete_batch_size = delete_batch_size
        self.insert_batch_size = insert_batch_size
        self.inserted = 0
        self.deleted = 0
        self.phase = None

    def inc_inserted(self, by=1):
        self.inserted += by

    def inc_deleted(self, by=1):
        self.deleted += by

    def start_migration(self):
        self._migration_thread = ShardMovementThread(
            self.collection_name, self.shard_key, self.new_location,
            manager=self)
        self._migration_thread.start()

    def is_finished(self):
        if self._migration_thread.exception:
            exc = traceback.format_exception(*self._migration_thread.exception)
            raise Exception(
                'Migration failed, exception in thread:\n'
                '\033[91m> %s\033[0m' % "> ".join(exc))
        return not self._migration_thread.is_alive()

    def block_until_finished(self, status_interval=60):
        """Blocks the current thread until the manager is finished."""
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
        collection_name, shard_key, new_location,
        delete_throttle=None, insert_throttle=None,
        delete_batch_size=1000, insert_batch_size=1000):
    if metadata.are_migrations_happening():
        raise Exception(
            'Cannot start migration when another migration is in progress')
    manager = ShardMovementManager(
        collection_name, shard_key, new_location,
        delete_throttle=delete_throttle, insert_throttle=insert_throttle,
        delete_batch_size=delete_batch_size, insert_batch_size=insert_batch_size)  # noqa
    manager.start_migration()
    return manager


def do_migration(
        collection_name, shard_key, new_location,
        delete_throttle=None, insert_throttle=None,
        delete_batch_size=1000, insert_batch_size=1000):
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
    :param int delete_batch_size: The number of documents which will be deleted
        at once using the bulk_write feature of pymongo
    :param int insert_batch_size: The number of documents which will be inserted
        at once using the bulk_write feature of pymongo

    This method returns a reference to the migration manager which can be
    queried for it's status or can be used to block for completion.
    """
    manager = _begin_migration(
        collection_name, shard_key, new_location,
        delete_throttle=delete_throttle, insert_throttle=insert_throttle,
        delete_batch_size=delete_batch_size, insert_batch_size=insert_batch_size)  # noqa
    return manager


def tum_ti_tum(wait_time):
    if wait_time:
        time.sleep(wait_time)
