How it works
============

Realms
------

A realm is a mapping describing a collection's sharding strategy. It consists of
documents of the form::

    {
        name: 'some-realm',
        shard_field: 'some-field',
        collection: 'collection-name'
    }

Shard data
----------

There is a shards collection that stores documents of the form::

    {
        realm: 'some-realm',
        shard_key: 'some-key',
        status: 'migrating-copy'
                | 'migrating-sync'
                | 'post-migration-paused-at-source'
                | 'post-migration-paused-at-destination'
                | 'post-migration-delete'
                | 'at-rest',
        location: 'cluster/database'
    }

This describes the position of the data where some-field is equal to some-key.
In the event that the data is in a migrating phase then there will be
additional fields::

        new_location: 'cluster2/database',

Migration phases
----------------

**Copy**

The copy phase is copying all data that matches the shard query from the current
location to the new location. Once this phase is finished, the sync phase
begins.

During this phase all reads and writes will go to the original location of the
data.

**Sync**

The sync phase is where the oplog is replayed since the start of the migration.

Once the oplog is replayed and we are close to realtime the post migration
pause begins.

During this phase all reads and writes will go to the original location of the
data.

**Post migration pause at source**

Once data has been migrated the shard will enter a pause state. This allows for
someone (or something) to verify that the data has been migrated successfully
and everything is OK. This state shouldn't really be needed, but, it's a
nice safety valve during testing.

During this phase all reads and writes will go to the original location of the
data. The oplog continues to be synced during this period.

**Post migration pause at destination** 

Once any data validation has been performed the shard will be moved to
"paused at destination". During this phase all reads will go to the
new location of the data. Writes will be suspended. The oplog continues to be
synced during this period to catch any stragglers.

Due to the suspension of writes during this period it is expected that this
phase will be very short.

**Post migration delete**

Once a shard has been migrated the original location should have the data
removed. Doing this deletion helps with reducing the amount of query
customisation that has to happen to cope with data being in two places at once.

Crucially, the oplog is NOT synced during this phase. That would copy the
deletes and lead to all kinds of sadness. Specifically, the removal of most of
the data.

Only once this phase is completed is the shard moved to at rest and the
location field is updated.

**At rest**

Data that is at rest is no longer being migrated in any form. Lookups for
shard information will be cached for a short length of time.
