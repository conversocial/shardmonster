Getting Started
===============

Connecting the Controller
-------------------------

Shardmonster relies on a connection to a controller. This is where all the
shard metadata is stored. It is recommended that metadata is stored on a
dedicated cluster. This helps ensure that reads/writes to metadata are very
fast.

.. code-block:: python

    import shardmonster
    shardmonster.connect_to_controller(
        "mongodb://localhost:27017/?replicaset=cluster-1",
        "sharding_db"

Activate Caching
----------------

Shardmonster makes use of caching of metadata to ensure that queries are as fast
as possible. The cache length is set in seconds. All migrations are paused for
at least as long as the cache length to ensure caches are clear before writes
are moved to new locations. Having a high cache time will make migrations
slower. Having a low cache time will result in more hits to the metadata
cluster.

.. code-block:: python

    shardmonster.activate_caching(5)


Describe Clusters
-----------------

A cluster describes a replica set that is to store data. Typically, you will
have multiple clusters with data spread across them.

.. code-block:: python

    shardmonster.ensure_cluster_exists(
        'cluster-1', 'mongodb://localhost:27017/?replicaset=cluster-1')

Create Realms
-------------

A realm is a sharded collection. To create a realm you need to have defined a
field to use as a shard field - this will be used to determine on which shard a
document belongs.

The shard field must only contain strings or integers. No other data type is
currently supported here.

.. code-block:: python

    # Create a realm called messages that is a sharded version of the
    # messages_coll collection. The data will be sharded by the field called
    # "account".
    shardmonster.ensure_realm_exists(
        'messages', 'account', 'messages_coll')

Preparing for Queries
---------------------

To actually do sharded queries and inserts you will need a handle to a shard
aware collection.

.. code-block:: python

    sharded_collection = \
        shardmonster.make_collection_shard_aware("messages")
    sharded_collection.insert({"text": "Hello!", "account": 5})
 

Move some data around
---------------------

Before a shard can be moved to a new cluster it must be first placed at rest at
its current location.

.. code-block:: python

    # Following on from the previous block...
    # Set all data in the messages realm with an account value of 5 to be at
    # rest on cluster-1. As the data is already there this does do any movement
    # of data.
    shardmonster.set_shard_at_rest('messages', 5, 'cluster-1/some_db')

Once this is done the data can be migrated to a new location:

.. code-block:: python

    # This moves data from the messages collection with an account value of 5
    # to a different cluster. The method returns when it is completed.
    shardmonster.do_migration('messages', 5, 'cluster-2/some_other_db')

Where is my data?
-----------------

After you've been using shardmonster for some time you might want some help
interrogating your data and finding out where it is.

.. code-block:: python

    >>> shardmonster.where_is('messages', 5)
    'cluster-2/some_other_db'

