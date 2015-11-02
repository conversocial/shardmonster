Introduction to Shardmonster
============================

Mongo has great support for sharding. However, the system must be entirely the
same version. This means that you cannot selectively move data from Mongo 2.4 to
Mongo 3.0 (for example). Big bang upgrades of databases are scary (and we've had
downtime in the past when we tried to do an upgrade to 2.6). Writing this
ourselves allows us to move data between different shards of differing versions
at will.

Terminology
-----------

This is made more difficult by the fact that a Mongo server can contain multiple
databases.

**Cluster** - The complete set of mongo servers that store all data.

**Location** - A server (or replica set) combined with a database name that
contains data. A single cluster will contain multiple locations.

**Realm** - A collection that spans the cluster. May have data stored
in multiple versions of Mongo. A realm may span multiple locations (depending on
how much the data has been sharded). A cluster may contain multiple realms of
data.

**Shard** - A set of data denoted by a field (the shard field) and a value (the
shard key). The shard, during a migration, may be stored in multiple locations.
A realm typically contains multiple shards. A location also contains multiple
shards.

Further documentation
=====================

.. toctree::
   :maxdepth: 2

   getting-started
   api
   how-it-works
   developing

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
