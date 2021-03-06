CONN1 = {
    'uri': 'mongodb://replica_1a:27017,replica_1b:27017/?replicaSet=rs-1',
    'db_name': 'test_sharding'
}
HIDDEN_SECONDARY_1 = 'replica_1h:27017'
CONN2 = {
    'uri': 'mongodb://replica_2a:27017/?replicaSet=rs-2',
    'db_name': 'test_sharding'
}
HIDDEN_SECONDARY_2 = 'replica_2h:27017'
CONTROLLER = {
    'uri': 'mongodb://controller:27017/?replicaSet=rs-0',
    'db_name': 'test_metadata'
}

# Controls how many test runs of each size we do during integration tests.
INTEGRATION_TEST_RUNS = (
    [10] * 2 +
    [100] * 0 +
    [1000] * 0 +
    [10000] * 0 +
    [100000] * 0
)
