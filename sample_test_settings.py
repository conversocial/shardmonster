CONN1 = {
    'uri': 'mongodb://controller:27017/?replicaSet=cluster-1',
    'db_name': 'test_sharding'
}
CONN2 = {
    'uri': 'mongodb://replica:27017/?replicaSet=cluster-2',
    'db_name': 'test_sharding'
}
CONTROLLER = {
    'uri': 'mongodb://controller:27017/?replicaSet=cluster-1',
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
