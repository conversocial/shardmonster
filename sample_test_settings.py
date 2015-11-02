CONN1 = {'host': 'localhost', 'port': 27017, 'db_name': 'test_sharding'}
CONN2 = {'host': 'localhost', 'port': 27018, 'db_name': 'test_sharding'}

# Controls how many test runs of each size we do during integration tests.
INTEGRATION_TEST_RUNS = (
    [10] * 2 +
    [100] * 10 +
    [1000] * 0 +
    [10000] * 0 +
    [100000] * 0
)
