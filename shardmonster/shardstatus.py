class ShardStatus(object):
    def __init__(self):
        raise Exception('Enum. Do not instantiate')

    AT_REST = "at-rest"
    MIGRATING_COPY = "migrating-copy"
    MIGRATING_SYNC = "migrating-sync"
    POST_MIGRATION_PAUSED_AT_SOURCE = "post-migration-paused-at-source"
    POST_MIGRATION_PAUSED_AT_DESTINATION = "post-migration-paused-destination"
    POST_MIGRATION_DELETE = "post-migration-delete"


MIGRATION_PHASES = {
    ShardStatus.MIGRATING_COPY,
    ShardStatus.MIGRATING_SYNC,
    ShardStatus.POST_MIGRATION_PAUSED_AT_SOURCE,
}
POST_MIGRATION_PHASES = {
    ShardStatus.POST_MIGRATION_PAUSED_AT_DESTINATION,
    ShardStatus.POST_MIGRATION_DELETE,
}
