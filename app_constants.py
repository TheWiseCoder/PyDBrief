from enum import StrEnum, auto
from pypomes_core import APP_PREFIX, env_get_str
from typing import Final

REGISTRY_DOCKER: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_DOCKER")
REGISTRY_HOST: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_HOST")


class MigrationState(StrEnum):
    """
    Migration states for session.
    """
    ACTIVE = auto()
    INACTIVE = auto()
    MIGRATING = auto()
    ABORTING = auto()
    ABORTED = auto()
    FINISHED = auto()


class MigConfig(StrEnum):
    """
    Parameters grouping.
    """
    METRICS = "metrics"
    SPECS = "specs"
    SPOTS = "spots"
    STEPS = "steps"


class MigSpot(StrEnum):
    """
    Sources and targets for migration.
    """
    FROM_RDBMS = "from-rdbms"
    TO_RDBMS = "to-rdbms"
    TO_S3 = "to-s3"


class MigStep(StrEnum):
    """
    Steps for migration.
    """
    MIGRATE_METADATA = "migrate-metadata"
    MIGRATE_PLAINDATA = "migrate-plaindata"
    MIGRATE_LOBDATA = "migrate-lobdata"
    SYNCHRONIZE_PLAINDATA = "synchronize-plaindata"


class MigSpec(StrEnum):
    """
    Specs for migration.
    """
    CLIENT_ID = "client-id"
    EXCLUDE_COLUMNS = "exclude-columns"
    EXCLUDE_CONSTRAINTS = "exclude-constraints"
    EXCLUDE_RELATIONS = "exclude-relations"
    FLATTEN_STORAGE = "flatten-storage"
    FROM_SCHEMA = "from-schema"
    INCLUDE_RELATIONS = "include-relations"
    INCREMENTAL_MIGRATIONS = "incremental-migrations"
    IS_ACTIVE = "is-active"
    MIGRATION_BADGE = "migration-badge"
    NAMED_LOBDATA = "named-lobdata"
    OVERRIDE_COLUMNS = "override-columns"
    PROCESS_INDEXES = "process-indexes"
    PROCESS_VIEWS = "process-views"
    REFLECT_FILETYPE = "reflect-filetype"
    RELAX_REFLECTION = "relax-reflection"
    REMOVE_NULLS = "remove-nulls"
    SESSION_ID = "session-id"
    SKIP_NONEMPTY = "skip-nonempty"
    STATE = "state"
    TO_SCHEMA = "to-schema"


class MigMetric(StrEnum):
    """
    Metrics for migration.
    """
    BATCH_SIZE_IN = "batch-size-in"
    BATCH_SIZE_OUT = "batch-size-out"
    CHUNK_SIZE = "chunk-size"
    INCREMENTAL_SIZE = "incremental-size"
    LOBDATA_CHANNELS = "lobdata-channels"
    LOBDATA_CHANNEL_SIZE = "lobdata-channel-size"
    PLAINDATA_CHANNELS = "plaindata-channels"
    PLAINDATA_CHANNEL_SIZE = "plaindata-channel-size"


class DbConfig(StrEnum):
    """
    Parameters for database configuration.
    """
    CLIENT = "db-client"
    DRIVER = "db-driver"
    ENGINE = "db-engine"
    HOST = "db-host"
    NAME = "db-name"
    PORT = "db-port"
    PWD = "db-pwd"
    USER = "db-user"
    VERSION = "version"


class S3Config(StrEnum):
    """
    Parameters for S3 configuration.
    """
    ACCESS_KEY = "s3-access-key"
    BUCKET_NAME = "s3-bucket-name"
    ENDPOINT_URL = "s3-endpoint-url"
    ENGINE = "s3-engine"
    REGION_NAME = "s3-region-name"
    SECRET_KEY = "s3-secret-key"
    SECURE_ACCESS = "s3-secure-access"
    VERSION = "version"


# values are (min, max, default)
RANGE_BATCH_SIZE_IN: Final[tuple[int, int, int]] = (1000, 1000000, 1000000)
RANGE_BATCH_SIZE_OUT: Final[tuple[int, int, int]] = (1000, 1000000, 1000000)
RANGE_CHUNK_SIZE: Final[tuple[int, int, int]] = (1024, 16777216, 1048576)
RANGE_INCREMENTAL_SIZE: Final[tuple[int, int, int]] = (1000, 10000000, 100000)
RANGE_LOBDATA_CHANNELS: Final[tuple[int, int, int]] = (1, 128, 1)
RANGE_LOBDATA_CHANNEL_SIZE: Final[tuple[int, int, int]] = (10000, 1000000, 100000)
RANGE_PLAINDATA_CHANNELS: Final[tuple[int, int, int]] = (1, 128, 1)
RANGE_PLAINDATA_CHANNEL_SIZE: Final[tuple[int, int, int]] = (10000, 1000000, 100000)
