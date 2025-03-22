from enum import StrEnum
from pypomes_core import APP_PREFIX, env_get_str
from typing import Final

REGISTRY_DOCKER: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_DOCKER")
REGISTRY_HOST: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_HOST")


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


class MetricsConfig(StrEnum):
    """
    Metrics for migration.
    """
    BATCH_SIZE_IN = "batch-size-in"
    BATCH_SIZE_OUT = "batch-size-out"
    CHUNK_SIZE = "chunk-size"
    INCREMENTAL_SIZE = "incremental-size"


class MigrationConfig(StrEnum):
    """
    Parameters for migration.
    """
    ACCEPT_EMPTY = "accept-empty"
    EXCLUDE_COLUMNS = "exclude-columns"
    EXCLUDE_CONSTRAINTS = "exclude-constraints"
    EXCLUDE_RELATIONS = "exclude-relations"
    FLATTEN_STORAGE = "flatten-storage"
    FROM_RDBMS = "from-rdbms"
    FROM_SCHEMA = "from-schema"
    INCLUDE_RELATIONS = "include-relations"
    INCREMENTAL_MIGRATION = "incremental-migration"
    MIGRATE_METADATA = "migrate-metadata"
    MIGRATE_PLAINDATA = "migrate-plaindata"
    MIGRATE_LOBDATA = "migrate-lobdata"
    MIGRATION_BADGE = "migration-badge"
    NAMED_LOBDATA = "named-lobdata"
    OVERRIDE_COLUMNS = "override-columns"
    PROCESS_INDEXES = "process-indexes"
    PROCESS_VIEWS = "process-views"
    REFLECT_FILETYPE = "reflect-filetype"
    RELAX_REFLECTION = "relax-reflection"
    REMOVE_NULLS = "remove-nulls"
    SKIP_NONEMPTY = "skip-nonempty"
    SYNCHRONIZE_PLAINDATA = "synchronize-plaindata"
    TO_S3 = "to-s3"
    TO_RDBMS = "to-rdbms"
    TO_SCHEMA = "to-schema"
