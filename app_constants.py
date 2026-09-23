from enum import StrEnum, auto
from pypomes_core import APP_PREFIX, env_get_str
from typing import Final

PYDB_DB_ENGINE: Final[str] = env_get_str(key="PYDB_DB_ENGINE",
                                         def_value="pydbrief")
PYDB_S3_ENGINE: Final[str] = env_get_str(key="PYDB_DB_ENGINE")
REGISTRY_DOCKER: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_DOCKER")
REGISTRY_HOST: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_HOST")


class InputParam(StrEnum):
    """
    Parameters for data input.
    """
    # entities
    DATABASE = "database"
    MIGRATION = "migration"
    MIGRATION_ISSUE = "migration-issue"
    MIGRATION_REPORT = "migration-report"
    MIGRATION_SPAN = "migration-span"
    MIGRATION_TABLE = "migration-table"
    MIGRATION_WORK = "migration-work"
    S3 = "s3"
    SESSION = "session"
    SOURCE_DB = "source-db"
    TARGET_DB = "target-db"
    TARGET_S3 = "target-s3"

    # database properties
    DB_CLIENT = "db-client"
    DB_DRIVER = "db-driver"
    DB_ENGINE = "db-engine"
    DB_HOST = "db-host"
    DB_NAME = "db-name"
    DB_PORT = "db-port"
    DB_PWD = "db-pwd"
    DB_TYPE = "db-type"
    DB_USER = "db-user"
    DB_VERSION = "db-version"

    # s3 properties
    S3_ACCESS_KEY = "s3-access-key"
    S3_BUCKET_NAME = "s3-bucket-name"
    S3_ENDPOINT_URL = "s3-endpoint-url"
    S3_ENGINE = "s3-engine"
    S3_REGION_NAME = "s3-region-name"
    S3_SECRET_KEY = "s3-secret-key"
    S3_SECURE_ACCESS = "s3-secure-access"
    S3_TYPE = "s3-type"
    S3_VERSION = "s3-version"

    CLIENT_ID = "client-id"
    ENGINE_ID = "engine-id"
    ISSUE_ID = "issue-id"
    MIGRATION_ID = "migration-id"
    SESSION_ID = "session-id"
    TABLE_ID = "table-id"

    BADGE = "badge"
    BATCH_SIZE_IN = "batch-size-in"
    BATCH_SIZE_OUT = "batch-size-out"
    CD_BADGE = "cd-badge"
    CD_ENGINE = "cd-engine"
    CD_ISSUE = "cd-issue"
    CD_SESSION = "cd-session"
    CD_TABLE = "cd-table"
    CHUNK_SIZE = "chunk-size"
    CREATION = "creation"
    EXCLUDE_COLUMNS = "exclude-columns"
    EXCLUDE_CONSTRAINTS = "exclude-constraints"
    EXCLUDE_RELATIONS = "exclude-relations"
    FLATTEN_STORAGE = "flatten-storage"
    INCLUDE_RELATIONS = "include-relations"
    INCREMENTAL_COUNT = "incremental-count"
    INCREMENTAL_OFFSET = "incremental-offset"
    ISSUE = "issue"
    LOBDATA_CHANNELS = "lobdata-channels"
    LOBDATA_CHANNEL_SIZE = "lobdata-channel-size"
    NAMED_LOBDATA = "named-lobdata"
    OMIT_DEFAULTS = "omit-defaults"
    OPTIMIZE_PKS = "optimize-pks"
    OVERRIDE_COLUMNS = "override-columns"
    PATH = "path"
    PLAINDATA_CHANNELS = "plaindata-channels"
    PLAINDATA_CHANNEL_SIZE = "plaindata-channel-size"
    PROCESS_INDEXES = "process-indexes"
    PROCESS_VIEWS = "process-views"
    REFLECT_FILETYPE = "reflect-filetype"
    RELAX_REFLECTION = "relax-reflection"
    REMOVE_CTRLCHARS = "remove-ctrlchars"
    REPORTS = "reports"
    SKIP_NONEMPTY = "skip-nonempty"
    SOURCE_SCHEMA = "source-schema"
    STEP = "step"
    TARGET_SCHEMA = "target-schema"

    CUSTOM_TABLES = "custom-tables"
    DESCRIPTION = "description"
    DONE = "done"
    FINISH = "finish"
    FIRST_ROW = "first-row"
    ISSUES = "issues"
    LAST_ROW = "last-row"
    NAME = "name"
    ONSET = "onset"
    SPANS = "spans"
    SPECS = "specs"
    START = "start"
    STATE = "state"
    TABLE = "table"
    TABLES = "tables"
    TYPE = "type"
    WORK_TABLES = "custom-tables"


class OpType(StrEnum):
    """
    Tipos de operação.
    """
    CREATE = auto()
    DELETE = auto()
    RETRIEVE = auto()
    UPDATE = auto()
    VERIFY = auto()
