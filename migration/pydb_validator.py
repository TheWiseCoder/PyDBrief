import sys
from enum import StrEnum
from pypomes_core import (
    str_sanitize, str_splice, str_is_int,
    exc_format, validate_bool, validate_enum,
    validate_str, validate_strs, validate_format_error
)
from pypomes_db import (
    DbEngine, db_get_engines, db_assert_access
)
from pypomes_http import HttpMethod
from pypomes_s3 import (
    S3Engine, s3_get_engines, s3_assert_access
)
from sqlalchemy.sql.elements import Type
from typing import Any, Final

from app_constants import (
    DbConfig, S3Config, MetricsConfig, MigrationConfig, MigrationState,
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE,
    RANGE_PLAINDATA_CHANNELS, RANGE_LOBDATA_CHANNELS
)
from migration.pydb_common import (
    get_metrics_params, get_rdbms_params, get_s3_params
)
from migration.pydb_sessions import get_session_state
from migration.pydb_types import name_to_type

SERVICE_PARAMS: Final[dict[str, list[str]]] = {
    f"/sessions:{HttpMethod.PATCH}": [MigrationConfig.IS_ACTIVE],
    f"/rdbms:{HttpMethod.POST}": list(map(str, DbConfig)),
    f"/s3:{HttpMethod.POST}": list(map(str, S3Config)),
    f"/migrate:{HttpMethod.POST}": list(map(str, MigrationConfig)),
    f"/migration/metrics:{HttpMethod.PATCH}":  list(map(str, MetricsConfig)),
    f"/migration:verify:{HttpMethod.POST}": [
        MigrationConfig.FROM_RDBMS, MigrationConfig.TO_RDBMS, MigrationConfig.TO_S3
    ],
}


def assert_expected_params(errors: list[str],
                           service: str,
                           method: str,
                           input_params: dict[str, str]) -> None:

    op: str = f"{service}:{method}"
    params: list[StrEnum] = SERVICE_PARAMS.get(op) or []
    params.append(MigrationConfig.CLIENT_ID)
    if op != f"/sessions/{HttpMethod.GET}":
        params.append(MigrationConfig.SESSION_ID)
    # 122 Attribute is unknown or invalid in this context
    errors.extend([validate_format_error(122,
                                         f"@{key}") for key in input_params if key not in params])


def assert_rdbms_dual(errors: list[str],
                      input_params: dict[str, str]) -> tuple[DbEngine, DbEngine]:

    # initialize the return variable
    result: tuple[DbEngine | None, DbEngine | None] = (None, None)

    from_rdbms: DbEngine = validate_enum(errors=errors,
                                         source=input_params,
                                         attr=MigrationConfig.FROM_RDBMS,
                                         enum_class=DbEngine,
                                         required=True)
    if not errors and from_rdbms not in db_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            from_rdbms,
                                            "unknown or unconfigured DB engine",
                                            f"@{MigrationConfig.FROM_RDBMS}"))

    to_rdbms: DbEngine = validate_enum(errors=errors,
                                       source=input_params,
                                       attr=MigrationConfig.TO_RDBMS,
                                       enum_class=DbEngine,
                                       required=True)
    if not errors and to_rdbms not in db_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            to_rdbms,
                                            "unknown or unconfigured DB engine",
                                            f"@{MigrationConfig.TO_RDBMS}"))

    if not errors and from_rdbms == to_rdbms:
        # 126: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(126,
                                            to_rdbms,
                                            f"@'{MigrationConfig.FROM_RDBMS}, "
                                            f"{MigrationConfig.TO_RDBMS})'"))
    if not errors:
        result = (from_rdbms, to_rdbms)

    return result


def assert_migration(errors: list[str],
                     input_params: dict[str, str],
                     run_mode: bool) -> None:

    # validate the metric parameters
    session_id: str = input_params.get(MigrationConfig.SESSION_ID)
    migration_metrics: dict[MetricsConfig, int] = get_metrics_params(session_id=session_id)
    assert_metrics(errors=errors,
                   migration_metrics=migration_metrics)
    if run_mode:
        # validate the migration steps
        assert_migration_steps(errors=errors,
                               input_params=input_params)

    # validate the migration session
    state: MigrationState = get_session_state(session_id=session_id)
    if state in [MigrationState.MIGRATING, MigrationState.ABORTING]:
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Operation not possible for session with state '{state}'"))

    # validate the source and target RDBMS engines
    rdbms: tuple[DbEngine, DbEngine] = assert_rdbms_dual(errors=errors,
                                                         input_params=input_params)
    source_rdbms: DbEngine = rdbms[0]
    target_rdbms: DbEngine = rdbms[1]
    if source_rdbms and target_rdbms and \
            (source_rdbms != DbEngine.ORACLE or target_rdbms != DbEngine.POSTGRES):
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"The migration path '{source_rdbms} -> {target_rdbms}' "
                                            "has not been validated yet. For details, please email the developer."))
    # validate S3
    s3_engine: S3Engine = validate_enum(errors=errors,
                                        source=input_params,
                                        attr=MigrationConfig.TO_S3,
                                        enum_class=S3Engine,
                                        required=False)
    if s3_engine and s3_engine not in s3_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine,
                                            "unknown or unconfigured S3 engine",
                                            f"@{MigrationConfig.TO_S3}"))
    if run_mode:
        # verify  database runtime capabilities
        if source_rdbms and not db_assert_access(errors=errors,
                                                 engine=source_rdbms):
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Source database '{source_rdbms}' "
                                                "is not accessible as configured"))
        if target_rdbms and not db_assert_access(errors=errors,
                                                 engine=target_rdbms):
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Target database '{source_rdbms}' "
                                                "is not accessible as configured"))
        if s3_engine and not s3_assert_access(errors=errors,
                                              engine=s3_engine):
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Target S3 '{s3_engine}' "
                                                "is not accessible as configured"))


def assert_metrics(errors: list[str],
                   migration_metrics: dict[MetricsConfig, int]) -> None:

    param: int = migration_metrics.get(MetricsConfig.BATCH_SIZE_IN)
    min_val: int = RANGE_BATCH_SIZE_IN[0]
    max_val: int = RANGE_BATCH_SIZE_IN[1]
    if not (param == 0 or min_val <= param <= max_val):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [min_val, max_val],
                                            f"@{MetricsConfig.BATCH_SIZE_IN}"))

    param = migration_metrics.get(MetricsConfig.BATCH_SIZE_OUT)
    min_val = RANGE_BATCH_SIZE_OUT[0]
    max_val = RANGE_BATCH_SIZE_OUT[1]
    if not (param == 0 or min_val <= param <= max_val):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [min_val, max_val],
                                            f"@{MetricsConfig.BATCH_SIZE_OUT}"))

    param = migration_metrics.get(MetricsConfig.CHUNK_SIZE)
    min_val = RANGE_CHUNK_SIZE[0]
    max_val = RANGE_CHUNK_SIZE[1]
    if not (param == 0 or min_val <= param <= max_val):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [min_val, max_val],
                                            f"@{MetricsConfig.CHUNK_SIZE}"))

    param = migration_metrics.get(MetricsConfig.INCREMENTAL_SIZE)
    min_val = RANGE_INCREMENTAL_SIZE[0]
    max_val = RANGE_INCREMENTAL_SIZE[1]
    if not (param == 0 or min_val <= param <= max_val):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [min_val, max_val],
                                            f"@{MetricsConfig.INCREMENTAL_SIZE}"))

    param = migration_metrics.get(MetricsConfig.PLAINDATA_CHANNELS)
    min_val = RANGE_PLAINDATA_CHANNELS[0]
    max_val = RANGE_PLAINDATA_CHANNELS[1]
    if not (param == 0 or min_val <= param <= max_val):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [min_val, max_val],
                                            f"@{MetricsConfig.PLAINDATA_CHANNELS}"))

    param = migration_metrics.get(MetricsConfig.LOBDATA_CHANNELS)
    min_val = RANGE_LOBDATA_CHANNELS[0]
    max_val = RANGE_LOBDATA_CHANNELS[1]
    if not (param == 0 or min_val <= param <= max_val):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [min_val, max_val],
                                            f"@{MetricsConfig.LOBDATA_CHANNELS}"))


def assert_migration_steps(errors: list[str],
                           input_params: dict[str, str]) -> None:

    # retrieve the migration steps
    step_metadata: bool = validate_bool(errors=errors,
                                        source=input_params,
                                        attr=MigrationConfig.MIGRATE_METADATA,
                                        required=True)
    step_plaindata: bool = validate_bool(errors=errors,
                                         source=input_params,
                                         attr=MigrationConfig.MIGRATE_PLAINDATA,
                                         required=True)
    step_lobdata: bool = validate_bool(errors=errors,
                                       source=input_params,
                                       attr=MigrationConfig.MIGRATE_LOBDATA,
                                       required=True)
    step_synchronize: bool = validate_bool(errors=errors,
                                           source=input_params,
                                           attr=MigrationConfig.SYNCHRONIZE_PLAINDATA,
                                           required=False)
    # validate them
    err_msg: str | None = None
    if not (step_metadata or step_lobdata or
            step_plaindata or step_synchronize):
        err_msg = "At least one migration step must be specified"
    elif (step_synchronize and
          (step_metadata or step_plaindata or step_lobdata)):
        err_msg = "Synchronization can not be combined with another operation"
    elif step_metadata and step_lobdata and not step_plaindata:
        target_s3: str = validate_str(errors=errors,
                                      source=input_params,
                                      attr=MigrationConfig.TO_S3,
                                      required=False)
        if not target_s3:
            err_msg = "Migrating metadata and lobdata to a database requires migrating plaindata as well"

    if err_msg:
        # 101: {}
        errors.append(validate_format_error(101, err_msg))

    # validate the include and exclude relations lists
    if input_params.get(MigrationConfig.INCLUDE_RELATIONS) and input_params.get(MigrationConfig.EXCLUDE_RELATIONS):
        # 151: Attributes {} cannot be assigned values at the same time
        errors.append(validate_format_error(151,
                                            f"'({MigrationConfig.INCLUDE_RELATIONS}, "
                                            f"{MigrationConfig.EXCLUDE_RELATIONS})'"))


def assert_override_columns(errors: list[str],
                            input_params: dict[str, str]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the foreign columns list
    override_columns: list[str] = validate_strs(errors=errors,
                                                source=input_params,
                                                attr=MigrationConfig.OVERRIDE_COLUMNS) or []
    try:
        rdbms: DbEngine = DbEngine(input_params.get("to-rdbms"))
        for override_column in override_columns:
            # format of 'override_column' is <column_name>=<column_type>
            column_name: str = override_column[:f"={override_column.rindex('=')-1}"]
            type_name: str = override_column.replace(column_name, "", 1)[1:]
            column_type: Type = name_to_type(rdbms=rdbms,
                                             type_name=type_name.lower())
            if column_name and column_type:
                result[column_name.lower()] = column_type
            else:
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(142,
                                                    type_name,
                                                    f"not a valid column type for RDBMS {rdbms}"))
    except Exception as e:
        exc_err: str = str_sanitize(source=exc_format(exc=e,
                                                      exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            f"@{MigrationConfig.OVERRIDE_COLUMNS}"))
    return result


def assert_incremental_migrations(errors: list[str],
                                  input_params: dict[str, Any]) -> dict[str, tuple[int, int]]:

    # initialize the return variable
    result: dict[str, tuple[int, int]] = {}

    # process the foreign columns list
    incremental_tables: list[str] = \
        validate_strs(errors=errors,
                      source=input_params,
                      attr=MigrationConfig.INCREMENTAL_MIGRATIONS) or []

    # format of 'incremental_tables' is [<table_name>[=<size>[:<offset>],...]
    for incremental_table in incremental_tables:
        # noinspection PyTypeChecker
        terms: tuple[str, str, str] = str_splice(source=incremental_table,
                                                 seps=["=", ":"])
        if str_is_int(source=terms[1]):
            size: int = int(terms[1])
        else:
            session_id = input_params.get(MigrationConfig.SESSION_ID)
            session_metrics: dict[MetricsConfig, int] = get_metrics_params(session_id=session_id)
            size: int = session_metrics.get(MetricsConfig.INCREMENTAL_SIZE)
        offset: int = int(terms[2]) if str_is_int(source=terms[2]) else 0
        result[terms[0]] = (size, offset)

    return result


def get_migration_context(errors: list[str],
                          input_params: dict[str, str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    # retrieve the session identification
    session_id: str = input_params.get(MigrationConfig.SESSION_ID)

    # retrieve the source RDBMS parameters
    rdbms: str = input_params.get(MigrationConfig.FROM_RDBMS)
    from_rdbms: DbEngine = DbEngine(rdbms) if rdbms else None
    from_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                   session_id=session_id,
                                                   db_engine=from_rdbms)
    # retrieve the target RDBMS parameters
    rdbms = input_params.get(MigrationConfig.TO_RDBMS)
    to_rdbms: DbEngine = DbEngine(rdbms) if rdbms else None
    to_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                 session_id=session_id,
                                                 db_engine=to_rdbms)
    # retrieve the target S3 parameters
    s3_params: dict[str, Any] | None = None
    s3: str = input_params.get(MigrationConfig.TO_S3)
    to_s3: S3Engine = S3Engine(s3) if s3 in S3Engine else None
    if to_s3:
        s3_params = get_s3_params(errors=errors,
                                  session_id=session_id,
                                  s3_engine=to_s3)
    # build the return data
    if not errors:
        result: dict[str, Any] = {
            MigrationConfig.METRICS: get_metrics_params(session_id=session_id),
            MigrationConfig.FROM_RDBMS: from_params,
            MigrationConfig.TO_RDBMS: to_params
        }
        if s3_params:
            result[MigrationConfig.TO_S3] = s3_params

    return result
