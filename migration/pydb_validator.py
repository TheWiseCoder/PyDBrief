import sys
from enum import StrEnum
from pypomes_core import (
    str_sanitize, str_splice, exc_format,
    validate_bool, validate_str,
    validate_strs, validate_format_error
)
from pypomes_db import (
    DbEngine, db_get_engines, db_assert_access
)
from pypomes_s3 import (
    S3Engine, s3_get_engines, s3_assert_access
)
from sqlalchemy.sql.elements import Type
from typing import Any, Final

from app_constants import (
    DbConfig, S3Config, MetricsConfig, MigrationConfig
)
from migration.pydb_common import (
    MigrationMetrics, get_rdbms_params, get_s3_params
)
from migration.pydb_types import name_to_type

SERVICE_PARAMS: Final[dict[str, list[str]]] = {
    "/migration:metrics:PATCH":  list(map(str, MetricsConfig)),
    "/migrate:POST": list(map(str, MigrationConfig)),
    "/rdbms:POST": list(map(str, DbConfig)),
    "/s3:POST": list(map(str, S3Config)),
    "/migration:verify:POST": [
        MigrationConfig.FROM_RDBMS, MigrationConfig.TO_RDBMS, MigrationConfig.TO_S3
    ],
}


def assert_params(errors: list[str],
                  service: str,
                  method: str,
                  input_params: dict) -> None:

    params: list[StrEnum] = SERVICE_PARAMS.get(f"{service}:{method}") or []
    # 122 Attribute is unknown or invalid in this context
    errors.extend([validate_format_error(122,
                                         f"@{key}") for key in input_params if key not in params])


def assert_rdbms_dual(errors: list[str],
                      input_params: dict) -> tuple[DbEngine, DbEngine]:

    # initialize the return variable
    result: tuple[DbEngine | None, DbEngine | None] = (None, None)

    engines: list[DbEngine] = db_get_engines()

    from_rdbms: str = validate_str(errors=errors,
                                   source=input_params,
                                   attr=MigrationConfig.FROM_RDBMS,
                                   values=list(map(str, DbEngine)))
    if from_rdbms and DbEngine(from_rdbms) not in engines:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            from_rdbms,
                                            "unknown or unconfigured RDBMS engine",
                                            f"@{MigrationConfig.FROM_RDBMS}"))

    to_rdbms: str = validate_str(errors=errors,
                                 source=input_params,
                                 attr=MigrationConfig.TO_RDBMS,
                                 values=list(map(str, DbEngine)))
    if to_rdbms and DbEngine(to_rdbms) not in engines:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            to_rdbms,
                                            "unknown or unconfigured RDBMS engine",
                                            f"@{MigrationConfig.TO_RDBMS}"))

    if from_rdbms and from_rdbms == to_rdbms:
        # 126: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(126,
                                            to_rdbms,
                                            f"@'{MigrationConfig.FROM_RDBMS}, "
                                            f"{MigrationConfig.TO_RDBMS})'"))
    if not errors:
        result = (DbEngine(from_rdbms), DbEngine(to_rdbms))

    return result


def assert_migration(errors: list[str],
                     inpt_params: dict,
                     run_mode: bool) -> None:

    # validate the migration parameters
    assert_metrics_params(errors=errors)

    # validate the migration steps
    if run_mode:
        assert_migration_steps(errors=errors,
                               input_params=inpt_params)

    # validate the source and target RDBMS engines
    source_rdbms: DbEngine
    target_rdbms: DbEngine
    source_rdbms, target_rdbms = assert_rdbms_dual(errors=errors,
                                                   input_params=inpt_params)
    if source_rdbms and target_rdbms and \
            (source_rdbms != DbEngine.ORACLE or target_rdbms != DbEngine.POSTGRES):
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"The migration path '{source_rdbms} -> {target_rdbms}' "
                                            "has not been validated yet. For details, please email the developer."))
    # verify  database runtime capabilities
    if source_rdbms and run_mode:
        db_assert_access(errors=errors,
                         engine=source_rdbms)
    if target_rdbms and run_mode:
        db_assert_access(errors=errors,
                         engine=target_rdbms)
    # validate S3
    to_s3: str = validate_str(errors=errors,
                              source=inpt_params,
                              attr=MigrationConfig.TO_S3,
                              values=list(map(str, S3Engine)))
    if to_s3:
        s3_engine: S3Engine = S3Engine(to_s3)
        if s3_engine not in s3_get_engines() or \
                (run_mode and not s3_assert_access(errors=errors,
                                                   engine=s3_engine)):
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                to_s3,
                                                "unknown or unconfigured S3 engine",
                                                f"@{MigrationConfig.TO_S3}"))


def assert_metrics_params(errors: list[str]) -> None:

    param: int = MigrationMetrics.get(MetricsConfig.BATCH_SIZE_IN)
    if not (param == 0 or 1000 <= param <= 10000000):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1000, 10000000],
                                            f"@{MetricsConfig.BATCH_SIZE_IN}"))
    param = MigrationMetrics.get(MetricsConfig.BATCH_SIZE_OUT)
    if not (param == 0 or 1000 <= param <= 10000000):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1000, 10000000],
                                            f"@{MetricsConfig.BATCH_SIZE_OUT}"))
    param = MigrationMetrics.get(MetricsConfig.CHUNK_SIZE)
    if not 1024 <= param <= 16777216:
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1024, 16777216],
                                            f"@{MetricsConfig.CHUNK_SIZE}"))
    param = MigrationMetrics.get(MetricsConfig.INCREMENTAL_SIZE)
    if not 1000 <= param <= 10000000:
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1000, 10000000],
                                            f"@{MetricsConfig.INCREMENTAL_SIZE}"))


def assert_migration_steps(errors: list[str],
                           input_params: dict) -> None:

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
                            scheme: dict[str, Any]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the foreign columns list
    override_columns: list[str] = validate_strs(errors=errors,
                                                source=scheme,
                                                attr=MigrationConfig.OVERRIDE_COLUMNS) or []
    try:
        rdbms: DbEngine = DbEngine(scheme.get("to-rdbms"))
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
        exc_err: str = str_sanitize(target_str=exc_format(exc=e,
                                                          exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            f"@{MigrationConfig.OVERRIDE_COLUMNS}"))
    return result


def assert_incremental_migrations(errors: list[str],
                                  scheme: dict[str, Any]) -> dict[str, tuple[int, int]]:

    # initialize the return variable
    result: dict[str, tuple[int, int]] = {}

    # process the foreign columns list
    incremental_tables: list[str] = \
        validate_strs(errors=errors,
                      source=scheme,
                      attr=MigrationConfig.INCREMENTAL_MIGRATIONS) or []
    try:
        # format of 'incremental_tables' is [<table_name>[=<size>[:<offset>],...]
        for incremental_table in incremental_tables:
            # noinspection PyTypeChecker
            terms: tuple[str, str, str] = str_splice(source=incremental_table,
                                                     seps=["=", ":"])
            size: int = None if terms[1] is None or not terms[1].isdigit() else int(terms[1])
            offset: int = None if terms[2] is None or not terms[2].isdigit() else int(terms[2])
            result[terms[0]] = (size, offset)
    except Exception as e:
        exc_err: str = str_sanitize(target_str=exc_format(exc=e,
                                                          exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            f"@{MigrationConfig.INCREMENTAL_MIGRATIONS}"))
    return result


def get_migration_context(errors: list[str],
                          scheme: dict) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    # obtain the source RDBMS parameters
    rdbms: str = scheme.get(MigrationConfig.FROM_RDBMS)
    from_rdbms: DbEngine = DbEngine(rdbms) if rdbms else None
    from_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                   db_engine=from_rdbms)
    # obtain the target RDBMS parameters
    rdbms = scheme.get(MigrationConfig.TO_RDBMS)
    to_rdbms: DbEngine = DbEngine(rdbms) if rdbms else None
    to_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                 db_engine=to_rdbms)
    # obtain the target S3 parameters
    s3_params: dict[str, Any] | None = None
    s3: str = scheme.get(MigrationConfig.TO_S3)
    to_s3: S3Engine = S3Engine(s3) if s3 in S3Engine else None
    if to_s3:
        s3_params = get_s3_params(errors=errors,
                                  s3_engine=to_s3)
    # build the return data
    if not errors:
        result: dict[str, Any] = {
            "metrics": MigrationMetrics,
            MigrationConfig.FROM_RDBMS: from_params,
            MigrationConfig.TO_RDBMS: to_params
        }
        if s3_params:
            result[MigrationConfig.TO_S3] = s3_params

    return result
