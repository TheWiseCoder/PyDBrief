import sys
from pypomes_core import (
    exc_format, str_sanitize,
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

from migration.pydb_common import (
    MIGRATION_METRICS, Metrics,
    get_migration_metrics, get_rdbms_params, get_s3_params
)
from migration.pydb_types import name_to_type

SERVICE_PARAMS: Final[dict[str, list[str]]] = {
    "/migration:metrics:PATCH": ["batch-size-in", "batch-size-out", "chunk-size", "incremental-size"],
    "/migration:verify:POST": ["from-rdbms", "to-rdbms", "to-s3"],
    "/migrate:POST": ["from-rdbms", "from-schema", "to-rdbms", "to-schema", "to-s3",
                      "migrate-metadata", "migrate-plaindata", "migrate-lobdata", "synchronize-plaindata",
                      "process-indexes", "process-views", "relax-reflection", "accept-empty",
                      "skip-nonempty", "reflect-filetype", "flatten-storage", "remove-nulls",
                      "incremental-migration", "include-relations", "exclude-relations", "exclude-constraints",
                      "exclude-columns", "override-columns", "named-lobdata", "migration-badge"],
    "/rdbms:POST": ["db-engine", "db-name", "db-user", "db-pwd",
                    "db-host", "db-port", "db-client", "db-driver"],
    "/s3:POST": ["s3-engine", "s3-endpoint-url", "s3-bucket-name",
                 "s3-access-key", "s3-secret-key", "s3-region-name", "s3-secure-access"]
}


def assert_params(errors: list[str],
                  service: str,
                  method: str,
                  scheme: dict) -> None:

    params: list[str] = SERVICE_PARAMS.get(f"{service}:{method}") or []
    for key in scheme:
        if key not in params:
            # 122: Attribute is unknown or invalid in this context
            errors.append(validate_format_error(122,  # noqa: PERF401
                                                f"@{key}"))


def assert_rdbms_dual(errors: list[str],
                      scheme: dict) -> tuple[DbEngine, DbEngine]:

    engines: list[DbEngine] = db_get_engines()

    source_rdbms: str | None = scheme.get("from-rdbms")
    result_source_rdbms: DbEngine = DbEngine(source_rdbms) if source_rdbms in DbEngine else None
    if result_source_rdbms not in engines:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            source_rdbms,
                                            "unknown or unconfigured RDBMS engine",
                                            "@from-rdbms"))

    target_rdbms: str | None = scheme.get("to-rdbms")
    result_target_rdbms: DbEngine = DbEngine(target_rdbms) if target_rdbms in DbEngine else None
    if result_target_rdbms not in engines:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            target_rdbms,
                                            "unknown or unconfigured RDBMS engine",
                                            "@to-rdbms"))
    if result_source_rdbms and \
       result_source_rdbms == result_target_rdbms:
        # 126: {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(126,
                                            source_rdbms,
                                            "'from-rdbms' and 'to-rdbms'"))

    return result_source_rdbms, result_target_rdbms


def assert_migration(errors: list[str],
                     scheme: dict,
                     run_mode: bool) -> None:

    # validate the migration parameters
    assert_metrics_params(errors=errors)

    # validate the migration steps
    if run_mode:
        assert_migration_steps(errors=errors,
                               scheme=scheme)

    # validate the source and target RDBMS engines
    source_rdbms, target_rdbms = assert_rdbms_dual(errors=errors,
                                                   scheme=scheme)
    if source_rdbms and run_mode:
        db_assert_access(errors=errors,
                         engine=source_rdbms)
    if target_rdbms and run_mode:
        db_assert_access(errors=errors,
                         engine=target_rdbms)
    if not errors and \
       (source_rdbms != "oracle" or target_rdbms != "postgres"):
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"The migration path '{source_rdbms} -> {target_rdbms}' has not "
                                            "been validated yet. For details, please email the developer."))
    # validate S3
    to_s3: str = validate_str(errors=errors,
                              scheme=scheme,
                              attr="to-s3",
                              values=list(map(str, S3Engine)))
    if to_s3 and \
            (S3Engine(to_s3) not in s3_get_engines() or
             (run_mode and not s3_assert_access(errors=errors,
                                                engine=to_s3))):
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            to_s3,
                                            "unknown or unconfigured S3 engine",
                                            "@to-s3"))


def assert_metrics_params(errors: list[str]) -> None:

    param: int = MIGRATION_METRICS.get(Metrics.BATCH_SIZE_IN)
    if not (param == 0 or 1000 <= param <= 10000000):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1000, 10000000],
                                            "@batch-size-in"))
    param = MIGRATION_METRICS.get(Metrics.BATCH_SIZE_OUT)
    if not (param == 0 or 1000 <= param <= 10000000):
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1000, 10000000],
                                            "@batch-size-out"))
    param = MIGRATION_METRICS.get(Metrics.CHUNK_SIZE)
    if not 1024 <= param <= 16777216:
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1024, 16777216],
                                            "@chunk-size"))
    param = MIGRATION_METRICS.get(Metrics.INCREMENTAL_SIZE)
    if not 1000 <= param <= 10000000:
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            param,
                                            [1000, 10000000],
                                            "@incremental-size"))


def assert_migration_steps(errors: list[str],
                           scheme: dict) -> None:

    # retrieve the migration steps
    step_metadata: bool = validate_bool(errors=errors,
                                        scheme=scheme,
                                        attr="migrate-metadata",
                                        required=True)
    step_plaindata: bool = validate_bool(errors=errors,
                                         scheme=scheme,
                                         attr="migrate-plaindata",
                                         required=True)
    step_lobdata: bool = validate_bool(errors=errors,
                                       scheme=scheme,
                                       attr="migrate-lobdata",
                                       required=True)
    step_synchronize: bool = validate_bool(errors=errors,
                                           scheme=scheme,
                                           attr="synchronize-plaindata",
                                           required=False)
    # validate them
    err_msg: str | None = None
    if not step_metadata and not step_lobdata and \
       not step_plaindata and not step_synchronize:
        err_msg = "At least one migration step must be specified"
    elif (step_synchronize and
          (step_metadata or step_plaindata or step_lobdata)):
        err_msg = "Synchronization can not be combined with another operation"
    elif step_metadata and step_lobdata and not step_plaindata:
        target_s3: str = validate_str(errors=errors,
                                      scheme=scheme,
                                      attr="to-s3",
                                      required=False)
        if not target_s3:
            err_msg = "Migrating metadata and lobdata to a database requires migrating plaindata as well"

    if err_msg:
        # 101: {}
        errors.append(validate_format_error(101, err_msg))

    # validate the include and exclude relations lists
    if scheme.get("include-relations") and scheme.get("exclude-relations"):
        # 151: "Attributes {} cannot be assigned values at the same time
        errors.append(validate_format_error(151,
                                            "'include-relations', 'exclude-relations'"))


def assert_override_columns(errors: list[str],
                            scheme: dict[str, Any]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the foreign columns list
    override_columns: list[str] = validate_strs(errors=errors,
                                                scheme=scheme,
                                                attr="override-columns") or []
    try:
        rdbms: str = scheme.get("to-rdbms")
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
                                            "@override-columns"))
    return result


def assert_incremental_migration(errors: list[str],
                                 scheme: dict[str, Any]) -> dict[str, int]:

    # initialize the return variable
    result: dict[str, int] = {}

    # process the foreign columns list
    incremental_tables: list[str] = validate_strs(errors=errors,
                                                  scheme=scheme,
                                                  attr="incremental-migration") or []
    try:
        for incremental_table in incremental_tables:
            # format of 'incremental_table' is <table_name>[=<offset>]
            pos: int = incremental_table.find("=")
            if pos > 0:
                table_name: str = incremental_table[:pos]
                size: int = int(incremental_table[pos+1:])
                if size == -1:
                    result[table_name] = 0
                else:
                    result[table_name] = size
            else:
                result[incremental_table] = MIGRATION_METRICS.get(Metrics.INCREMENTAL_SIZE)
    except Exception as e:
        exc_err: str = str_sanitize(target_str=exc_format(exc=e,
                                                          exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            "@incremental-migration"))
    return result


def get_migration_context(errors: list[str],
                          scheme: dict) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    # obtain the source RDBMS parameters
    from_rdbms: str = scheme.get("from-rdbms")
    from_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                   db_engine=from_rdbms)
    if from_params:
        from_params["rdbms"] = from_rdbms

    # obtain the target RDBMS parameters
    to_rdbms: str = scheme.get("to-rdbms")
    to_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                 db_engine=to_rdbms)
    if to_params:
        to_params["rdbms"] = to_rdbms

    # obtain the target S3 parameters
    s3_params: dict[str, Any] | None = None
    to_s3: str = scheme.get("to-s3")
    if to_s3:
        s3_params = get_s3_params(errors=errors,
                                  s3_engine=to_s3)
    # build the return data
    if not errors:
        result: dict[str, Any] = {
            "metrics": get_migration_metrics(),
            "from-rdbms": from_params,
            "to-rdbms": to_params
        }
        if s3_params:
            result["to-s3"] = s3_params

    return result
