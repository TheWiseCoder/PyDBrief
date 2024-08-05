from pypomes_core import (
    dict_jsonify,
    validate_bool, validate_str,
    validate_strs, validate_format_error
)
from pypomes_db import (
    db_get_engines, db_assert_connection
)
from pypomes_s3 import (
    s3_get_engines, s3_get_param, s3_assert_access, s3_startup
)
from sqlalchemy.sql.elements import Type
from typing import Any

from migration.pydb_common import (
    MIGRATION_BATCH_SIZE, MIGRATION_CHUNK_SIZE,
    get_migration_metrics, get_rdbms_params, get_s3_params
)
from migration.pydb_types import name_to_type


def assert_rdbms_dual(errors: list[str],
                      scheme: dict) -> tuple[str, str]:

    engines: list[str] = db_get_engines()
    source_rdbms: str | None = scheme.get("from-rdbms")
    if source_rdbms not in engines:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            source_rdbms,
                                            "unknown or unconfigured RDBMS engine",
                                            "@from-rdbms"))
        source_rdbms = None

    target_rdbms: str | None = scheme.get("to-rdbms")
    if target_rdbms not in engines:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            target_rdbms,
                                            "unknown or unconfigured RDBMS engine",
                                            "@to-rdbms"))
        target_rdbms = None

    if source_rdbms and source_rdbms == target_rdbms:
        # 126: {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(126,
                                            source_rdbms,
                                            "'from-rdbms' and 'to-rdbms'"))
    return source_rdbms, target_rdbms


def assert_migration(errors: list[str],
                     scheme: dict,
                     run_mode: bool) -> None:

    # validate the migration parameters
    assert_metrics_params(errors=errors)

    # validate the migration steps
    assert_migration_steps(errors=errors,
                           scheme=scheme)

    # validate the source and target RDBMS engines
    source_rdbms, target_rdbms = assert_rdbms_dual(errors=errors,
                                                   scheme=scheme)
    if source_rdbms and run_mode:
        db_assert_connection(errors=errors,
                             engine=source_rdbms)
    if target_rdbms and run_mode:
        db_assert_connection(errors=errors,
                             engine=target_rdbms)
    if not errors and \
       (source_rdbms != "oracle" or target_rdbms != "postgres"):
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"The migration path '{source_rdbms}->{target_rdbms}' has not "
                                            "been validated yet. For details, please email the developer."))
    # validate S3
    to_s3: str = validate_str(errors=errors,
                              scheme=scheme,
                              attr="to-s3",
                              values=["aws", "minio"])
    # validate S3
    if to_s3 in s3_get_engines():
        s3_assert_access(errors=errors,
                         engine=to_s3)
        if not errors and run_mode:
            bucket: str = s3_get_param(key="bucket-name",
                                       engine=to_s3)
            s3_startup(errors=errors,
                       engine=to_s3,
                       bucket=bucket)
    elif to_s3:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            to_s3,
                                            "unknown or unconfigured S3 engine",
                                            "@to-s3"))


def assert_metrics_params(errors: list[str]) -> None:

    if MIGRATION_BATCH_SIZE < 1000 or \
       MIGRATION_BATCH_SIZE > 10000000:
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            MIGRATION_BATCH_SIZE,
                                            [1000, 10000000],
                                            "@batch-size"))
    if MIGRATION_CHUNK_SIZE < 1024 or \
       MIGRATION_CHUNK_SIZE > 16777216:
        # 151: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(151,
                                            MIGRATION_CHUNK_SIZE,
                                            [1024, 16777216],
                                            "@chunk-size"))


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
    # validate them
    err_msg: str | None = None
    if not step_metadata and not step_lobdata and not step_plaindata:
        err_msg = "At least one migration step must be specified"
    elif step_metadata and step_lobdata and not step_plaindata:
        err_msg = "Migrating metadata and LOBs requires migrating plain data as well"
    if err_msg:
        # 101: {}
        errors.append(validate_format_error(101, err_msg))

    # validate the include and exclude relations lists
    if scheme.get("include-relations") and scheme.get("exclude-relations"):
        # 151: "Attributes {} cannot be assigned values at the same time
        errors.append(validate_format_error(151,
                                            "'include-relations', 'exclude-relations'"))


def assert_column_types(errors: list[str],
                        scheme: dict[str, Any]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the foreign columns list
    foreign_columns: list[str] = validate_strs(errors=errors,
                                               scheme=scheme,
                                               attr="override-columns")
    if foreign_columns:
        rdbms: str = scheme.get("to-rdbms")
        result: dict[str, Type] = {}
        for foreign_column in foreign_columns:
            # format of 'foreign_column' is <column_name>=<column_type>
            column_name: str = foreign_column[:f"={foreign_column}".rindex('=')-1]
            type_name: str = foreign_column.replace(column_name, "", 1)[1:]
            column_type: Type = name_to_type(rdbms=rdbms,
                                             type_name=type_name.lower())
            if column_name and column_type:
                result[column_name.lower()] = column_type
            else:
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(142,
                                                    type_name,
                                                    f"not a valid column type for RDBMS {rdbms}"))
    return result


def get_migration_context(errors: list[str],
                          scheme: dict) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    # obtain the source RDBMS parameters
    from_rdbms: str = scheme.get("from-rdbms")
    from_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                   rdbms=from_rdbms)
    if from_params:
        dict_jsonify(source=from_params)
        from_params["rdbms"] = from_rdbms

    # obtain the target RDBMS parameters
    to_rdbms: str = scheme.get("to-rdbms")
    to_params: dict[str, Any] = get_rdbms_params(errors=errors,
                                                 rdbms=to_rdbms)
    if to_params:
        dict_jsonify(source=to_params)
        to_params["rdbms"] = to_rdbms

    # obtain the target S3 parameters
    s3_params: dict[str, Any] | None = None
    to_s3: str = scheme.get("to-s3")
    if to_s3:
        s3_params = get_s3_params(errors=errors,
                                  s3_engine=to_s3)
        dict_jsonify(source=s3_params)

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
