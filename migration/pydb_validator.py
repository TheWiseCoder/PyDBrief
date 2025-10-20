import sys
from enum import StrEnum
from logging import Logger
from pypomes_core import (
    str_sanitize, str_splice, str_is_int,
    exc_format, validate_int, validate_bool, validate_enum,
    validate_str, validate_strs, validate_format_error
)
from pypomes_db import (
    DbEngine, DbParam, db_setup,
    db_assert_access, db_get_param, db_get_engines
)
from pypomes_http import HttpMethod
from pypomes_s3 import (
    S3Engine, S3Param, s3_setup,
    s3_startup, s3_get_param, s3_get_engines
)
from sqlalchemy.sql.elements import Type
from typing import Any, Final

from app_constants import (
    DbConfig, S3Config, MigMetric, MigConfig,
    MigSpec, MigStep, MigSpot, MigIncremental,
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE,
    RANGE_PLAINDATA_CHANNELS, RANGE_PLAINDATA_CHANNEL_SIZE,
    RANGE_LOBDATA_CHANNELS, RANGE_LOBDATA_CHANNEL_SIZE
)
from migration.pydb_database import db_pool_setup
from migration.pydb_sessions import get_session_registry
from migration.pydb_types import name_to_type

SERVICE_PARAMS: Final[dict[str, list[str]]] = {
    f"/sessions:{HttpMethod.PATCH}": [MigSpec.IS_ACTIVE],
    f"/rdbms:{HttpMethod.POST}": list(map(str, DbConfig)),
    f"/s3:{HttpMethod.POST}": list(map(str, S3Config)),
    f"/migrate:{HttpMethod.POST}": (list(map(str, MigSpot)) + list(map(str, MigStep)) +
                                    list(map(str, MigSpec)) + list(map(str, MigMetric))),
    f"/migration/metrics:{HttpMethod.PATCH}":  list(map(str, MigMetric)),
    f"/migration:verify:{HttpMethod.POST}": [
        MigSpot.FROM_RDBMS, MigSpot.TO_RDBMS, MigSpot.TO_S3
    ],
}


def assert_expected_params(service: str,
                           method: str,
                           input_params: dict[str, str],
                           errors: list[str]) -> None:

    op: str = f"{service}:{method}"
    params: list[StrEnum] = SERVICE_PARAMS.get(op) or []
    params.append(MigSpec.CLIENT_ID)
    params.append(MigSpec.SESSION_ID)

    # 122 Attribute is unknown or invalid in this context
    errors.extend([validate_format_error(122,
                                         f"@{key}") for key in input_params if key not in params])


def validate_rdbms(input_params: dict[str, Any],
                   session_id: str,
                   errors: list[str],
                   logger: Logger) -> None:

    db_engine: DbEngine = validate_enum(source=input_params,
                                        attr=DbConfig.ENGINE,
                                        enum_class=DbEngine,
                                        required=True,
                                        errors=errors)
    db_name: str = validate_str(source=input_params,
                                attr=DbConfig.NAME,
                                required=True,
                                errors=errors)
    db_host: str = validate_str(source=input_params,
                                attr=DbConfig.HOST,
                                required=True,
                                errors=errors)
    db_port: int = validate_int(source=input_params,
                                attr=DbConfig.PORT,
                                min_val=1,
                                required=True,
                                errors=errors)
    db_user: str = validate_str(source=input_params,
                                attr=DbConfig.USER,
                                required=True,
                                errors=errors)
    db_pwd: str = validate_str(source=input_params,
                               attr=DbConfig.PWD,
                               required=True,
                               errors=errors)
    db_client: str = validate_str(source=input_params,
                                  attr=DbConfig.CLIENT,
                                  errors=errors)
    db_driver: str = validate_str(source=input_params,
                                  attr=DbConfig.DRIVER,
                                  errors=errors)
    if not errors:
        if db_setup(engine=db_engine,
                    db_name=db_name,
                    db_host=db_host,
                    db_port=db_port,
                    db_user=db_user,
                    db_pwd=db_pwd,
                    db_client=db_client,
                    db_driver=db_driver):
            # add DB specs to registry
            session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
            session_registry[db_engine] = {
                DbConfig.ENGINE: db_engine,
                DbConfig.NAME: db_name,
                DbConfig.HOST: db_host,
                DbConfig.PORT: db_port,
                DbConfig.USER: db_user,
                DbConfig.PWD: db_pwd,
                DbConfig.VERSION: ""
            }
            if db_client:
                session_registry[db_engine][DbConfig.CLIENT] = db_client
            if db_driver:
                session_registry[db_engine][DbConfig.DRIVER] = db_driver
            # build the connection pool
            db_pool_setup(rdbms=db_engine,
                          errors=errors,
                          logger=logger)
        else:
            # 145: Invalid, inconsistent, or missing arguments
            errors.append(validate_format_error(145))


def validate_s3(input_params: dict[str, Any],
                session_id: str,
                errors: list[str]) -> None:

    engine: S3Engine = validate_enum(source=input_params,
                                     attr=S3Config.ENGINE,
                                     enum_class=S3Engine,
                                     required=True,
                                     errors=errors)
    endpoint_url: str = validate_str(source=input_params,
                                     attr=S3Config.ENDPOINT_URL,
                                     required=True,
                                     errors=errors)
    bucket_name: str = validate_str(source=input_params,
                                    attr=S3Config.BUCKET_NAME,
                                    required=True,
                                    errors=errors)
    access_key: str = validate_str(source=input_params,
                                   attr=S3Config.ACCESS_KEY,
                                   required=True,
                                   errors=errors)
    secret_key: str = validate_str(source=input_params,
                                   attr=S3Config.SECRET_KEY,
                                   required=True,
                                   errors=errors)
    region_name: str = validate_str(source=input_params,
                                    attr=S3Config.REGION_NAME,
                                    errors=errors)
    secure_access: bool = validate_bool(source=input_params,
                                        attr=S3Config.SECURE_ACCESS,
                                        errors=errors)
    if not errors:
        if s3_setup(engine=engine,
                    endpoint_url=endpoint_url,
                    bucket_name=bucket_name,
                    access_key=access_key,
                    secret_key=secret_key,
                    region_name=region_name,
                    secure_access=secure_access):
            # add S3 specs to registry
            session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
            session_registry[engine] = {
                S3Config.ENGINE: engine,
                S3Config.ENDPOINT_URL: endpoint_url,
                S3Config.BUCKET_NAME: bucket_name,
                S3Config.ACCESS_KEY: access_key,
                S3Config.SECRET_KEY: secret_key,
                S3Config.SECURE_ACCESS: secure_access,
                S3Config.VERSION: ""
            }
            if region_name:
                session_registry[engine][S3Config.REGION_NAME] = region_name
        else:
            # 145: Invalid, inconsistent, or missing arguments
            errors.append(validate_format_error(145))


def validate_metrics(input_params: dict[str, Any],
                     session_id: str,
                     errors: list[str]) -> None:

    # validate 'batch-size-in'
    batch_size_in: int = validate_int(source=input_params,
                                      attr=MigMetric.BATCH_SIZE_IN,
                                      min_val=RANGE_BATCH_SIZE_IN[0],
                                      max_val=RANGE_BATCH_SIZE_IN[1],
                                      errors=errors)
    # validate 'batch-size-out'
    batch_size_out = validate_int(source=input_params,
                                  attr=MigMetric.BATCH_SIZE_OUT,
                                  min_val=RANGE_BATCH_SIZE_OUT[0],
                                  max_val=RANGE_BATCH_SIZE_OUT[1],
                                  errors=errors)
    # validate 'chunk-size'
    chunk_size: int = validate_int(source=input_params,
                                   attr=MigMetric.CHUNK_SIZE,
                                   min_val=RANGE_CHUNK_SIZE[0],
                                   max_val=RANGE_CHUNK_SIZE[1],
                                   errors=errors)
    # validate 'incremental-size'
    incremental_size: int = validate_int(source=input_params,
                                         attr=MigMetric.INCREMENTAL_SIZE,
                                         min_val=RANGE_INCREMENTAL_SIZE[0],
                                         max_val=RANGE_INCREMENTAL_SIZE[1],
                                         errors=errors)
    # validate 'lobdata-channels'
    lobdata_channels: int = validate_int(source=input_params,
                                         attr=MigMetric.LOBDATA_CHANNELS,
                                         min_val=RANGE_LOBDATA_CHANNELS[0],
                                         max_val=RANGE_LOBDATA_CHANNELS[1],
                                         errors=errors)
    # validate 'lobdata-channel-size'
    lobdata_channel_size: int = validate_int(source=input_params,
                                             attr=MigMetric.LOBDATA_CHANNEL_SIZE,
                                             min_val=RANGE_LOBDATA_CHANNEL_SIZE[0],
                                             max_val=RANGE_LOBDATA_CHANNEL_SIZE[1],
                                             errors=errors)
    # validate 'plaindata-channels'
    plaindata_channels: int = validate_int(source=input_params,
                                           attr=MigMetric.PLAINDATA_CHANNELS,
                                           min_val=RANGE_PLAINDATA_CHANNELS[0],
                                           max_val=RANGE_PLAINDATA_CHANNELS[1],
                                           errors=errors)
    # validate 'plaindata-channel-size'
    plaindata_channel_size: int = validate_int(source=input_params,
                                               attr=MigMetric.PLAINDATA_CHANNEL_SIZE,
                                               min_val=RANGE_PLAINDATA_CHANNEL_SIZE[0],
                                               max_val=RANGE_PLAINDATA_CHANNEL_SIZE[1],
                                               errors=errors)
    if not errors:
        # set the metrics for the session
        session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
        session_metrics: dict[MigMetric, int] = session_registry[MigConfig.METRICS]
        if batch_size_in:
            session_metrics[MigMetric.BATCH_SIZE_IN] = batch_size_in
        if batch_size_out:
            session_metrics[MigMetric.BATCH_SIZE_OUT] = batch_size_out
        if chunk_size:
            session_metrics[MigMetric.CHUNK_SIZE] = chunk_size
        if incremental_size:
            session_metrics[MigMetric.INCREMENTAL_SIZE] = incremental_size
        if lobdata_channels:
            session_metrics[MigMetric.LOBDATA_CHANNELS] = lobdata_channels
        if lobdata_channel_size:
            session_metrics[MigMetric.LOBDATA_CHANNEL_SIZE] = lobdata_channel_size
        if plaindata_channels:
            session_metrics[MigMetric.PLAINDATA_CHANNELS] = plaindata_channels
        if plaindata_channel_size:
            session_metrics[MigMetric.PLAINDATA_CHANNEL_SIZE] = plaindata_channel_size


def validate_spots(input_params: dict[str, str],
                   session_id: str,
                   errors: list[str],
                   logger: Logger) -> None:

    # obtain the session registry
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)

    # validate source RDBMS
    from_rdbms: DbEngine = validate_enum(source=input_params,
                                         attr=MigSpot.FROM_RDBMS,
                                         enum_class=DbEngine,
                                         required=True,
                                         errors=errors)
    if not errors and from_rdbms not in db_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            from_rdbms,
                                            "unknown or unconfigured DB engine",
                                            f"@{MigSpot.FROM_RDBMS}"))
    # validate target RDBMS
    to_rdbms: DbEngine = validate_enum(source=input_params,
                                       attr=MigSpot.TO_RDBMS,
                                       enum_class=DbEngine,
                                       required=True,
                                       errors=errors)
    if not errors and to_rdbms not in db_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            to_rdbms,
                                            "unknown or unconfigured DB engine",
                                            f"@{MigSpot.TO_RDBMS}"))
    # validate source RDBMS vs target RDBMS
    if not errors and from_rdbms == to_rdbms:
        # 126: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(126,
                                            to_rdbms,
                                            f"@'{MigSpot.FROM_RDBMS}, "
                                            f"{MigSpot.TO_RDBMS})'"))
    if not errors and \
            (from_rdbms != DbEngine.ORACLE or to_rdbms != DbEngine.POSTGRES):
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"The migration path '{from_rdbms} -> {to_rdbms}' "
                                            "has not been validated yet. For details, please email the developer."))
    # validate S3
    to_s3: S3Engine = validate_enum(source=input_params,
                                    attr=MigSpot.TO_S3,
                                    enum_class=S3Engine,
                                    errors=errors)
    if to_s3 and to_s3 not in s3_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142, to_s3,
                                            "unknown or unconfigured S3 engine",
                                            f"@{MigSpot.TO_S3}"))
    if not errors:
        # verify source database runtime access
        if from_rdbms and not session_registry[from_rdbms].get(DbConfig.VERSION):
            if db_assert_access(engine=from_rdbms,
                                errors=errors,
                                logger=logger):
                session_registry[from_rdbms][DbConfig.VERSION] = db_get_param(key=DbParam.VERSION,
                                                                              engine=from_rdbms)
            else:
                # 101: {}
                errors.append(validate_format_error(101,
                                                    f"Source database '{from_rdbms}' "
                                                    "is not accessible as configured"))
        # verify target database runtime access
        if to_rdbms and not session_registry[to_rdbms].get(DbConfig.VERSION):
            if db_assert_access(engine=to_rdbms,
                                errors=errors,
                                logger=logger):
                session_registry[from_rdbms][DbConfig.VERSION] = db_get_param(key=DbParam.VERSION,
                                                                              engine=to_rdbms)
            else:
                # 101: {}
                errors.append(validate_format_error(101,
                                                    f"Target database '{from_rdbms}' "
                                                    "is not accessible as configured"))
        # verify target S3 runtime access
        if to_s3 and not session_registry[to_s3].get(S3Config.VERSION):
            if s3_startup(engine=to_s3,
                          errors=errors):
                session_registry[to_s3][S3Config.VERSION] = s3_get_param(key=S3Param.VERSION,
                                                                         engine=to_s3)
            else:
                # 101: {}
                errors.append(validate_format_error(101,
                                                    f"Target S3 '{to_s3}' "
                                                    "is not accessible as configured"))
    if not errors:
        # set the spots for the session
        session_registry[MigConfig.SPOTS] = {
            MigSpot.FROM_RDBMS: from_rdbms,
            MigSpot.TO_RDBMS: to_rdbms,
            MigSpot.TO_S3: to_s3
        }


def validate_steps(input_params: dict[str, str],
                   session_id: str,
                   errors: list[str]) -> None:

    # retrieve the migration steps
    step_metadata: bool = validate_bool(source=input_params,
                                        attr=MigStep.MIGRATE_METADATA,
                                        required=True,
                                        errors=errors)
    step_plaindata: bool = validate_bool(source=input_params,
                                         attr=MigStep.MIGRATE_PLAINDATA,
                                         required=True,
                                         errors=errors)
    step_lobdata: bool = validate_bool(source=input_params,
                                       attr=MigStep.MIGRATE_LOBDATA,
                                       required=True,
                                       errors=errors)
    step_sync_plain: bool = validate_bool(source=input_params,
                                          attr=MigStep.SYNCHRONIZE_PLAINDATA,
                                          errors=errors)
    step_sync_lobs: bool = validate_bool(source=input_params,
                                         attr=MigStep.SYNCHRONIZE_LOBDATA,
                                         errors=errors)
    # validate them
    err_msg: str | None = None
    if not (step_metadata or step_lobdata or
            step_plaindata or step_sync_plain or step_sync_lobs):
        err_msg = "At least one migration step must be specified"
    elif (step_sync_plain and
          (step_metadata or step_plaindata or step_lobdata or step_sync_lobs)):
        err_msg = "Plaindata synchronization can not be combined with another operation"
    else:
        target_s3: str = validate_str(source=input_params,
                                      attr=MigSpot.TO_S3,
                                      errors=errors)
        if step_metadata and step_lobdata and not step_plaindata and not target_s3:
            err_msg = "Migrating metadata and lobdata to a database requires migrating plaindata as well"
        elif step_sync_lobs:
            if step_metadata or step_plaindata or step_lobdata or step_sync_plain:
                err_msg = "LOB synchronization can not be combined with another operation"
            elif not target_s3:
                err_msg = "LOB synchronization requires S3 destination"

    if err_msg:
        # 101: {}
        errors.append(validate_format_error(101,
                                            err_msg))
    else:
        # set the steps for the session
        session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
        session_registry[MigConfig.STEPS] = {
            MigStep.MIGRATE_METADATA: step_metadata,
            MigStep.MIGRATE_PLAINDATA: step_plaindata,
            MigStep.MIGRATE_LOBDATA: step_lobdata,
            MigStep.SYNCHRONIZE_PLAINDATA: step_sync_plain,
            MigStep.SYNCHRONIZE_LOBDATA: step_sync_lobs
        }


def validate_specs(input_params: dict[str, str],
                   session_id: str,
                   errors: list[str]) -> None:

    # obtain the session registry
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)

    # obtain the optional migration badge
    migration_badge: str = input_params.get(MigSpec.MIGRATION_BADGE)

    # assert and retrieve the source schema
    from_schema: str = (validate_str(source=input_params,
                                     attr=MigSpec.FROM_SCHEMA,
                                     required=True,
                                     errors=errors) or "").lower()

    # assert and retrieve the target schema
    to_schema: str = (validate_str(source=input_params,
                                   attr=MigSpec.TO_SCHEMA,
                                   required=True,
                                   errors=errors) or "").lower()

    # assert and retrieve the override columns parameter
    override_columns: dict[str, Type] = \
        __assert_override_columns(input_params=input_params,
                                  rdbms=session_registry[MigConfig.SPOTS][MigSpot.TO_RDBMS],
                                  errors=errors)

    # assert and retrieve the incremental migrations parameter
    incr_migrations: dict[str, dict[MigIncremental, int]] = __assert_incr_migrations(
        input_params=input_params,
        def_size=session_registry[MigConfig.METRICS][MigMetric.INCREMENTAL_SIZE],
        errors=errors
    )
    process_indexes: bool = validate_bool(source=input_params,
                                          attr=MigSpec.PROCESS_INDEXES,
                                          errors=errors)
    process_views: bool = validate_bool(source=input_params,
                                        attr=MigSpec.PROCESS_VIEWS,
                                        errors=errors)
    relax_reflection: bool = validate_bool(source=input_params,
                                           attr=MigSpec.RELAX_REFLECTION,
                                           errors=errors)
    skip_nonempty: bool = validate_bool(source=input_params,
                                        attr=MigSpec.SKIP_NONEMPTY,
                                        errors=errors)
    reflect_filetype: bool = validate_bool(source=input_params,
                                           attr=MigSpec.REFLECT_FILETYPE,
                                           errors=errors)
    flatten_storage: bool = validate_bool(source=input_params,
                                          attr=MigSpec.FLATTEN_STORAGE,
                                          errors=errors)
    optimize_pks: bool = validate_bool(source=input_params,
                                       attr=MigSpec.OPTIMIZE_PKS,
                                       default=True,
                                       errors=errors)
    remove_ctrlschars: list[str] = [s.lower()
                                    for s in (validate_strs(source=input_params,
                                                            attr=MigSpec.REMOVE_CTRLCHARS,
                                                            errors=errors) or [])]
    include_relations: list[str] = [s.lower()
                                    for s in (validate_strs(source=input_params,
                                                            attr=MigSpec.INCLUDE_RELATIONS,
                                                            errors=errors) or [])]
    exclude_relations: list[str] = [s.lower()
                                    for s in (validate_strs(source=input_params,
                                                            attr=MigSpec.EXCLUDE_RELATIONS,
                                                            errors=errors) or [])]
    exclude_columns: list[str] = [s.lower()
                                  for s in (validate_strs(source=input_params,
                                                          attr=MigSpec.EXCLUDE_COLUMNS,
                                                          errors=errors) or [])]
    exclude_constraints: list[str] = [s.lower()
                                      for s in (validate_strs(source=input_params,
                                                              attr=MigSpec.EXCLUDE_CONSTRAINTS,
                                                              errors=errors) or [])]
    named_lobdata: list[str] = [s.lower()
                                for s in (validate_strs(source=input_params,
                                                        attr=MigSpec.NAMED_LOBDATA,
                                                        errors=errors) or [])]
    # validate the include and exclude relations lists
    if input_params.get(MigSpec.INCLUDE_RELATIONS) and \
            input_params.get(MigSpec.EXCLUDE_RELATIONS):
        # 127: Attributes {} cannot be assigned values at the same time
        errors.append(validate_format_error(127,
                                            f"'({MigSpec.INCLUDE_RELATIONS}, "
                                            f"{MigSpec.EXCLUDE_RELATIONS})'"))
    if errors:
        session_registry[MigConfig.SPECS] = {}
    else:
        # set the specs for the session
        session_registry[MigConfig.SPECS] = {
            MigSpec.EXCLUDE_COLUMNS: exclude_columns,
            MigSpec.EXCLUDE_CONSTRAINTS: exclude_constraints,
            MigSpec.EXCLUDE_RELATIONS: exclude_relations,
            MigSpec.FROM_SCHEMA: from_schema,
            MigSpec.FLATTEN_STORAGE: flatten_storage,
            MigSpec.INCLUDE_RELATIONS: include_relations,
            MigSpec.INCR_MIGRATIONS: incr_migrations,
            MigSpec.MIGRATION_BADGE: migration_badge,
            MigSpec.NAMED_LOBDATA: named_lobdata,
            MigSpec.OVERRIDE_COLUMNS: override_columns,
            MigSpec.OPTIMIZE_PKS: optimize_pks,
            MigSpec.PROCESS_INDEXES: process_indexes,
            MigSpec.PROCESS_VIEWS: process_views,
            MigSpec.REFLECT_FILETYPE: reflect_filetype,
            MigSpec.RELAX_REFLECTION: relax_reflection,
            MigSpec.REMOVE_CTRLCHARS: remove_ctrlschars,
            MigSpec.TO_SCHEMA: to_schema,
            MigSpec.SKIP_NONEMPTY: skip_nonempty
        }


def __assert_override_columns(input_params: dict[str, str],
                              rdbms: DbEngine,
                              errors: list[str]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the foreign columns list
    override_columns: list[str] = [s.lower()
                                   for s in (validate_strs(source=input_params,
                                                           attr=MigSpec.OVERRIDE_COLUMNS,
                                                           errors=errors) or [])]
    try:
        for override_column in override_columns:
            # format of 'override_column' is <table_name>.<column_name>=<column_type>
            column_name: str = override_column[:int(f"{override_column.rindex('=')}")].lower()
            type_name: str = override_column.replace(column_name, "", 1)[1:].lower()
            column_type: Type = name_to_type(type_name=type_name,
                                             rdbms=rdbms)
            if column_name and column_type:
                result[column_name] = column_type
            else:
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(142,
                                                    type_name,
                                                    f"not a valid column type for RDBMS {rdbms}"))
    except Exception as e:
        exc_err: str = str_sanitize(exc_format(exc=e,
                                               exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            f"@{MigSpec.OVERRIDE_COLUMNS}"))
    return result


def __assert_incr_migrations(input_params: dict[str, Any],
                             def_size: int,
                             errors: list[str]) -> dict[str, dict[MigIncremental, int]]:

    # initialize the return variable
    result: dict[str, dict[MigIncremental, int]] = {}

    # process the foreign columns list
    incremental_tables: list[str] = [s.lower()
                                     for s in (validate_strs(source=input_params,
                                                             attr=MigSpec.INCR_MIGRATIONS,
                                                             errors=errors) or [])]

    # format of 'incremental_tables' is [<table-name>[=<size>[:<offset>],...]
    for incremental_table in incremental_tables:
        if ":" not in incremental_table:
            incremental_table += ":"
        # noinspection PyTypeChecker
        terms: tuple[str, str, str] = str_splice(incremental_table,
                                                 seps=["=", ":"])
        size: int = int(terms[1]) if str_is_int(terms[1]) else def_size
        offset: int = int(terms[2]) if str_is_int(terms[2]) else 0
        result[terms[0]] = {
            MigIncremental.COUNT: size,
            MigIncremental.OFFSET: offset
        }

    return result
