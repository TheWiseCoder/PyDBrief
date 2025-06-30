import sys
from enum import StrEnum
from pypomes_core import (
    str_sanitize, str_splice, str_is_int,
    exc_format, validate_int, validate_bool, validate_enum,
    validate_str, validate_strs, validate_format_error
)
from pypomes_db import (
    DbEngine, db_assert_access, db_setup,
    db_get_engines, db_get_version
)
from pypomes_http import HttpMethod
from pypomes_s3 import (
    S3Engine, s3_assert_access, s3_setup,
    s3_get_engines, s3_get_version
)
from sqlalchemy.sql.elements import Type
from typing import Any, Final

from app_constants import (
    DbConfig, S3Config, MigMetric,
    MigConfig, MigSpec, MigStep, MigSpot,
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE,
    RANGE_PLAINDATA_CHANNELS, RANGE_PLAINDATA_CHANNEL_SIZE,
    RANGE_LOBDATA_CHANNELS, RANGE_LOBDATA_CHANNEL_SIZE
)
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


def assert_expected_params(errors: list[str],
                           service: str,
                           method: str,
                           input_params: dict[str, str]) -> None:

    op: str = f"{service}:{method}"
    params: list[StrEnum] = SERVICE_PARAMS.get(op) or []
    params.append(MigSpec.CLIENT_ID)
    params.append(MigSpec.SESSION_ID)
    # 122 Attribute is unknown or invalid in this context
    errors.extend([validate_format_error(122,
                                         f"@{key}") for key in input_params if key not in params])


def validate_rdbms(errors: list[str],
                   input_params: dict[str, Any]) -> None:

    db_engine: DbEngine = validate_enum(errors=errors,
                                        source=input_params,
                                        attr=DbConfig.ENGINE,
                                        enum_class=DbEngine,
                                        required=True)
    db_name: str = validate_str(errors=errors,
                                source=input_params,
                                attr=DbConfig.NAME,
                                required=True)
    db_host: str = validate_str(errors=errors,
                                source=input_params,
                                attr=DbConfig.HOST,
                                required=True)
    db_port: int = validate_int(errors=errors,
                                source=input_params,
                                attr=DbConfig.PORT,
                                min_val=1,
                                required=True)
    db_user: str = validate_str(errors=errors,
                                source=input_params,
                                attr=DbConfig.USER,
                                required=True)
    db_pwd: str = validate_str(errors=errors,
                               source=input_params,
                               attr=DbConfig.PWD,
                               required=True)
    db_client: str = validate_str(errors=errors,
                                  source=input_params,
                                  attr=DbConfig.CLIENT)
    db_driver: str = validate_str(errors=errors,
                                  source=input_params,
                                  attr=DbConfig.DRIVER)
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
            db_specs: dict[DbConfig, Any] = {
                DbConfig.ENGINE: db_engine,
                DbConfig.NAME: db_name,
                DbConfig.HOST: db_host,
                DbConfig.PORT: db_port,
                DbConfig.USER: db_user,
                DbConfig.PWD: db_pwd,
                DbConfig.VERSION: db_get_version(engine=db_engine)
            }
            if db_client:
                db_specs[DbConfig.CLIENT] = db_client
            if db_driver:
                db_specs[DbConfig.DRIVER] = db_driver
            # retrieve the session id and save the DB specs
            session_id: str = input_params.get(MigSpec.SESSION_ID)
            session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
            session_registry[db_engine] = db_specs
        else:
            # 145: Invalid, inconsistent, or missing arguments
            errors.append(validate_format_error(error_id=145))


def validate_s3(errors: list[str],
                input_params: dict[str, Any]) -> None:

    engine: S3Engine = validate_enum(errors=errors,
                                     source=input_params,
                                     attr=S3Config.ENGINE,
                                     enum_class=S3Engine,
                                     required=True)
    endpoint_url: str = validate_str(errors=errors,
                                     source=input_params,
                                     attr=S3Config.ENDPOINT_URL,
                                     required=True)
    bucket_name: str = validate_str(errors=errors,
                                    source=input_params,
                                    attr=S3Config.BUCKET_NAME,
                                    required=True)
    access_key: str = validate_str(errors=errors,
                                   source=input_params,
                                   attr=S3Config.ACCESS_KEY,
                                   required=True)
    secret_key: str = validate_str(errors=errors,
                                   source=input_params,
                                   attr=S3Config.SECRET_KEY,
                                   required=True)
    region_name: str = validate_str(errors=errors,
                                    source=input_params,
                                    attr=S3Config.REGION_NAME)
    secure_access: bool = validate_bool(errors=errors,
                                        source=input_params,
                                        attr=S3Config.SECURE_ACCESS)
    if not errors:
        if s3_setup(engine=engine,
                    endpoint_url=endpoint_url,
                    bucket_name=bucket_name,
                    access_key=access_key,
                    secret_key=secret_key,
                    region_name=region_name,
                    secure_access=secure_access):
            # add S3 specs to registry
            s3_specs: dict[S3Config, Any] = {
                S3Config.ENGINE: engine,
                S3Config.ENDPOINT_URL: endpoint_url,
                S3Config.BUCKET_NAME: bucket_name,
                S3Config.ACCESS_KEY: access_key,
                S3Config.SECRET_KEY: secret_key,
                S3Config.SECURE_ACCESS: secure_access,
                S3Config.VERSION: s3_get_version(engine=engine)
            }
            if region_name:
                s3_specs[S3Config.REGION_NAME] = region_name
            # retrieve the session id and save the S3 specs
            session_id: str = input_params.get(MigSpec.SESSION_ID)
            session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
            session_registry[engine] = s3_specs
        else:
            # 145: Invalid, inconsistent, or missing arguments
            errors.append(validate_format_error(error_id=145))


def validate_metrics(errors: list[str],
                     input_params: dict[str, Any]) -> None:

    # validate 'batch-size-in'
    batch_size_in: int = validate_int(errors=errors,
                                      source=input_params,
                                      attr=MigMetric.BATCH_SIZE_IN,
                                      min_val=RANGE_BATCH_SIZE_IN[0],
                                      max_val=RANGE_BATCH_SIZE_IN[1])
    # validate 'batch-size-out'
    batch_size_out = validate_int(errors=errors,
                                  source=input_params,
                                  attr=MigMetric.BATCH_SIZE_OUT,
                                  min_val=RANGE_BATCH_SIZE_OUT[0],
                                  max_val=RANGE_BATCH_SIZE_OUT[1])
    # validate 'chunk-size'
    chunk_size: int = validate_int(errors=errors,
                                   source=input_params,
                                   attr=MigMetric.CHUNK_SIZE,
                                   min_val=RANGE_CHUNK_SIZE[0],
                                   max_val=RANGE_CHUNK_SIZE[1])
    # validate 'incremental-size'
    incremental_size: int = validate_int(errors=errors,
                                         source=input_params,
                                         attr=MigMetric.INCREMENTAL_SIZE,
                                         min_val=RANGE_INCREMENTAL_SIZE[0],
                                         max_val=RANGE_INCREMENTAL_SIZE[1])
    # validate 'lobdata-channels'
    lobdata_channels: int = validate_int(errors=errors,
                                         source=input_params,
                                         attr=MigMetric.LOBDATA_CHANNELS,
                                         min_val=RANGE_LOBDATA_CHANNELS[0],
                                         max_val=RANGE_LOBDATA_CHANNELS[1])
    # validate 'lobdata-channel-size'
    lobdata_channel_size: int = validate_int(errors=errors,
                                             source=input_params,
                                             attr=MigMetric.LOBDATA_CHANNEL_SIZE,
                                             min_val=RANGE_LOBDATA_CHANNEL_SIZE[0],
                                             max_val=RANGE_LOBDATA_CHANNEL_SIZE[1])
    # validate 'plaindata-channels'
    plaindata_channels: int = validate_int(errors=errors,
                                           source=input_params,
                                           attr=MigMetric.PLAINDATA_CHANNELS,
                                           min_val=RANGE_PLAINDATA_CHANNELS[0],
                                           max_val=RANGE_PLAINDATA_CHANNELS[1])
    # validate 'plaindata-channel-size'
    plaindata_channel_size: int = validate_int(errors=errors,
                                               source=input_params,
                                               attr=MigMetric.PLAINDATA_CHANNEL_SIZE,
                                               min_val=RANGE_PLAINDATA_CHANNEL_SIZE[0],
                                               max_val=RANGE_PLAINDATA_CHANNEL_SIZE[1])
    if not errors:
        # set the metrics for the session
        session_id: str = input_params.get(MigSpec.SESSION_ID)
        session_metrics: dict[MigMetric, int] = get_session_registry(session_id=session_id)[MigConfig.METRICS]
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


def validate_spots(errors: list[str],
                   input_params: dict[str, str]) -> None:

    # validate source RDBMS
    from_rdbms: DbEngine = validate_enum(errors=errors,
                                         source=input_params,
                                         attr=MigSpot.FROM_RDBMS,
                                         enum_class=DbEngine,
                                         required=True)
    if not errors and from_rdbms not in db_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            from_rdbms,
                                            "unknown or unconfigured DB engine",
                                            f"@{MigSpot.FROM_RDBMS}"))
    # validate target RDBMS
    to_rdbms: DbEngine = validate_enum(errors=errors,
                                       source=input_params,
                                       attr=MigSpot.TO_RDBMS,
                                       enum_class=DbEngine,
                                       required=True)
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
    to_s3: S3Engine = validate_enum(errors=errors,
                                    source=input_params,
                                    attr=MigSpot.TO_S3,
                                    enum_class=S3Engine)
    if to_s3 and to_s3 not in s3_get_engines():
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            to_s3,
                                            "unknown or unconfigured S3 engine",
                                            f"@{MigSpot.TO_S3}"))
    if not errors:
        # verify  database runtime capabilities
        if from_rdbms and not db_assert_access(errors=errors,
                                               engine=from_rdbms):
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Source database '{from_rdbms}' "
                                                "is not accessible as configured"))
        if to_rdbms and not db_assert_access(errors=errors,
                                             engine=to_rdbms):
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Target database '{to_rdbms}' "
                                                "is not accessible as configured"))
        if to_s3 and not s3_assert_access(errors=errors,
                                          engine=to_s3):
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Target S3 '{to_s3}' "
                                                "is not accessible as configured"))
    if not errors:
        # set the spots for the session
        session_id: str = input_params.get(MigSpec.SESSION_ID)
        session_spots: dict[MigSpot, Any] = get_session_registry(session_id=session_id)[MigConfig.SPOTS]
        session_spots[MigSpot.FROM_RDBMS] = from_rdbms
        session_spots[MigSpot.TO_RDBMS] = to_rdbms
        session_spots[MigSpot.TO_S3] = to_s3


def validate_steps(errors: list[str],
                   input_params: dict[str, str]) -> None:

    # retrieve the migration steps
    step_metadata: bool = validate_bool(errors=errors,
                                        source=input_params,
                                        attr=MigStep.MIGRATE_METADATA,
                                        required=True)
    step_plaindata: bool = validate_bool(errors=errors,
                                         source=input_params,
                                         attr=MigStep.MIGRATE_PLAINDATA,
                                         required=True)
    step_lobdata: bool = validate_bool(errors=errors,
                                       source=input_params,
                                       attr=MigStep.MIGRATE_LOBDATA,
                                       required=True)
    step_synchronize: bool = validate_bool(errors=errors,
                                           source=input_params,
                                           attr=MigStep.SYNCHRONIZE_PLAINDATA,
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
                                      attr=MigSpot.TO_S3,
                                      required=False)
        if not target_s3:
            err_msg = "Migrating metadata and lobdata to a database requires migrating plaindata as well"

    if err_msg:
        # 101: {}
        errors.append(validate_format_error(101, err_msg))
    else:
        # set the steps for the session
        session_id: str = input_params.get(MigSpec.SESSION_ID)
        session_steps: dict[MigStep, bool] = get_session_registry(session_id=session_id)[MigConfig.STEPS]
        session_steps[MigStep.MIGRATE_METADATA] = step_metadata
        session_steps[MigStep.MIGRATE_PLAINDATA] = step_plaindata
        session_steps[MigStep.MIGRATE_LOBDATA] = step_lobdata
        session_steps[MigStep.SYNCHRONIZE_PLAINDATA] = step_synchronize


def validate_specs(errors: list[str],
                   input_params: dict[str, str]) -> None:

    # obtain the session registry
    session_id: str = input_params.get(MigSpec.SESSION_ID)
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)

    # obtain the optional migration badge
    migration_badge: str = input_params.get(MigSpec.MIGRATION_BADGE)

    # assert and retrieve the source schema
    from_schema: str = (validate_str(errors=errors,
                                     source=input_params,
                                     attr=MigSpec.FROM_SCHEMA,
                                     required=True) or "").lower()

    # assert and retrieve the target schema
    to_schema: str = (validate_str(errors=errors,
                                   source=input_params,
                                   attr=MigSpec.TO_SCHEMA,
                                   required=True) or "").lower()

    # assert and retrieve the override columns parameter
    override_columns: dict[str, Type] = \
        __assert_override_columns(errors=errors,
                                  input_params=input_params,
                                  rdbms=session_registry[MigConfig.SPOTS][MigSpot.TO_RDBMS])
    # assert and retrieve the incremental migrations parameter
    incremental_migrations: dict[str, tuple[int, int]] = \
        __assert_incremental_migrations(errors=errors,
                                        input_params=input_params,
                                        def_size=session_registry[MigConfig.METRICS][MigMetric.INCREMENTAL_SIZE])
    process_indexes: bool = validate_bool(errors=None,
                                          source=input_params,
                                          attr=MigSpec.PROCESS_INDEXES)
    process_views: bool = validate_bool(errors=None,
                                        source=input_params,
                                        attr=MigSpec.PROCESS_VIEWS)
    relax_reflection: bool = validate_bool(errors=None,
                                           source=input_params,
                                           attr=MigSpec.RELAX_REFLECTION)
    skip_nonempty: bool = validate_bool(errors=None,
                                        source=input_params,
                                        attr=MigSpec.SKIP_NONEMPTY)
    reflect_filetype: bool = validate_bool(errors=None,
                                           source=input_params,
                                           attr=MigSpec.REFLECT_FILETYPE)
    flatten_storage: bool = validate_bool(errors=None,
                                          source=input_params,
                                          attr=MigSpec.FLATTEN_STORAGE)
    remove_nulls: list[str] = [s.lower()
                               for s in validate_strs(errors=None,
                                                      source=input_params,
                                                      attr=MigSpec.REMOVE_NULLS)]
    include_relations: list[str] = [s.lower()
                                    for s in validate_strs(errors=None,
                                                           source=input_params,
                                                           attr=MigSpec.INCLUDE_RELATIONS)]
    exclude_relations: list[str] = [s.lower()
                                    for s in validate_strs(errors=None,
                                                           source=input_params,
                                                           attr=MigSpec.EXCLUDE_RELATIONS)]
    exclude_columns: list[str] = [s.lower()
                                  for s in validate_strs(errors=None,
                                                         source=input_params,
                                                         attr=MigSpec.EXCLUDE_COLUMNS)]
    exclude_constraints: list[str] = [s.lower()
                                      for s in validate_strs(errors=None,
                                                             source=input_params,
                                                             attr=MigSpec.EXCLUDE_CONSTRAINTS)]
    named_lobdata: list[str] = [s.lower()
                                for s in validate_strs(errors=None,
                                                       source=input_params,
                                                       attr=MigSpec.NAMED_LOBDATA)]
    # validate the include and exclude relations lists
    if input_params.get(MigSpec.INCLUDE_RELATIONS) and \
            input_params.get(MigSpec.EXCLUDE_RELATIONS):
        # 151: Attributes {} cannot be assigned values at the same time
        errors.append(validate_format_error(151,
                                            f"'({MigSpec.INCLUDE_RELATIONS}, "
                                            f"{MigSpec.EXCLUDE_RELATIONS})'"))
    if errors:
        session_registry[MigConfig.SPECS] = {}
    else:
        # set the specs for the session
        session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
        session_specs[MigSpec.EXCLUDE_COLUMNS] = exclude_columns
        session_specs[MigSpec.EXCLUDE_CONSTRAINTS] = exclude_constraints
        session_specs[MigSpec.EXCLUDE_RELATIONS] = exclude_relations
        session_specs[MigSpec.FROM_SCHEMA] = from_schema
        session_specs[MigSpec.FLATTEN_STORAGE] = flatten_storage
        session_specs[MigSpec.INCLUDE_RELATIONS] = include_relations
        session_specs[MigSpec.INCREMENTAL_MIGRATIONS] = incremental_migrations
        session_specs[MigSpec.MIGRATION_BADGE] = migration_badge
        session_specs[MigSpec.NAMED_LOBDATA] = named_lobdata
        session_specs[MigSpec.OVERRIDE_COLUMNS] = override_columns
        session_specs[MigSpec.PROCESS_INDEXES] = process_indexes
        session_specs[MigSpec.PROCESS_VIEWS] = process_views
        session_specs[MigSpec.REFLECT_FILETYPE] = reflect_filetype
        session_specs[MigSpec.RELAX_REFLECTION] = relax_reflection
        session_specs[MigSpec.REMOVE_NULLS] = remove_nulls
        session_specs[MigSpec.TO_SCHEMA] = to_schema
        session_specs[MigSpec.SKIP_NONEMPTY] = skip_nonempty


def __assert_override_columns(errors: list[str],
                              input_params: dict[str, str],
                              rdbms: DbEngine) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the foreign columns list
    override_columns: list[str] = [s.lower()
                                   for s in validate_strs(errors=errors,
                                                          source=input_params,
                                                          attr=MigSpec.OVERRIDE_COLUMNS) or []]
    try:
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
                                            f"@{MigSpec.OVERRIDE_COLUMNS}"))
    return result


def __assert_incremental_migrations(errors: list[str],
                                    input_params: dict[str, Any],
                                    def_size: int) -> dict[str, tuple[int, int]]:

    # initialize the return variable
    result: dict[str, tuple[int, int]] = {}

    # process the foreign columns list
    incremental_tables: list[str] = [s.lower()
                                     for s in validate_strs(errors=errors,
                                                            source=input_params,
                                                            attr=MigSpec.INCREMENTAL_MIGRATIONS) or []]

    # format of 'incremental_tables' is [<table-name>[=<size>[:<offset>],...]
    for incremental_table in incremental_tables:
        # noinspection PyTypeChecker
        terms: tuple[str, str, str] = str_splice(source=incremental_table,
                                                 seps=["=", ":"])
        if str_is_int(source=terms[1]):
            size: int = int(terms[1])
        else:
            size: int = def_size
        offset: int = int(terms[2]) if str_is_int(source=terms[2]) else 0
        result[terms[0]] = (size, offset)

    return result
