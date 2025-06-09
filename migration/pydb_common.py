from enum import StrEnum
from logging import Logger
from pypomes_core import (
    validate_bool, validate_int,
    validate_str, validate_enum,
    validate_format_error, dict_jsonify
)
from pypomes_db import (
    DbEngine, db_get_version, db_setup
)
from pypomes_s3 import (
    S3Engine, s3_get_version, s3_setup
)
from typing import Any

from app_constants import (
    DbConfig, S3Config, MetricsConfig, MigrationConfig, MigrationState,
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE,
    RANGE_LOBDATA_CHANNELS, RANGE_PLAINDATA_CHANNELS
)
from migration.pydb_sessions import get_session_registry


def get_metrics_params(session_id: str) -> dict[MetricsConfig, int]:

    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    return session_registry.get(MigrationConfig.METRICS)


def set_metrics_params(errors: list[str],
                       input_params: dict[str, Any]) -> None:

    # validate 'batch-size-in'
    batch_size_in: int = validate_int(errors=errors,
                                      source=input_params,
                                      attr=MetricsConfig.BATCH_SIZE_IN,
                                      min_val=RANGE_BATCH_SIZE_IN[0],
                                      max_val=RANGE_BATCH_SIZE_IN[1])
    # validate 'batch-size-out'
    batch_size_out = validate_int(errors=errors,
                                  source=input_params,
                                  attr=MetricsConfig.BATCH_SIZE_OUT,
                                  min_val=RANGE_BATCH_SIZE_OUT[0],
                                  max_val=RANGE_BATCH_SIZE_OUT[1])
    # validate 'chunk-size'
    chunk_size: int = validate_int(errors=errors,
                                   source=input_params,
                                   attr=MetricsConfig.CHUNK_SIZE,
                                   min_val=RANGE_CHUNK_SIZE[0],
                                   max_val=RANGE_CHUNK_SIZE[1])
    # validate 'incremental-size'
    incremental_size: int = validate_int(errors=errors,
                                         source=input_params,
                                         attr=MetricsConfig.INCREMENTAL_SIZE,
                                         min_val=RANGE_INCREMENTAL_SIZE[0],
                                         max_val=RANGE_INCREMENTAL_SIZE[1])
    # validate 'lobdata-channels'
    lobdata_channels: int = validate_int(errors=errors,
                                         source=input_params,
                                         attr=MetricsConfig.LOBDATA_CHANNELS,
                                         min_val=RANGE_LOBDATA_CHANNELS[0],
                                         max_val=RANGE_LOBDATA_CHANNELS[1])
    # validate 'plaindata-channels'
    plaindata_channels: int = validate_int(errors=errors,
                                           source=input_params,
                                           attr=MetricsConfig.LOBDATA_CHANNELS,
                                           min_val=RANGE_PLAINDATA_CHANNELS[0],
                                           max_val=RANGE_PLAINDATA_CHANNELS[1])
    if not errors:
        # retrieve the session id
        session_id: str = input_params.get(MigrationConfig.SESSION_ID)
        # retrieve the metrics for the session
        session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
        metrics: dict[MetricsConfig, int] = session_registry.get(MigrationConfig.METRICS)
        # set the parameters
        if batch_size_in:
            metrics[MetricsConfig.BATCH_SIZE_IN] = batch_size_in
        if batch_size_out:
            metrics[MetricsConfig.BATCH_SIZE_OUT] = batch_size_out
        if chunk_size:
            metrics[MetricsConfig.CHUNK_SIZE] = chunk_size
        if incremental_size:
            metrics[MetricsConfig.INCREMENTAL_SIZE] = incremental_size
        if lobdata_channels:
            metrics[MetricsConfig.LOBDATA_CHANNELS] = lobdata_channels
        if plaindata_channels:
            metrics[MetricsConfig.PLAINDATA_CHANNELS] = plaindata_channels


def get_rdbms_params(errors: list[str],
                     session_id: str,
                     db_engine: DbEngine) -> dict[DbConfig, Any] | None:

    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    result: dict[DbConfig, Any] = session_registry.get(db_engine)
    if isinstance(result, dict):
        result.pop(DbConfig.PWD)
        dict_jsonify(source=result)
        result[DbConfig.VERSION] = db_get_version(engine=db_engine)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            db_engine,
                                            "unknown or unconfigured RDBMS engine",
                                            f"@{DbConfig.ENGINE}"))
    return result


def set_rdbms_params(errors: list[str],
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
            session_id: str = input_params.get(MigrationConfig.SESSION_ID)
            session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
            session_registry[db_engine] = db_specs
        else:
            # 145: Invalid, inconsistent, or missing arguments
            errors.append(validate_format_error(error_id=145))


def get_s3_params(errors: list[str],
                  session_id: str,
                  s3_engine: S3Engine) -> dict[str, Any] | None:

    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    result: dict[S3Config, Any] = session_registry.get(s3_engine)
    if isinstance(result, dict):
        result.pop(S3Config.SECRET_KEY)
        result[S3Config.VERSION] = s3_get_version(engine=s3_engine)
        dict_jsonify(source=result)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine,
                                            "unknown or unconfigured S3 engine",
                                            f"@{S3Config.ENGINE}"))
    return result


def set_s3_params(errors: list[str],
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
            session_id: str = input_params.get(MigrationConfig.SESSION_ID)
            session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
            session_registry[engine] = s3_specs
        else:
            # 145: Invalid, inconsistent, or missing arguments
            errors.append(validate_format_error(error_id=145))


def assert_abort_state(errors: list[str],
                       session_id: str,
                       logger: Logger) -> bool:

    # initialize the return variable
    result: bool = False

    # verify whether current migration is marked for abortion
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    if session_registry.get(MigrationConfig.STATE) == MigrationState.ABORTING:
        err_msg: str = f"Migration in session '{session_id}' aborted on request"
        logger.error(msg=err_msg)
        # 101: {}
        errors.append(validate_format_error(101,
                                            err_msg))
        result = True

    return result
