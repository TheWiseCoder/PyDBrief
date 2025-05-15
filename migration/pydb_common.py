from logging import Logger
from pypomes_core import (
    validate_bool, validate_int,
    validate_format_error, validate_str
)
from pypomes_db import (
    DbEngine, DbParam,
    db_get_params, db_get_version, db_setup
)
from pypomes_s3 import (
    S3Engine, S3Param,
    s3_get_params, s3_get_version, s3_setup
)
from typing import Any

from app_constants import (
    DbConfig, S3Config, MetricsConfig,
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE
)

MigrationMetrics: dict[MetricsConfig, int] = {
    MetricsConfig.BATCH_SIZE_IN: RANGE_BATCH_SIZE_IN[2],
    MetricsConfig.BATCH_SIZE_OUT: RANGE_BATCH_SIZE_OUT[2],
    MetricsConfig.CHUNK_SIZE: RANGE_CHUNK_SIZE[2],
    MetricsConfig.INCREMENTAL_SIZE: RANGE_INCREMENTAL_SIZE[2]
}

OngoingMigrations: list[str] = []


def set_migration_metrics(errors: list[str],
                          input_params: dict[str, Any],
                          logger: Logger) -> None:

    # validate the optional 'batch-size-in' parameter
    batch_size_in: int = validate_int(errors=errors,
                                      source=input_params,
                                      attr=MetricsConfig.BATCH_SIZE_IN,
                                      min_val=RANGE_BATCH_SIZE_IN[0],
                                      max_val=RANGE_BATCH_SIZE_IN[1],
                                      logger=logger)
    # was it obtained ?
    if batch_size_in:
        # yes, set the corresponding migration parameter
        MigrationMetrics[MetricsConfig.BATCH_SIZE_IN] = batch_size_in

    # validate the optional 'batch-size-out' parameter
    batch_size_out = validate_int(errors=errors,
                                  source=input_params,
                                  attr=MetricsConfig.BATCH_SIZE_OUT,
                                  min_val=RANGE_BATCH_SIZE_OUT[0],
                                  max_val=RANGE_BATCH_SIZE_OUT[1],
                                  logger=logger)
    # was it obtained ?
    if batch_size_out:
        # yes, set the corresponding migration parameter
        MigrationMetrics[MetricsConfig.BATCH_SIZE_OUT] = batch_size_out

    # validate the optional 'chunk-size' parameter
    chunk_size: int = validate_int(errors=errors,
                                   source=input_params,
                                   attr=MetricsConfig.CHUNK_SIZE,
                                   min_val=RANGE_CHUNK_SIZE[0],
                                   max_val=RANGE_CHUNK_SIZE[1],
                                   logger=logger)
    # was it obtained ?
    if chunk_size:
        # yes, set the corresponding migration parameter
        MigrationMetrics[MetricsConfig.CHUNK_SIZE] = chunk_size

    # validate the optional 'incremental-size' parameter
    incremental_size: int = validate_int(errors=errors,
                                         source=input_params,
                                         attr=MetricsConfig.INCREMENTAL_SIZE,
                                         min_val=RANGE_INCREMENTAL_SIZE[0],
                                         max_val=RANGE_INCREMENTAL_SIZE[1],
                                         logger=logger)
    # was it obtained ?
    if incremental_size:
        # yes, set the corresponding migration parameter
        MigrationMetrics[MetricsConfig.INCREMENTAL_SIZE] = incremental_size


def get_rdbms_params(errors: list[str],
                     db_engine: DbEngine) -> dict[str, Any]:

    result: dict[str, Any] = db_get_params(engine=db_engine)
    if isinstance(result, dict):
        result.pop(DbParam.PWD)
        result["version"] = db_get_version(engine=db_engine)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            db_engine,
                                            "unknown or unconfigured RDBMS engine",
                                            f"@{DbConfig.ENGINE}"))
    return result


def set_rdbms_params(errors: list[str],
                     input_params: dict[str, Any]) -> None:

    db_engine: str = validate_str(errors=errors,
                                  source=input_params,
                                  attr=DbConfig.ENGINE,
                                  values=list(map(str, DbEngine)),
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
    if not errors and not db_setup(engine=DbEngine(db_engine),
                                   db_name=db_name,
                                   db_host=db_host,
                                   db_port=db_port,
                                   db_user=db_user,
                                   db_pwd=db_pwd,
                                   db_client=db_client,
                                   db_driver=db_driver):
        # 145: Invalid, inconsistent, or missing arguments
        errors.append(validate_format_error(error_id=145))


def get_s3_params(errors: list[str],
                  s3_engine: S3Engine) -> dict[str, Any]:

    result: dict[str, Any] = s3_get_params(engine=s3_engine)
    if result:
        result.pop(S3Param.SECRET_KEY)
        result["version"] = s3_get_version(engine=s3_engine)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine,
                                            "unknown or unconfigured S3 engine",
                                            f"@{S3Config.ENGINE}"))
    return result


def set_s3_params(errors: list[str],
                  input_params: dict[str, Any]) -> None:

    engine: str = validate_str(errors=errors,
                               source=input_params,
                               attr=S3Config.ENGINE,
                               values=list(map(str, S3Engine)),
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
    if not errors and not s3_setup(engine=S3Engine(engine),
                                   endpoint_url=endpoint_url,
                                   bucket_name=bucket_name,
                                   access_key=access_key,
                                   secret_key=secret_key,
                                   region_name=region_name,
                                   secure_access=secure_access):
        # 145: Invalid, inconsistent, or missing arguments
        errors.append(validate_format_error(error_id=145))
