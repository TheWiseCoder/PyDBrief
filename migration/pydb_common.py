from logging import Logger
from pypomes_core import (
    dict_jsonify,
    validate_bool, validate_int,
    validate_format_error, validate_str
)
from pypomes_db import (
    DbEngine, db_get_params, db_get_version, db_setup
)
from pypomes_s3 import (
    S3Engine, s3_get_params, s3_setup
)
from typing import Any, Final, cast

from app_constants import DbConfig, S3Config, MetricsConfig

MIGRATION_METRICS: Final[dict[MetricsConfig, Any]] = {
    MetricsConfig.BATCH_SIZE_IN: 1000000,
    MetricsConfig.BATCH_SIZE_OUT: 1000000,
    MetricsConfig.CHUNK_SIZE: 1048576,
    MetricsConfig.INCREMENTAL_SIZE: 100000
}


def get_migration_metrics() -> dict[str, int]:

    return dict_jsonify(source=MIGRATION_METRICS,
                        jsonify_keys=True,
                        jsonify_values=False)


def set_migration_metrics(errors: list[str],
                          scheme: dict[str, Any],
                          logger: Logger) -> None:

    # validate the optional 'batch-size-in' parameter
    batch_size_in: int = validate_int(errors=errors,
                                      scheme=scheme,
                                      attr=cast("str", MetricsConfig.BATCH_SIZE_IN.value),
                                      min_val=1000,
                                      max_val=10000000,
                                      logger=logger)
    # was it obtained ?
    if batch_size_in:
        # yes, set the corresponding migration parameter
        MIGRATION_METRICS[MetricsConfig.BATCH_SIZE_IN] = batch_size_in

    # validate the optional 'batch-size-out' parameter
    batch_size_out = validate_int(errors=errors,
                                  scheme=scheme,
                                  attr=cast("str", MetricsConfig.BATCH_SIZE_OUT.value),
                                  min_val=1000,
                                  max_val=10000000,
                                  logger=logger)
    # was it obtained ?
    if batch_size_out:
        # yes, set the corresponding migration parameter
        MIGRATION_METRICS[MetricsConfig.BATCH_SIZE_OUT] = batch_size_out

    # validate the optional 'chunk-size' parameter
    chunk_size: int = validate_int(errors=errors,
                                   scheme=scheme,
                                   attr=cast("str", MetricsConfig.CHUNK_SIZE.value),
                                   min_val=1024,
                                   max_val=16777216,
                                   logger=logger)
    # was it obtained ?
    if chunk_size:
        # yes, set the corresponding migration parameter
        MIGRATION_METRICS[MetricsConfig.CHUNK_SIZE] = chunk_size

    # validate the optional 'incremental-size' parameter
    incremental_size: int = validate_int(errors=errors,
                                         scheme=scheme,
                                         attr=cast("str", MetricsConfig.INCREMENTAL_SIZE.value),
                                         min_val=1000,
                                         max_val=10000000,
                                         logger=logger)
    # was it obtained ?
    if incremental_size:
        # yes, set the corresponding migration parameter
        MIGRATION_METRICS[MetricsConfig.INCREMENTAL_SIZE] = incremental_size


def get_rdbms_params(errors: list[str],
                     db_engine: DbEngine) -> dict[str, Any]:

    result: dict[str, Any] = db_get_params(engine=db_engine)
    if isinstance(result, dict):
        result["engine"] = db_engine.value
        result["version"] = db_get_version(engine=db_engine)
        dict_jsonify(source=result,
                     jsonify_keys=False,
                     jsonify_values=True)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            str(db_engine),
                                            "unknown or unconfigured RDBMS engine", "@rdbms"))
    return result


def set_rdbms_params(errors: list[str],
                     scheme: dict[str, Any]) -> None:

    db_engine: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr=cast("str", DbConfig.ENGINE.value),
                                  values=list(map(str, DbEngine)),
                                  required=True)
    db_name: str = validate_str(errors=errors,
                                scheme=scheme,
                                attr=cast("str", DbConfig.NAME.value),
                                required=True)
    db_host: str = validate_str(errors=errors,
                                scheme=scheme,
                                attr=cast("str", DbConfig.HOST.value),
                                required=True)
    db_port: int = validate_int(errors=errors,
                                scheme=scheme,
                                attr=cast("str", DbConfig.PORT.value),
                                min_val=1,
                                required=True)
    db_user: str = validate_str(errors=errors,
                                scheme=scheme,
                                attr=cast("str", DbConfig.USER.value),
                                required=True)
    db_pwd: str = validate_str(errors=errors,
                               scheme=scheme,
                               attr=cast("str", DbConfig.PWD.value),
                               required=True)
    db_client: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr=cast("str", DbConfig.CLIENT.value))
    db_driver: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr=cast("str", DbConfig.DRIVER.value))
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
        result["engine"] = s3_engine.value
        dict_jsonify(source=result,
                     jsonify_keys=False,
                     jsonify_values=True)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine.value,
                                            "unknown or unconfigured S3 engine", "@s3-engine"))
    return result


def set_s3_params(errors: list[str],
                  scheme: dict[str, Any]) -> None:

    engine: str = validate_str(errors=errors,
                               scheme=scheme,
                               attr=cast("str", S3Config.ENGINE.value),
                               values=list(map(str, S3Engine)),
                               required=True)
    endpoint_url: str = validate_str(errors=errors,
                                     scheme=scheme,
                                     attr=cast("str", S3Config.ENDPOINT_URL.value),
                                     required=True)
    bucket_name: str = validate_str(errors=errors,
                                    scheme=scheme,
                                    attr=cast("str", S3Config.BUCKET_NAME.value),
                                    required=True)
    access_key: str = validate_str(errors=errors,
                                   scheme=scheme,
                                   attr=cast("str", S3Config.ACCESS_KEY.value),
                                   required=True)
    secret_key: str = validate_str(errors=errors,
                                   scheme=scheme,
                                   attr=cast("str", S3Config.SECRET_KEY.value),
                                   required=True)
    region_name: str = validate_str(errors=errors,
                                    scheme=scheme,
                                    attr=cast("str", S3Config.REGION_NAME.value))
    secure_access: bool = validate_bool(errors=errors,
                                        scheme=scheme,
                                        attr=cast("str", S3Config.SECURE_ACCESS.value))
    if not errors and not s3_setup(engine=S3Engine(engine),
                                   endpoint_url=endpoint_url,
                                   bucket_name=bucket_name,
                                   access_key=access_key,
                                   secret_key=secret_key,
                                   region_name=region_name,
                                   secure_access=secure_access):
        # 145: Invalid, inconsistent, or missing arguments
        errors.append(validate_format_error(error_id=145))
