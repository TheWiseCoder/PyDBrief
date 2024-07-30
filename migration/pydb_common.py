from logging import Logger
from pypomes_core import (
    validate_bool, validate_int,
    validate_format_error, validate_str
)
from pypomes_db import db_get_params, db_setup
from pypomes_s3 import s3_get_params, s3_setup
from typing import Any

# migration parameters
MIGRATION_BATCH_SIZE: int = 1000000
MIGRATION_CHUNK_SIZE: int = 1048576


def get_migration_metrics() -> dict[str, Any]:

    return {
        "batch-size": MIGRATION_BATCH_SIZE,
        "chunk-size": MIGRATION_CHUNK_SIZE
    }


def set_migration_metrics(errors: list[str],
                          scheme: dict[str, Any],
                          logger: Logger) -> None:

    # validate the optional 'batch-size' parameter
    batch_size: int = validate_int(errors=errors,
                                   scheme=scheme,
                                   attr="batch-size",
                                   min_val=1000,
                                   max_val=10000000,
                                   default=False,
                                   logger=logger)
    # was it obtained ?
    if batch_size:
        # yes, set the corresponding global parameter
        global MIGRATION_BATCH_SIZE
        MIGRATION_BATCH_SIZE = batch_size

    # validate the optional 'chunk-size' parameter
    chunk_size: int = validate_int(errors=errors,
                                   scheme=scheme,
                                   attr="chunk-size",
                                   min_val=1024,
                                   max_val=16777216,
                                   default=False,
                                   logger=logger)
    # was it obtained ?
    if chunk_size:
        # yes, set the corresponding global parameter
        global MIGRATION_CHUNK_SIZE
        MIGRATION_CHUNK_SIZE = chunk_size


def get_rdbms_params(errors: list[str],
                     rdbms: str) -> dict[str, Any]:

    result: dict[str, Any] = db_get_params(engine=rdbms)
    if isinstance(result, dict):
        result["rdbms"] = rdbms
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142, rdbms,
                                            "unknown or unconfigured RDBMS engine", "@rdbms"))

    return result


def set_rdbms_params(errors: list[str],
                     scheme: dict[str, Any]) -> None:

    db_engine: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr="db-engine",
                                  values=["mysql", "oracle", "postgres", "sqlserver"],
                                  required=True)
    db_name: str = validate_str(errors=errors,
                                scheme=scheme,
                                attr="db-name",
                                required=True)
    db_host: str = validate_str(errors=errors,
                                scheme=scheme,
                                attr="db-host",
                                required=True)
    db_port: int = validate_int(errors=errors,
                                scheme=scheme,
                                attr="db-port",
                                min_val=1,
                                required=True)
    db_user: str = validate_str(errors=errors,
                                scheme=scheme,
                                attr="db-user",
                                required=True)
    db_pwd: str = validate_str(errors=errors,
                               scheme=scheme,
                               attr="db-pwd",
                               required=True)
    db_client: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr="db-client")
    db_driver: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr="db-driver")
    # noinspection PyTypeChecker
    if not errors and not db_setup(engine=db_engine,
                                   db_name=db_name,
                                   db_host=db_host,
                                   db_port=db_port,
                                   db_user=db_user,
                                   db_pwd=db_pwd,
                                   db_client=db_client,
                                   db_driver=db_driver):
        # 145: Argumento(s) inválido(s), inconsistente(s) ou não fornecido(s)
        errors.append(validate_format_error(145))


def get_s3_params(errors: list[str],
                  s3_engine: str) -> dict[str, Any]:

    result: dict[str, Any] = s3_get_params(engine=s3_engine)
    if isinstance(result, dict):
        result["engine"] = s3_engine
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142, s3_engine,
                                            "unknown or unconfigured S3 engine", "@s3-engine"))

    return result


def set_s3_params(errors: list[str],
                  scheme: dict[str, Any]) -> None:

    engine: str = validate_str(errors=errors,
                               scheme=scheme,
                               attr="s3-engine",
                               values=["aws", "ecs", "minio"],
                               required=True)
    endpoint_url: str = validate_str(errors=errors,
                                     scheme=scheme,
                                     attr="s3-endpoint-url",
                                     required=True)
    bucket_name: str = validate_str(errors=errors,
                                    scheme=scheme,
                                    attr="s3-bucket-name",
                                    required=True)
    access_key: str = validate_str(errors=errors,
                                   scheme=scheme,
                                   attr="s3-access-key",
                                   required=True)
    secret_key: str = validate_str(errors=errors,
                                   scheme=scheme,
                                   attr="s3-secret-key",
                                   required=True)
    region_name: str = validate_str(errors=errors,
                                    scheme=scheme,
                                    attr="s3-region-name")
    secure_access: bool = validate_bool(errors=errors,
                                        scheme=scheme,
                                        attr="s3-secure-access")
    # noinspection PyTypeChecker
    if not errors and not s3_setup(engine=engine,
                                   endpoint_url=endpoint_url,
                                   bucket_name=bucket_name,
                                   access_key=access_key,
                                   secret_key=secret_key,
                                   region_name=region_name,
                                   secure_access=secure_access):
        # 145: Argumento(s) inválido(s), inconsistente(s) ou não fornecido(s)
        errors.append(validate_format_error(145))


def log(logger: Logger,
        level: int,
        msg: str) -> None:

    if logger:
        match level:
            case 10:    # DEBUG
                logger.debug(msg)
            case 20:    # INFO
                logger.info(msg)
            case 30:    # WARNING
                logger.warning(msg)
            case 40:    # ERROR
                logger.error(msg)
            case 50:    # CRITICAL
                logger.critical(msg)
