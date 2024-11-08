from logging import Logger
from pypomes_core import (
    APP_PREFIX, env_get_str,
    validate_bool, validate_int,
    validate_format_error, validate_str
)
from pypomes_db import (
    DbEngine, db_get_params, db_setup
)
from pypomes_s3 import (
    S3Engine, s3_get_params, s3_setup
)
from typing import Any, Final

REGISTRY_DOCKER: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_DOCKER")
REGISTRY_HOST: Final[str] = env_get_str(key=f"{APP_PREFIX}_REGISTRY_HOST")

# migration parameters
MIGRATION_BATCH_SIZE_IN: int = 1000000
MIGRATION_BATCH_SIZE_OUT: int = 100000
MIGRATION_CHUNK_SIZE: int = 1048576
MIGRATION_INCREMENTAL_SIZE: int = 100000


def get_migration_metrics() -> dict[str, Any]:

    return {
        "batch-size-in": MIGRATION_BATCH_SIZE_IN,
        "batch-size-out": MIGRATION_BATCH_SIZE_OUT,
        "chunk-size": MIGRATION_CHUNK_SIZE,
        "incremental-size": MIGRATION_INCREMENTAL_SIZE
    }


def set_migration_metrics(errors: list[str],
                          scheme: dict[str, Any],
                          logger: Logger) -> None:

    # validate the optional 'batch-size-in' parameter
    batch_size_in: int = validate_int(errors=errors,
                                      scheme=scheme,
                                      attr="batch-size-in",
                                      min_val=1000,
                                      max_val=10000000,
                                      default=False,
                                      logger=logger)
    # was it obtained ?
    if batch_size_in:
        # yes, set the corresponding global parameter
        global MIGRATION_BATCH_SIZE_IN
        MIGRATION_BATCH_SIZE_IN = batch_size_in

    # validate the optional 'batch-size-out' parameter
    batch_size_out = validate_int(errors=errors,
                                  scheme=scheme,
                                  attr="batch-size-out",
                                  min_val=1000,
                                  max_val=10000000,
                                  default=False,
                                  logger=logger)
    # was it obtained ?
    if batch_size_out:
        # yes, set the corresponding global parameter
        global MIGRATION_BATCH_SIZE_OUT
        MIGRATION_BATCH_SIZE_OUT = batch_size_out

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

    # validate the optional 'incremental-size' parameter
    incremental_size: int = validate_int(errors=errors,
                                         scheme=scheme,
                                         attr="incremental-size",
                                         min_val=1000,
                                         max_val=10000000,
                                         default=False,
                                         logger=logger)
    # was it obtained ?
    if incremental_size:
        # yes, set the corresponding global parameter
        global MIGRATION_INCREMENTAL_SIZE
        MIGRATION_INCREMENTAL_SIZE = incremental_size


def get_rdbms_params(errors: list[str],
                     rdbms: str) -> dict[str, Any]:

    db_engine: DbEngine = DbEngine(rdbms) \
                          if rdbms in DbEngine else None
    result: dict[str, Any] = db_get_params(engine=db_engine)
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
                                  values=list(map(str, DbEngine)),
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
                  s3_engine: str) -> dict[str, Any]:

    result: dict[str, Any] = s3_get_params(engine=S3Engine(s3_engine)) \
                             if s3_engine in S3Engine else None
    if result:
        result["engine"] = s3_engine
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine,
                                            "unknown or unconfigured S3 engine", "@s3-engine"))

    return result


def set_s3_params(errors: list[str],
                  scheme: dict[str, Any]) -> None:

    engine: str = validate_str(errors=errors,
                               scheme=scheme,
                               attr="s3-engine",
                               values=list(map(str, S3Engine)),
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
    if not errors and not s3_setup(engine=S3Engine(engine),
                                   endpoint_url=endpoint_url,
                                   bucket_name=bucket_name,
                                   access_key=access_key,
                                   secret_key=secret_key,
                                   region_name=region_name,
                                   secure_access=secure_access):
        # 145: Argumento(s) inválido(s), inconsistente(s) ou não fornecido(s)
        errors.append(validate_format_error(145))
