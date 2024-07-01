from logging import Logger
from pypomes_core import (
    str_sanitize, validate_int, validate_format_error, validate_str
)
from pypomes_db import db_get_params, db_setup

# migration parameters
MIGRATION_BATCH_SIZE: int = 1000000
MIGRATION_CHUNK_SIZE: int = 1048576
MIGRATION_MAX_PROCESSES: int = 1


def get_migration_params() -> dict:

    return {
        "batch-size": MIGRATION_BATCH_SIZE,
        "chunk-size": MIGRATION_CHUNK_SIZE,
        "max-processes": MIGRATION_MAX_PROCESSES
    }


def set_migration_params(errors: list[str],
                         scheme: dict,
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

    # validate the optional 'processes' parameter
    processes: int = validate_int(errors=errors,
                                  scheme=scheme,
                                  attr="max-processes",
                                  min_val=1,
                                  max_val=20,
                                  default=False,
                                  logger=logger)
    # was it obtained ?
    if processes:
        # yes, set the corresponding global parameter
        global MIGRATION_MAX_PROCESSES
        MIGRATION_MAX_PROCESSES = processes


def get_connection_params(errors: list[str],
                          rdbms: str) -> dict:

    result: dict = db_get_params(engine=rdbms)
    if isinstance(result, dict):
        result["rdbms"] = rdbms
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142, rdbms,
                                            "unknown or unconfigured RDBMS engine", "@rdbms"))

    return result


def set_connection_params(errors: list[str],
                          scheme: dict) -> None:

    db_engine: str = validate_str(errors=errors,
                                  scheme=scheme,
                                  attr="db-engine",
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
        errors.append(validate_format_error(120))


def db_build_query_msg(query_stmt: str,
                       bind_vals: tuple) -> str:
    """
    Format and return the message indicative of an empty search.

    :param query_stmt: the query command
    :param bind_vals: values associated with the query command
    :return: message indicative of empty search
    """
    result: str = str_sanitize(query_stmt)

    if bind_vals:
        for val in bind_vals:
            if isinstance(val, str):
                sval: str = f"'{val}'"
            else:
                sval: str = str(val)
            result = result.replace("?", sval, 1)

    return result


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


def db_log(errors: list[str],
           err_msg: str,
           logger: Logger,
           query_stmt: str,
           bind_vals: tuple = None) -> None:
    """
    Log *err_msg* and add it to *errors*, or else log the executed query, whichever is applicable.

    :param errors: incidental errors
    :param err_msg: the error message
    :param logger: the logger object
    :param query_stmt: the query statement
    :param bind_vals: optional bind values for the query statement
    """
    if err_msg:
        if logger:
            logger.error(err_msg)
        if isinstance(errors, list):
            errors.append(err_msg)
    elif logger:
        debug_msg: str = db_build_query_msg(query_stmt=query_stmt,
                                            bind_vals=bind_vals)
        logger.debug(msg=debug_msg)
