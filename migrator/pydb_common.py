from logging import Logger
from pypomes_core import (
    str_sanitize, validate_format_error, validate_str, validate_int
)
from typing import Final

# list of supported DB engines
SUPPORTED_ENGINES: Final[list[str]] = ["mysql", "oracle", "postgres", "sqlserver"]

# migration parameters
MIGRATION_BATCH_SIZE: int = 100000
MIGRATION_PROCESSES: int = 1


def get_migration_params() -> dict:

    return {
        "bach-size": MIGRATION_BATCH_SIZE,
        "processes": MIGRATION_PROCESSES
    }


def set_migration_parameters(errors: list[str],
                             scheme: dict) -> None:

    # validate the optional 'batch-size' parameter
    batch_size: int = validate_int(errors=errors,
                                   scheme=scheme,
                                   attr="batch-size",
                                   min_val=1000,
                                   max_val=200000,
                                   default=False)
    # was it obtained ?
    if batch_size:
        # yes, set the corresponding global parameter
        global MIGRATION_BATCH_SIZE
        MIGRATION_BATCH_SIZE = batch_size

    # validate the optional 'processes' parameter
    processes: int = validate_int(errors=errors,
                                  scheme=scheme,
                                  attr="processes",
                                  min_val=1,
                                  max_val=100,
                                  default=False)
    # was it obtained ?
    if processes:
        # yes, set the corresponding global parameter
        global MIGRATION_PROCESSES
        MIGRATION_PROCESSES = processes


def validate_rdbms(errors: list[str],
                   scheme: dict,
                   attr: str) -> str:

    # validate the provided value
    return validate_str(errors=errors,
                        scheme=scheme,
                        attr=attr,
                        default=SUPPORTED_ENGINES)


def validate_rdbms_dual(errors: list[str],
                        scheme: dict) -> tuple[str, str]:

    # retrieve the provided values
    source_rdbms: str = validate_rdbms(errors, scheme, "from-rdbms")
    target_rdbms: str = validate_rdbms(errors, scheme, "to-rdbms")

    if len(errors) == 0 and source_rdbms == target_rdbms:
        # 116: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(116, source_rdbms, "'from-rdbms' and 'to-rdbms'"))

    return source_rdbms, target_rdbms


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


def db_except_msg(exception: Exception,
                  db_name: str,
                  db_host: str) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :param db_name: the name of the database
    :param db_host: the database connection URL
    :return: the formatted error message
    """
    # 101: Error accessing the DB {} in {}: {}
    return validate_format_error(101, db_name, db_host, str_sanitize(f"{exception}"))


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
        debug_msg: str = db_build_query_msg(query_stmt, bind_vals)
        logger.debug(debug_msg)
