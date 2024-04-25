from logging import Logger
from pypomes_core import str_sanitize


def _db_except_msg(exception: Exception, db_name: str, db_host: str) -> str:
    """
    Format and return the error message corresponding to the exception raised while accessing the database.

    :param exception: the exception raised
    :return: the formatted error message
    """
    return f"Error accessing '{db_name}' at '{db_host}': {str_sanitize(f'{exception}')}"


def _db_build_query_msg(query_stmt: str,
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


def _db_log(errors: list[str],
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
        debug_msg: str = _db_build_query_msg(query_stmt, bind_vals)
        logger.debug(debug_msg)

