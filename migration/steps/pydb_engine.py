import sys
from logging import Logger
from pypomes_core import str_sanitize, exc_format, validate_format_error
from pypomes_db import db_get_connection_string
from sqlalchemy import Engine, create_engine, Result, TextClause, text, RootTransaction


def build_engine(errors: list[str],
                 rdbms: str,
                 logger: Logger) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    # obtain the connection string
    conn_str: str = db_get_connection_string(engine=rdbms)

    # build the engine
    try:
        result = create_engine(url=conn_str)
        logger.debug(msg=f"RDBMS {rdbms}, created migration engine")
    except Exception as e:
        exc_err = str_sanitize(exc_format(exc=e,
                               exc_info=sys.exc_info()))
        logger.error(msg=exc_err)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102, exc_err))

    return result


def excecute_stmt(errors: list[str],
                  rdbms: str,
                  engine: Engine,
                  stmt: str,
                  logger: Logger) -> Result:

    result: Result | None = None
    exc_stmt: TextClause = text(stmt)
    try:
        with engine.connect() as conn:
            trans: RootTransaction = conn.begin()
            result = conn.execute(statement=exc_stmt)
            trans.commit()
            logger.debug(msg=f"RDBMS {rdbms}, sucessfully executed {stmt}")
    except Exception as e:
        exc_err = str_sanitize(exc_format(exc=e,
                                          exc_info=sys.exc_info()))
        logger.error(msg=exc_err)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102, exc_err))

    return result
