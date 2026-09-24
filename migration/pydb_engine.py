import sys
from logging import Logger
from pypomes_core import str_sanitize, exc_format, validate_format_error
from pypomes_db import DbEngine, db_get_connection_string
from sqlalchemy import Engine, Result, TextClause, RootTransaction, create_engine, text


def build_engine(db_engine: DbEngine | str,
                 errors: list[str],
                 logger: Logger) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    # obtain the connection string
    conn_str: str = db_get_connection_string(engine=db_engine)

    # build the engine
    try:
        # 'echo' set to False prevent default stdout logging
        result = create_engine(url=conn_str)
        logger.debug(msg=f"RDBMS '{db_engine}', created migration engine")
    except Exception as e:
        exc_err = str_sanitize(exc_format(exc=e,
                                          exc_info=sys.exc_info()))
        logger.error(msg=exc_err)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102,
                                            exc_err))
    return result


def excecute_stmt(db_engine: DbEngine | str,
                  sa_engine: Engine,
                  stmt: str,
                  errors: list[str],
                  logger: Logger) -> Result:

    # initialize the return variable
    result: Result | None = None

    # execute the statement
    exc_stmt: TextClause = text(stmt)
    try:
        with sa_engine.connect() as conn:
            trans: RootTransaction = conn.begin()
            result = conn.execute(statement=exc_stmt)
            trans.commit()
            logger.debug(msg=f"RDBMS '{db_engine}', sucessfully executed '{stmt}'")
    except Exception as e:
        exc_err = str_sanitize(exc_format(exc=e,
                                          exc_info=sys.exc_info()))
        logger.error(msg=exc_err)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102,
                                            exc_err))
    return result
