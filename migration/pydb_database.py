import sys
from logging import Logger
from pypomes_core import validate_format_error, str_sanitize, exc_format
from pypomes_db import (
    DbEngine, DbParam,
    db_get_param, db_get_type, db_get_view_ddl, db_execute, db_get_connection_string
)
from sqlalchemy import Engine, create_engine
from typing import Literal


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


def schema_create(schema: str,
                  db_engine: str,
                  errors: list[str],
                  logger: Logger) -> None:

    if db_get_type(engine=db_engine) == DbEngine.ORACLE:
        stmt: str = f"CREATE USER {schema} IDENTIFIED BY {schema}"
    else:
        user: str = db_get_param(key=DbParam.USER,
                                 engine=db_engine)
        stmt = f"CREATE SCHEMA {schema} AUTHORIZATION {user}"
    db_execute(exc_stmt=stmt,
               engine=db_engine,
               errors=errors)

    logger.debug(msg=f"RDBMS {db_engine}, created schema {schema}")


def column_set_nullable(db_type: DbEngine,
                        table: str,
                        column: str,
                        errors: list[str]) -> None:

    # build the statement
    alter_stmt: str | None = None
    match db_type:
        case DbEngine.MYSQL:
            pass
        case DbEngine.ORACLE:
            alter_stmt = (f"ALTER TABLE {table} "
                          f"MODIFY ({column} NULL)")
        case DbEngine.POSTGRES | DbEngine.SQLSERVER:
            alter_stmt = (f"ALTER TABLE {table} "
                          f"ALTER COLUMN {column} DROP NOT NULL")
    # execute it
    db_execute(exc_stmt=alter_stmt,
               engine=db_type,
               errors=errors)


def view_get_ddl(view_name: str,
                 view_type: Literal["M", "P"],
                 source_db: str,
                 source_schema: str,
                 target_schema: str,
                 errors: list[str],
                 logger: Logger) -> str:

    # obtain the DDL used to create the view
    result: str = db_get_view_ddl(view_type=view_type,
                                  view_name=f"{source_schema}.{view_name}",
                                  engine=source_db,
                                  errors=errors)
    if result:
        # DDL has been retrieved, modify it to point to the target schema
        result = result.lower().replace(f"{source_schema}.", f"{target_schema}.").replace('"', "")
        if view_type == "M":
            # for material views, reduce DDL to the bare minimum, as per the Oracle example below
            #   from:
            #     CREATE MATERIALIZED VIEW <source-schema>.<view>
            #       (<view-column-1>, ..., <view-column-n>)
            #       on prebuilt table without reduced precision using index
            #       refresh fast on demand start with sysdate+0 next trunc(sysdate +1) + 21/24
            #       with primary key using default local rollback segment
            #       using enforced constraints disable query rewrite
            #       AS SELECT <table-column-1>, ..., <table-column-n>
            #       FROM [[<source-schema>.<table>] | [<table>@<url>]]
            #   to:
            #     CREATE MATERIALIZED VIEW <target-schema>.<view>
            #       (<view-column-1>, ..., <view-column-n>)
            #       AS SELECT <table-column-1>, ..., <table-column-n>
            #       FROM <target-schema>.<table>
            pos1: int = result.index(")") + 1
            pos2: int = result.index("as select ")
            result = f"{result[:pos1]} {result[pos2:]}"
            pos2 = result.rfind("@")
            if pos2 > 0:
                result = result[:pos2]
                pos1 = result.rindex(" ") + 1
                if result.find(".", pos1) < 0:
                    result = f"{result[:pos1]} {target_schema}.{result[pos1:]}"
    else:
        # DDL has not been retrieved, report the problem
        err_msg: str = ("unable to retrieve DDL script "
                        f"for view {source_db}.{source_schema}.{view_name}")
        logger.error(msg=err_msg)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102,
                                            err_msg))
    return result


def table_embedded_nulls(db_engine: str,
                         table: str,
                         errors: list[str],
                         logger: Logger) -> None:

    # was a 'ValueError' exception on NULLs in strings raised ?
    # ("A string literal cannot contain NUL (0x00) characters.")
    if " contain NUL " in " ".join(errors):
        # yes, provide instructions on how to handle the problem
        err_msg: str = (f"Table {db_engine}.{table} has control characters embedded in string data, "
                        f"which are not accepted by the destination database. Please add this "
                        f"table to the 'remove-ctrlchars' migration parameter, and try again.")
        logger.error(msg=err_msg)
        # 101: {}
        errors.append(validate_format_error(101,
                                            err_msg))
