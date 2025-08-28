from logging import Logger
from pypomes_core import validate_format_error
from pypomes_db import (
    DbEngine, DbParam, DbConnectionPool, DbPoolEvent,
    db_get_pool, db_get_param, db_get_view_ddl, db_execute
)
from typing import Literal


def db_pool_setup(rdbms: DbEngine,
                  errors: list[str],
                  logger: Logger) -> None:

    pool: DbConnectionPool = db_get_pool(engine=rdbms)
    if not pool:
        pool = DbConnectionPool(engine=rdbms,
                                errors=errors,
                                logger=logger)
        if not errors:
            stmts: list[str] = []
            # fine-tune all database sessions, as needed
            # (Oracle and SQLServer do not have session-scope commands for disabling triggers and/or rules)
            match rdbms:
                case DbEngine.MYSQL:
                    stmts.append("SET @@SESSION.DISABLE_TRIGGERS = 1")
                case DbEngine.ORACLE:
                    stmts.append("ALTER SESSION SET NLS_SORT = BINARY")
                    stmts.append("ALTER SESSION SET NLS_COMP = BINARY")
                case DbEngine.POSTGRES:
                    stmts.append("set session_replication_role = replica")
                case DbEngine.SQLSERVER:
                    pass
            if stmts:
                pool.on_event_actions(event=DbPoolEvent.CREATE,
                                      stmts=stmts)


def schema_create(schema: str,
                  rdbms: DbEngine,
                  errors: list[str],
                  logger: Logger) -> None:

    if rdbms == DbEngine.ORACLE:
        stmt: str = f"CREATE USER {schema} IDENTIFIED BY {schema}"
    else:
        user: str = db_get_param(key=DbParam.USER,
                                 engine=rdbms)
        stmt = f"CREATE SCHEMA {schema} AUTHORIZATION {user}"
    db_execute(exc_stmt=stmt,
               engine=rdbms,
               errors=errors,
               logger=logger)

    logger.debug(msg=f"RDBMS {rdbms}, created schema {schema}")


def column_set_nullable(rdbms: DbEngine,
                        table: str,
                        column: str,
                        errors: list[str],
                        logger: Logger) -> None:

    # build the statement
    alter_stmt: str | None = None
    match rdbms:
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
               engine=rdbms,
               errors=errors,
               logger=logger)


def view_get_ddl(view_name: str,
                 view_type: Literal["M", "P"],
                 source_rdbms: DbEngine,
                 source_schema: str,
                 target_schema: str,
                 errors: list[str],
                 logger: Logger) -> str:

    # obtain the script used to create the view
    result: str = db_get_view_ddl(view_type=view_type,
                                  view_name=f"{source_schema}.{view_name}",
                                  engine=source_rdbms,
                                  errors=errors,
                                  logger=logger)
    if result:
        # script has been retrieved, create the view in the target schema
        result = result.lower().replace(f"{source_schema}.", f"{target_schema}.")\
                               .replace(f'"{source_schema}".', f'"{target_schema}".')
        if source_rdbms == DbEngine.ORACLE:
            # purge Oracle-specific clauses
            result = result.replace("force editionable ", "")
        # errors ?
    else:
        # script has not been retrieved, report the problem
        err_msg: str = ("unable to retrieve creation script "
                        f"for view {source_rdbms}.{source_schema}.{view_name}")
        logger.error(msg=err_msg)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102,
                                            err_msg))
    return result


def table_embedded_nulls(rdbms: DbEngine,
                         table: str,
                         errors: list[str],
                         logger: Logger) -> None:

    # was a 'ValueError' exception on NULLs in strings raised ?
    # ("A string literal cannot contain NUL (0x00) characters.")
    if " contain NUL " in " ".join(errors):
        # yes, provide instructions on how to handle the problem
        err_msg: str = (f"Table {rdbms}.{table} has NULLs embedded in string data, "
                        f"which is not accepted by the destination database. Please add this "
                        f"table to the 'remove-nulls' migration parameter, and try again.")
        logger.error(msg=err_msg)
        # 101: {}
        errors.append(validate_format_error(101,
                                            err_msg))
