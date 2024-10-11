from logging import Logger
from pypomes_core import validate_format_error
from pypomes_db import db_get_param, db_get_view_ddl, db_execute
from typing import Any, Literal


def schema_create(errors: list[str],
                  schema: str,
                  rdbms: str,
                  logger: Logger) -> None:

    if rdbms == "oracle":
        stmt: str = f"CREATE USER {schema} IDENTIFIED BY {schema}"
    else:
        user: str = db_get_param(key="user",
                                 engine=rdbms)
        stmt = f"CREATE SCHEMA {schema} AUTHORIZATION {user}"
    db_execute(errors=errors,
               exc_stmt=stmt,
               engine=rdbms,
               logger=logger)


def session_disable_restrictions(errors: list[str],
                                 rdbms: str,
                                 conn: Any,
                                 logger: Logger) -> None:

    # disable session restrictions to speed-up bulk operations
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            pass
        case "postgres":
            db_execute(errors=errors,
                       exc_stmt="SET SESSION_REPLICATION_ROLE TO REPLICA",
                       engine="postgres",
                       connection=conn,
                       logger=logger)
        case "sqlserver":
            pass

    logger.debug(msg=f"RDBMS {rdbms}, disabled session "
                 "restrictions to speed-up bulk operations")


def session_restore_restrictions(errors: list[str],
                                 rdbms: str,
                                 conn: Any,
                                 logger: Logger) -> None:

    # restore session restrictions delaying bulk operations
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            pass
        case "postgres":
            db_execute(errors=errors,
                       exc_stmt="SET SESSION_REPLICATION_ROLE TO DEFAULT",
                       engine="postgres",
                       connection=conn,
                       logger=logger)
        case "sqlserver":
            pass

    logger.debug(msg=f"RDBMS {rdbms}, restored session "
                     "restrictions delaying bulk operations")


def column_set_nullable(errors: list[str],
                        rdbms: str,
                        table: str,
                        column: str,
                        logger: Logger) -> None:

    # build the statement
    alter_stmt: str | None = None
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            alter_stmt = (f"ALTER TABLE {table} "
                          f"MODIFY ({column} NULL)")
        case "postgres" | "sqlserver":
            alter_stmt = (f"ALTER TABLE {table} "
                          f"ALTER COLUMN {column} DROP NOT NULL")
    # execute it
    db_execute(errors=errors,
               exc_stmt=alter_stmt,
               engine=rdbms,
               logger=logger)


def view_get_ddl(errors: list[str],
                 view_name: str,
                 view_type: Literal["M", "P"],
                 source_rdbms: str,
                 source_schema: str,
                 target_schema: str,
                 logger: Logger) -> str:

    # obtain the script used to create the view
    result: str = db_get_view_ddl(errors=errors,
                                  view_type=view_type,
                                  view_name=f"{source_schema}.{view_name}",
                                  engine=source_rdbms,
                                  logger=logger)
    # has the script been retrieved ?
    if result:
        # yes, create the view in the target schema
        result = result.lower().replace(f"{source_schema}.", f"{target_schema}.")\
                               .replace(f'"{source_schema}".', f'"{target_schema}".')
        if source_rdbms == "oracle":
            # purge Oracle-specific clauses
            result = result.replace("force editionable ", "")
        # errors ?
    else:
        # no, report the problem
        err_msg: str = ("unable to retrieve creation script "
                        f"for view {source_rdbms}.{source_schema}.{view_name}")
        logger.error(msg=err_msg)
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102,
                                            err_msg))
    return result


def check_embedded_nulls(errors: list[str],
                         rdbms: str,
                         table: str,
                         logger: Logger) -> None:

    # did a 'ValueError' exception on NULLs in strings occur ?
    # ("A string literal cannot contain NUL (0x00) characters.")
    if " contain NUL " in " ".join(errors):
        # yes, provide instructions on how to handle the problem
        err_msg: str = (f"Table {rdbms}.{table} has NULLs embedded in string data, "
                        f"which is not accepted by the database. Please add this "
                        f"table to the 'remove-nulls' migration parameter, and try again.")
        logger.error(msg=err_msg)
        # 101: {}
        errors.append(validate_format_error(101,
                                            err_msg))
