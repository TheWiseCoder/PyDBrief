from logging import Logger, DEBUG
from pypomes_db import db_get_param, db_execute
from typing import Any

from migration import pydb_common


def create_schema(errors: list[str],
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


def disable_session_restrictions(errors: list[str],
                                 rdbms: str,
                                 conn: Any,
                                 logger: Logger) -> None:

    # disable session restrictions to speed-up bulk copy
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

    pydb_common.log(logger=logger,
                    level=DEBUG,
                    msg=f"RDBMS {rdbms}, disabled session restrictions to speed-up bulk copying")


def restore_session_restrictions(errors: list[str],
                                 rdbms: str,
                                 conn: Any,
                                 logger: Logger) -> None:

    # restore session restrictions delaying bulk copy
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

    pydb_common.log(logger=logger,
                    level=DEBUG,
                    msg=f"RDBMS {rdbms}, restored session restrictions delaying bulk copying")


def set_nullable(errors: list[str],
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
