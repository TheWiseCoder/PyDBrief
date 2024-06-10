from logging import Logger, DEBUG
from pypomes_db import db_execute, db_select
from typing import Any

from migration import pydb_common


def get_view_dependencies(errors: list[str],
                          rdbms: str,
                          schema: str,
                          view_name: str,
                          logger: Logger) -> list[str]:

    # initialize the return variable
    result: list[str] | None = None

    sel_stmt: str | None = None
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            sel_stmt = ("SELECT DISTINCT referenced_name "
                        "FROM all_dependencies "
                        "WHERE name = UPPER(:1) AND owner = UPPER(:2) "
                        "AND type = 'VIEW' AND referenced_type = 'TABLE'")
        case "postgres":
            sel_stmt = ("SELECT DISTINCT cl1.relname "
                        "FROM pg_class AS cl1 "
                        "JOIN pg_rewrite AS rw ON rw.ev_class = cl1.oid "
                        "JOIN pg_depend AS d ON d.objid = rw.oid "
                        "JOIN pg_class AS cl2 ON cl2.oid = d.refobjid "
                        "WHERE LOWER(cl2.relname) = LOWER(%s) AND cl2.relnamespace = "
                        "(SELECT oid FROM pg_namespace WHERE LOWER(nspname) = LOWER(%s))")
        case "sqlserver":
            sel_stmt = ("SELECT DISTINCT referencing_entity_name "
                        "FROM sys.dm_sql_referencing_entities (LOWER(?), 'OBJECT') "
                        "WHERE LOWER(referencing_schema_name) = LOWER(?)")

    # initialize the local errors list
    op_errors: list[str] = []

    # execute the query
    recs: list[tuple[str]] = db_select(errors=op_errors,
                                       sel_stmt=sel_stmt,
                                       where_vals=(view_name, schema),
                                       engine=rdbms,
                                       logger=logger)
    if op_errors:
        errors.extend(op_errors)
    else:
        result = [name[0].lower() for name in recs]

    return result


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
