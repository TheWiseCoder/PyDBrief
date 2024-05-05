from logging import Logger
from pypomes_db import db_get_params, db_execute


def build_connection_string() -> str:

    params: dict = db_get_params("postgres")
    return (
        f"postgresql+psycopg2://{params.get('user')}:"
        f"{params.get('pwd')}@{params.get('host')}:{params.get('port')}/{params.get('name')}"
    )


def build_bulk_select_stmt(schema: str,
                           table: str,
                           columns: str,
                           offset: int,
                           batch_size: int) -> str:
    return (
        f"SELECT {columns} "
        f"FROM {schema}.{table} "
        f"ORDER BY rowid "
        f"OFFSET {offset} ROWS FETCH NEXT {batch_size} ROWS ONLY"
    )


def build_bulk_insert_stmt(schema: str,
                           table: str,
                           columns: str) -> str:

    return (
        f"INSERT INTO {schema}.{table} "
        f"({columns}) "
        f"VALUES(%s)"
    )


def get_table_unlog_stmt(schema: str,
                         table: str) -> str:

    return f"ALTER TABLE {schema}.{table} SET UNLOGGED"


def get_disable_restriction_stmts() -> list[str]:

    return [
        "SET SESSION_REPLICATION_ROLE TO REPLICA",
        "UPDATE pg_index SET indisready = false"
    ]


def enable_restrictions(errors: list[str], logger: Logger) -> None:

    db_execute(errors=errors,
               exc_stmt="SET SESSION_REPLICATION_ROLE TO DEFAULT;",
               engine="posgres",
               logger=logger)
    if not errors:
        db_execute(errors=errors,
                   exc_stmt="UPDATE pg_index SET indisready = true;",
                   engine="posgres",
                   logger=logger)
