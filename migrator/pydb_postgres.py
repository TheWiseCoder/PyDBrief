from logging import Logger
# noinspection PyProtectedMember
from psycopg2._psycopg import connection
from pypomes_db import db_get_params, db_execute


def build_connection_string() -> str:

    params: dict = db_get_params("postgres")
    return (
        f"postgresql+psycopg2://{params.get('user')}:"
        f"{params.get('pwd')}@{params.get('host')}:{params.get('port')}/{params.get('name')}"
    )


def build_bulk_insert_stmt(schema: str,
                           table: str,
                           columns: str) -> str:

    return (
        f"INSERT INTO {schema}.{table} "
        f"({columns}) "
        f"VALUES %s"
    )


def disable_session_restrictions(errors: list[str],
                                 conn: connection,
                                 logger: Logger) -> None:

    db_execute(errors=errors,
               exc_stmt="SET SESSION_REPLICATION_ROLE TO REPLICA",
               engine="postgres",
               conn=conn,
               logger=logger)


def restore_session_restrictions(errors: list[str],
                                 conn: connection,
                                 logger: Logger) -> None:

    db_execute(errors=errors,
               exc_stmt="SET SESSION_REPLICATION_ROLE TO DEFAULT",
               engine="postgres",
               conn=conn,
               logger=logger)
