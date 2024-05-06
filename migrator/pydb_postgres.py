from logging import Logger
# noinspection PyProtectedMember
from psycopg2._psycopg import connection
from pypomes_db import db_get_params, db_execute  # , db_update


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


def disable_table_restrictions(errors: list[str],
                               schema: str,
                               table: str,
                               conn: connection,
                               logger: Logger) -> None:

    db_execute(errors=errors,
               exc_stmt=f"ALTER TABLE {schema}.{table} SET UNLOGGED",
               engine="postgres",
               conn=conn,
               logger=logger)

    # db_update(errors=errors,
    #           update_stmt=("UPDATE pg_index "
    #                        "SET indisready = false "
    #                        "WHERE = %s"),
    #           engine="postgres",
    #           conn=conn,
    #           logger=logger)
    pass


def restore_table_restrictions(errors: list[str],
                               schema: str,
                               table: str,
                               conn: connection,
                               logger: Logger) -> None:

    db_execute(errors=errors,
               exc_stmt=f"ALTER TABLE {schema}.{table} SET LOGGED",
               engine="postgres",
               conn=conn,
               logger=logger)

    # db_update(errors=op_errors,
    #           update_stmt=("UPDATE pg_index "
    #                        "SET indisready = true "
    #                        "WHERE "),
    #           engine="postgres",
    #           conn=conn,
    #           logger=logger)
    pass
