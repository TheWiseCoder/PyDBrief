from logging import Logger
from oracledb import Connection, makedsn
from pypomes_db import db_get_params


def build_connection_string() -> str:

    params: dict = db_get_params("oracle")
    dsn: str = makedsn(host=params.get("host"),
                       port=params.get("port"),
                       service_name=params.get("name"))
    return f"oracle+oracledb://{params.get('user')}:{params.get('pwd')}@{dsn}"


def build_bulk_insert_stmt(schema: str,
                           table: str,
                           columns: str) -> str:

    values: list(str) = [f", :{pos}" for pos in range(1, len(columns)+1)]
    return (
        f"INSERT INTO {schema}.{table} "
        f"({columns}) "
        f"VALUES({values[2:]})"
    )


def disable_session_restrictions(_errors: list[str],
                                 _conn: Connection,
                                 _logger: Logger) -> None:

    pass


def restore_session_restrictions(_errors: list[str],
                                 _conn: Connection,
                                 _logger: Logger) -> None:

    pass
