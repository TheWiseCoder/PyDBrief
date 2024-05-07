from logging import Logger
from mysql.connector.connection import MySQLConnection


def disable_session_restrictions(_errors: list[str],
                                 _conn: MySQLConnection,
                                 _logger: Logger) -> None:

    pass


def restore_session_restrictions(_errors: list[str],
                                 _conn: MySQLConnection,
                                 _logger: Logger) -> None:

    pass
