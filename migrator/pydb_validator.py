from typing import Any
from pypomes_core import validate_format_error

from . import (
    pydb_common, pydb_mysql, pydb_oracle, pydb_postgres, pydb_sqlserver
)


def assert_connection(errors: list[str],
                      rdbms: str) -> None:

    # attempt to connect
    conn: Any
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            from oracledb import Connection
            conn: Connection = pydb_oracle.db_connect(errors)
            if isinstance(conn, Connection):
                conn.close()
        case "postgres":
            # noinspection PyProtectedMember
            from psycopg2._psycopg import connection
            conn: connection = pydb_postgres.db_connect(errors)
            if isinstance(conn, connection):
                conn.close()
        case "sqlserver":
            from pyodbc import Connection
            conn: Connection = pydb_sqlserver.db_connect(errors)
            if isinstance(conn, Connection):
                conn.close()
        case _:
            # 119: Invalid value {}: {}
            errors.append(validate_format_error(119, rdbms, "unknown RDMS engine"))


def assert_connection_params(errors: list[str],
                             scheme: str | dict):

    # obtain the RDBMS name
    rdbms: str = scheme if isinstance(scheme, str) \
        else pydb_common.validate_rdbms(errors, scheme, "rdbms")

    # has it been obtained ?
    if isinstance(rdbms, str):
        # yes, retrieve the corresponding engine's configuration
        match rdbms:
            case "mysql":
                pydb_mysql.assert_connection_params(errors)
            case "oracle":
                pydb_oracle.assert_connection_params(errors)
            case "postgres":
                pydb_postgres.assert_connection_params(errors)
            case "sqlserver":
                pydb_sqlserver.assert_connection_params(errors)
            case _:
                # 119: Invalid value {}: {}
                errors.append(validate_format_error(119, rdbms, "unknown RDMS engine"))


def assert_migration(errors: list[str],
                     scheme: dict) -> None:

    if pydb_common.MIGRATION_BATCH_SIZE < 1000 or \
       pydb_common.MIGRATION_BATCH_SIZE > 200000:
        # 127: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(127, pydb_common.MIGRATION_BATCH_SIZE,
                                            [1000, 200000], "@batch-size"))

    if pydb_common.MIGRATION_PROCESSES < 1 or \
       pydb_common.MIGRATION_PROCESSES > 1000:
        # 127: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(127, pydb_common.MIGRATION_PROCESSES,
                                            [1, 100], "@processes"))

    # assert the connection parameters for origin and destination RDBMS engines
    from_rdbms: str = scheme.get("from")
    to_rdbms: str = scheme.get("to")
    opt_scheme: dict = {}
    opt_scheme.update(scheme)
    opt_scheme["rdbms"] = from_rdbms
    assert_connection_params(errors, opt_scheme)
    opt_scheme["rdbms"] = to_rdbms
    assert_connection_params(errors, opt_scheme)

    # verify if actual connection is possible
    if len(errors) == 0:
        assert_connection(errors, from_rdbms)
        assert_connection(errors, to_rdbms)


def get_migration_context(scheme: dict) -> dict:

    result: dict = {
        "from": get_connection_params([], scheme.get("from")),
        "to": get_connection_params([], scheme.get("to"))
    }
    result.update(pydb_common.get_migration_params())

    return result


def get_connection_string(rdbms: str) -> str:

    # initialize the return variable
    result: str | None = None

    # obtain the connection string
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            result = pydb_oracle.build_connection_string()
        case "postgres":
            result = pydb_postgres.build_connection_string()
        case "sqlserver":
            result = pydb_sqlserver.build_connection_string()

    return result


def get_connection_params(errors: list[str],
                          scheme: str | dict) -> dict:

    # initialize the return variable
    result: dict | None = None

    rdbms: str = scheme if isinstance(scheme, str) \
        else pydb_common.validate_rdbms(errors, scheme, "rdbms")

    # retrieve the engine's configuration
    if len(errors) == 0:
        match rdbms:
            case "mysql":
                result = pydb_mysql.get_connection_params()
            case "oracle":
                result = pydb_oracle.get_connection_params()
            case "postgres":
                result = pydb_postgres.get_connection_params()
            case "sqlserver":
                result = pydb_sqlserver.get_connection_params()
            case _:
                # 119: Invalid value {}: {}
                errors.append(validate_format_error(119, rdbms, "unknown engine"))

    return result


def set_connection_params(errors: list[str],
                          scheme: dict) -> None:

    rdbms: str = pydb_common.validate_rdbms(errors, scheme, "rdbms")

    # configure the engine
    if len(errors) == 0:
        match rdbms:
            case "mysql":
                pydb_mysql.set_connection_params(errors, scheme, False)
            case "oracle":
                pydb_oracle.set_connection_params(errors, scheme, False)
            case "postgres":
                pydb_postgres.set_connection_params(errors, scheme, False)
            case "sqlserver":
                pydb_sqlserver.set_connection_params(errors, scheme, False)
            case _:
                # 119: Invalid value {}: {}
                errors.append(validate_format_error(119, rdbms, "unknown engine"))
