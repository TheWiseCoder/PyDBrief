from logging import Logger
from psycopg2 import connect
# noinspection PyProtectedMember
from psycopg2._psycopg import connection
from pypomes_core import validate_format_error

from .pydb_common import db_except_msg, db_log

# noinspection DuplicatedCode
PG_DB_NAME: str | None = None
PG_DB_USER: str | None = None
PG_DB_PWD: str | None = None
PG_DB_HOST: str | None = None
PG_DB_PORT: int | None = None


def db_connect(errors: list[str] | None,
               logger: Logger = None) -> connection:
    """
    Obtain and return a connection to the database, or *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: connection | None = None

    # Obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(host=PG_DB_HOST,
                         port=PG_DB_PORT,
                         database=PG_DB_NAME,
                         user=PG_DB_USER,
                         password=PG_DB_PWD)
    except Exception as e:
        err_msg = db_except_msg(e, PG_DB_NAME, PG_DB_HOST)

    # log the results
    db_log(errors, err_msg, logger, f"Connected to '{PG_DB_NAME}' at '{PG_DB_HOST}'")

    return result


def get_connection_params() -> dict:
    
    return {
        "rdbms": "postgres",
        "name": PG_DB_NAME,
        "user": PG_DB_USER,
        "password": PG_DB_PWD,
        "host": PG_DB_HOST,
        "port": PG_DB_PORT
    }


def set_connection_params(errors: list[str],
                          scheme: dict,
                          mandatory: bool) -> None:
    """
    Establish the parameters for connection to the PostgreSQL engine.

    These are the parameters:
        - *db-name*: name of the database
        - *db-user*: name of logon user
        - *db-pwd*: password for login
        - *db-host*: host URL
        - *db-port*: host port

    :param errors: incidental error messages
    :param scheme: the provided parameters
    :param mandatory: the parameters must be provided
    """
    # noinspection DuplicatedCode
    if scheme.get("db-name"):
        global PG_DB_NAME
        PG_DB_NAME = scheme.get("db-name")
    if scheme.get("db-user"):
        global PG_DB_USER
        PG_DB_USER = scheme.get("db-user")
    if scheme.get("db-pwd"):
        global PG_DB_PWD
        PG_DB_PWD = scheme.get("db-pwd")
    if scheme.get("db-host"):
        global PG_DB_HOST
        PG_DB_HOST = scheme.get("db-host")
    if scheme.get("db-port"):
        if scheme.get("db-port").isnumeric():
            global PG_DB_PORT
            PG_DB_PORT = int(scheme.get("db-port"))
        else:
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@PG_DB_PORT"))

    if mandatory:
        assert_connection_params(errors)


def assert_connection_params(errors: list[str]) -> None:
    """
    Assert that the parameters for connecting with the PostgreSQL engine have been provided.

    The *errors* argument will contain the appropriate messages regarding missing parameters.

    :param errors: incidental error messages
    """
    # 112: Required attribute
    if not PG_DB_NAME:
        errors.append(validate_format_error(112, "@PG_DB_NAME"))
    if not PG_DB_USER:
        errors.append(validate_format_error(112, "@PG_DB_USER"))
    if not PG_DB_PWD:
        errors.append(validate_format_error(112, "@PG_DB_PWD"))
    if not PG_DB_HOST:
        errors.append(validate_format_error(112, "@PG_DB_HOST"))
    if not PG_DB_PORT:
        errors.append(validate_format_error(112, "@PG_DB_PORT"))


def build_connection_string() -> str:

    return (
        f"postgresql+psycopg2://{PG_DB_USER}:"
        f"{PG_DB_PWD}@{PG_DB_HOST}:{PG_DB_PORT}/{PG_DB_NAME}"
    )


def build_select_query(schema: str,
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


def get_table_unlog_stmt(table: str) -> str:

    return f"ALTER TABLE {table} SET UNLOGGED;"
