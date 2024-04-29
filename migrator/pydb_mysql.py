from logging import Logger
from pyodbc import connect, Connection
from pypomes_core import validate_format_error
from sqlalchemy import Engine, create_engine

# noinspection DuplicatedCode
MSQL_DB_NAME: str | None = None
MSQL_DB_USER: str | None = None
MSQL_DB_PWD: str | None = None
MSQL_DB_HOST: str | None = None
MSQL_DB_PORT: int | None = None


def get_connection_params() -> dict:
    
    return {
        "rdbms": "sqlserver",
        "name": MSQL_DB_NAME,
        "user": MSQL_DB_USER,
        "password": MSQL_DB_PWD,
        "host": MSQL_DB_HOST,
        "port": MSQL_DB_PORT
    }


def set_connection_params(errors: list[str],
                          scheme: dict) -> None:
    """
    Establish the parameters for connection to the SQLServer engine.

    These are the parameters:
        - *db-name*: name of the database
        - *db-user*: name of logon user
        - *db-pwd*: password for login
        - *db-host*: host URL
        - *db-port*: host port

    :param errors: incidental error messages
    :param scheme: the provided parameters
    """
    # Oracle-only parameter
    if scheme.get("db-client"):
        # 113: Attribute not applicable for {}
        errors.append(validate_format_error(113, "MySQL ", "@db-client"))

    # SQLServer-only parameter
    if scheme.get("db-driver"):
        # 113: Attribute not applicable for {}
        errors.append(validate_format_error(113, "MySQL ", "@db-driver"))

    if scheme.get("db-port"):
        if not scheme.get("db-port").isnumeric():
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@MSQL_DB_PORT"))
        elif len(errors) == 0:
            global MSQL_DB_PORT
            MSQL_DB_PORT = int(scheme.get("db-port"))

    # noinspection DuplicatedCode
    if len(errors) == 0:
        if scheme.get("db-name"):
            global MSQL_DB_NAME
            MSQL_DB_NAME = scheme.get("db-name")
        if scheme.get("db-user"):
            global MSQL_DB_USER
            MSQL_DB_USER = scheme.get("db-user")
        if scheme.get("db-pwd"):
            global MSQL_DB_PWD
            MSQL_DB_PWD = scheme.get("db-pwd")
        if scheme.get("db-host"):
            global MSQL_DB_HOST
            MSQL_DB_HOST = scheme.get("db-host")


def assert_connection_params(errors: list[str]) -> bool:
    """
    Assert that the parameters for connecting with the MySql engine have been provided.

    The *errors* argument will contain the appropriate messages regarding missing parameters.

    :param errors: incidental error messages
    :return: 'True' if all parameters have been provided, 'False' otherwise
    """
    # 112: Required attribute
    if not MSQL_DB_NAME:
        errors.append(validate_format_error(112, "@MSQL_DB_NAME"))
    if not MSQL_DB_USER:
        errors.append(validate_format_error(112, "@MSQL_DB_USER"))
    if not MSQL_DB_PWD:
        errors.append(validate_format_error(112, "@MSQL_DB_PWD"))
    if not MSQL_DB_HOST:
        errors.append(validate_format_error(112, "@MSQL_DB_HOST"))
    if not MSQL_DB_PORT:
        errors.append(validate_format_error(112, "@MSQL_DB_PORT"))

    return len(errors) == 0
