from logging import Logger
from pyodbc import connect, Connection
from pypomes_core import validate_format_error
from sqlalchemy import Engine, create_engine

from .pydb_common import db_except_msg, db_log

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.mssql import (
    BIGINT,
    BINARY,
    BIT,
    CHAR,
    DATE,
    DATETIME,
    DATETIME2,
    DATETIMEOFFSET,
    DECIMAL,
    DOUBLE_PRECISION,
    FLOAT,
    IMAGE,
    INTEGER,
    JSON,
    MONEY,
    NCHAR,
    NTEXT,
    NUMERIC,
    NVARCHAR,
    REAL,
    SMALLDATETIME,
    SMALLINT,
    SMALLMONEY,
    SQL_VARIANT,
    TEXT,
    TIME,
    TIMESTAMP,
    TINYINT,
    UNIQUEIDENTIFIER,
    VARBINARY,
    VARCHAR,
    # types which are specific to SQL Server, or have SQL Server-specific construction arguments:
    BIT,
    DATETIME2,
    DATETIMEOFFSET,
    DOUBLE_PRECISION,
    IMAGE,
    JSON,
    MONEY,
    NTEXT,
    REAL,
    ROWVERSION,
    SMALLDATETIME,
    SMALLMONEY,
    SQL_VARIANT,
    TIME,
    TIMESTAMP,
    TINYINT,
    UNIQUEIDENTIFIER,
    XML,
)


# noinspection DuplicatedCode
MS_DB_DRIVER: str | None = None
MS_DB_NAME: str | None = None
MS_DB_USER: str | None = None
MS_DB_PWD: str | None = None
MS_DB_HOST: str | None = None
MS_DB_PORT: int | None = None


def db_connect(errors: list[str] | None,
               logger: Logger = None) -> Connection:
    """
    Obtain and return a connection to the database, or *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return valiable
    result: Connection | None = None

    conn_string: str = (
        f"DRIVER={{{MS_DB_DRIVER}}};SERVER={MS_DB_HOST},{MS_DB_PORT};"
        f"DATABASE={MS_DB_NAME};UID={MS_DB_USER};PWD={MS_DB_PWD};TrustServerCertificate=yes;"
    )

    # Obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(conn_string)
    except Exception as e:
        err_msg = db_except_msg(e, MS_DB_NAME, MS_DB_HOST)

    # log the results
    db_log(errors, err_msg, logger, f"Connected to '{MS_DB_NAME}' at '{MS_DB_HOST}'")

    return result


def get_connection_params() -> dict:
    
    return {
        "rdbms": "sqlserver",
        "name": MS_DB_NAME,
        "user": MS_DB_USER,
        "password": MS_DB_PWD,
        "host": MS_DB_HOST,
        "port": MS_DB_PORT,
        "driver": MS_DB_DRIVER
    }


def set_connection_params(errors: list[str],
                          scheme: dict,
                          mandatory: bool) -> None:
    """
    Establish the parameters for connection to the SQLServer engine.

    These are the parameters:
        - *db-name*: name of the database
        - *db-user*: name of logon user
        - *db-pwd*: password for login
        - *db-host*: host URL
        - *db-port*: host port
        - *db-driver*: connection driver

    :param errors: incidental error messages
    :param scheme: the provided parameters
    :param mandatory: the parameters must be provided
    """
    # noinspection DuplicatedCode
    if hasattr(scheme, "db-name"):
        global MS_DB_NAME
        MS_DB_NAME = scheme.get("db-name")
    if hasattr(scheme, "db-user"):
        global MS_DB_USER
        MS_DB_USER = scheme.get("db-user")
    if hasattr(scheme, "db-pwd"):
        global MS_DB_PWD
        MS_DB_PWD = scheme.get("db-pwd")
    if hasattr(scheme, "db-driver"):
        global MS_DB_DRIVER
        MS_DB_NAME = scheme.get("db-driver")
    if hasattr(scheme, "db-host"):
        global MS_DB_HOST
        MS_DB_HOST = scheme.get("db-host")
    if hasattr(scheme, "db-port"):
        if scheme.get("db-port").isnumeric():
            global MS_DB_PORT
            MS_DB_PORT = int(scheme.get("db-port"))
        else:
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@MS_DB_PORT"))

    if mandatory:
        assert_connection_params(errors)


def assert_connection_params(errors: list[str]) -> bool:
    """
    Assert that the parameters for connecting with the PostgreSQL engine have been provided.

    The *errors* argument will contain the appropriate messages regarding missing parameters.

    :param errors: incidental error messages
    :return: 'True' if all parameters have been provided, 'False' otherwise
    """
    # 112: Required attribute
    if not MS_DB_NAME:
        errors.append(validate_format_error(112, "@MSQL_NAME"))
    if not MS_DB_USER:
        errors.append(validate_format_error(112, "@MSQL_USER"))
    if not MS_DB_PWD:
        errors.append(validate_format_error(112, "@MSQL_PWD"))
    if not MS_DB_HOST:
        errors.append(validate_format_error(112, "@MSQL_HOST"))
    if not MS_DB_PORT:
        errors.append(validate_format_error(112, "@MSQL_PORT"))
    if not MS_DB_DRIVER:
        errors.append(validate_format_error(112, "@MSQL_DRIVER"))

    return len(errors) == 0


def build_connection_string() -> str:

    return (
        f"mssql+pyodbc://{MS_DB_USER}:"
        f"{MS_DB_PWD}@{MS_DB_HOST}:{MS_DB_PORT}/{MS_DB_NAME}?driver={MS_DB_DRIVER}"
    )


def build_engine(errors: list[str]) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    conn_string: str = (
        f"mssql+pyodbc://{MS_DB_USER}:"
        f"{MS_DB_PWD}@{MS_DB_HOST}:{MS_DB_PORT}/{MS_DB_NAME}?driver={MS_DB_DRIVER}"
    )
    try:
        result = create_engine(url=conn_string)
    except Exception as e:
        errors.append(db_except_msg(e, MS_DB_NAME, MS_DB_HOST))

    return result


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


def get_table_unlog_stmt(_table: str) -> str:

    # table logging cannot be disable in SQLServer
    # noinspection PyTypeChecker
    return None
