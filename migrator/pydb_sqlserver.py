from logging import Logger
from pyodbc import connect, Connection
from pypomes_core import validate_format_error
from sqlalchemy import Engine, create_engine

from .pydb_common import db_except_msg, db_log

# noinspection DuplicatedCode
SQLS_DB_DRIVER: str | None = None
SQLS_DB_NAME: str | None = None
SQLS_DB_USER: str | None = None
SQLS_DB_PWD: str | None = None
SQLS_DB_HOST: str | None = None
SQLS_DB_PORT: int | None = None


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
        f"DRIVER={{{SQLS_DB_DRIVER}}};SERVER={SQLS_DB_HOST},{SQLS_DB_PORT};"
        f"DATABASE={SQLS_DB_NAME};UID={SQLS_DB_USER};PWD={SQLS_DB_PWD};TrustServerCertificate=yes;"
    )

    # Obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(conn_string)
    except Exception as e:
        err_msg = db_except_msg(e, SQLS_DB_NAME, SQLS_DB_HOST)

    # log the results
    db_log(errors, err_msg, logger, f"Connected to '{SQLS_DB_NAME}' at '{SQLS_DB_HOST}'")

    return result


def get_connection_params() -> dict:
    
    return {
        "rdbms": "sqlserver",
        "name": SQLS_DB_NAME,
        "user": SQLS_DB_USER,
        "password": SQLS_DB_PWD,
        "host": SQLS_DB_HOST,
        "port": SQLS_DB_PORT,
        "driver": SQLS_DB_DRIVER
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
    if scheme.get("db-name"):
        global SQLS_DB_NAME
        SQLS_DB_NAME = scheme.get("db-name")
    if scheme.get("db-user"):
        global SQLS_DB_USER
        SQLS_DB_USER = scheme.get("db-user")
    if scheme.get("db-pwd"):
        global SQLS_DB_PWD
        SQLS_DB_PWD = scheme.get("db-pwd")
    if scheme.get("db-driver"):
        global SQLS_DB_DRIVER
        SQLS_DB_DRIVER = scheme.get("db-driver")
    if scheme.get("db-host"):
        global SQLS_DB_HOST
        SQLS_DB_HOST = scheme.get("db-host")
    if scheme.get("db-port"):
        if scheme.get("db-port").isnumeric():
            global SQLS_DB_PORT
            SQLS_DB_PORT = int(scheme.get("db-port"))
        else:
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@SQLS_DB_PORT"))

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
    if not SQLS_DB_NAME:
        errors.append(validate_format_error(112, "@SQLS_DB_NAME"))
    if not SQLS_DB_USER:
        errors.append(validate_format_error(112, "@SQLS_DB_USER"))
    if not SQLS_DB_PWD:
        errors.append(validate_format_error(112, "@SQLS_DB_PWD"))
    if not SQLS_DB_HOST:
        errors.append(validate_format_error(112, "@SQLS_DB_HOST"))
    if not SQLS_DB_PORT:
        errors.append(validate_format_error(112, "@SQLS_DB_PORT"))
    if not SQLS_DB_DRIVER:
        errors.append(validate_format_error(112, "@SQLS_DB_DRIVER"))

    return len(errors) == 0


def build_connection_string() -> str:

    return (
        f"mssql+pyodbc://{SQLS_DB_USER}:"
        f"{SQLS_DB_PWD}@{SQLS_DB_HOST}:{SQLS_DB_PORT}/{SQLS_DB_NAME}?driver={SQLS_DB_DRIVER}"
    )


def build_engine(errors: list[str]) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    conn_string: str = (
        f"mssql+pyodbc://{SQLS_DB_USER}:"
        f"{SQLS_DB_PWD}@{SQLS_DB_HOST}:{SQLS_DB_PORT}/{SQLS_DB_NAME}?driver={SQLS_DB_DRIVER}"
    )
    try:
        result = create_engine(url=conn_string)
    except Exception as e:
        errors.append(db_except_msg(e, SQLS_DB_NAME, SQLS_DB_HOST))

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
