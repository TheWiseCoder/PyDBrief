import sqlalchemy
import oracledb
from logging import Logger
from oracledb import Connection, connect
from pypomes_core import validate_format_error

from .pydb_common import db_except_msg, db_log

ORCL_DB_NAME: str | None = None
ORCL_DB_USER: str | None = None
ORCL_DB_PWD: str | None = None
ORCL_DB_HOST: str | None = None
ORCL_DB_PORT: int | None = None


def set_connection_params(errors: list[str],
                          scheme: dict,
                          mandatory: bool) -> None:
    """
    Establish the parameters for connection to the Oracle engine.

    These are the parameters:
        - db_name: name of the database
        - db_user: name of logon user
        - db_pwd: password for login
        - db_host: host URL
        - db_port: host port

    :param errors: incidental error messages
    :param scheme: the provided parameters
    :param mandatory: the parameters must be provided
    """
    # noinspection DuplicatedCode
    if hasattr(scheme, "db_name"):
        global ORCL_DB_NAME
        ORCL_DB_NAME = scheme.get("db_name")
    if hasattr(scheme, "db_user"):
        global ORCL_DB_USER
        ORCL_DB_USER = scheme.get("db_user")
    if hasattr(scheme, "db_pwd"):
        global ORCL_DB_PWD
        ORCL_DB_PWD = scheme.get("db_pwd")
    if hasattr(scheme, "db_host"):
        global ORCL_DB_HOST
        ORCL_DB_HOST = scheme.get("db_host")
    if hasattr(scheme, "db_port"):
        if scheme.get("db_port").isnumeric():
            global ORCL_DB_PORT
            ORCL_DB_PORT = int(scheme.get("db_port"))
        else:
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@ORCL_PORT"))

    if mandatory:
        assert_connection_params(errors)


def assert_connection_params(errors: list[str]) -> bool:
    """
    Assert that the parameters for connecting with the PostgreSQL engine have been provided.

    The *errors* argument will contain the appropriate messages regarding missing parameters.

    :param errors: incidental error messages
    :return: 'True' if all parameters have been provided, 'False' otherwise
    """
    if not ORCL_DB_NAME:
        errors.append(validate_format_error(112, "@ORCL_NAME"))
    if not ORCL_DB_USER:
        errors.append(validate_format_error(112, "@ORCL_USER"))
    if not ORCL_DB_PWD:
        errors.append(validate_format_error(112, "@ORCL_PWD"))
    if not ORCL_DB_HOST:
        errors.append(validate_format_error(112, "@ORCL_HOST"))
    if not ORCL_DB_PORT:
        errors.append(validate_format_error(112, "@ORCL_PORT"))

    return len(errors) == 0


def db_connect(errors: list[str],
               logger: Logger = None) -> Connection:
    """
    Obtain and return a connection to the database, or *None* if the connection could not be obtained.

    :param errors: incidental error messages
    :param logger: optional logger
    :return: the connection to the database
    """
    # initialize the return variable
    result: Connection | None = None

    # obtain a connection to the database
    err_msg: str | None = None
    try:
        result = connect(
            host=ORCL_DB_HOST,
            port=ORCL_DB_PORT,
            user=ORCL_DB_USER,
            password=ORCL_DB_PWD
        )
    except Exception as e:
        err_msg = db_except_msg(e, ORCL_DB_NAME, ORCL_DB_HOST)

    # log the results
    db_log(errors, err_msg, logger, f"Connected to '{ORCL_DB_NAME}' at '{ORCL_DB_HOST}'")

    return result


def build_engine(errors: list[str]) -> sqlalchemy.engine:

    # initialize the return variable
    result: sqlalchemy.engine = None

    # build the connection string
    dsn: str = oracledb.makedsn(host=ORCL_DB_HOST,
                                port=ORCL_DB_PORT,
                                service_name=ORCL_DB_NAME)
    conn_string: str = f"oracle://{ORCL_DB_USER}:{ORCL_DB_PWD}@{dsn}"

    # create the engine
    try:
        result = sqlalchemy.create_engine(url=conn_string)
    except Exception as e:
        errors.append(db_except_msg(e, ORCL_DB_NAME, ORCL_DB_HOST))

    return result
