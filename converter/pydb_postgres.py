# noinspection PyDuplicateCode
import sqlalchemy
from pypomes_core import validate_format_error

from .pydb_common import db_except_msg

PG_DB_NAME: str | None = None
PG_DB_USER: str | None = None
PG_DB_PWD: str | None = None
PG_DB_HOST: str | None = None
PG_DB_PORT: int | None = None


def set_connection_params(errors: list[str],
                          scheme: dict,
                          mandatory: bool) -> None:
    """
    Establish the parameters for connection to the PostgreSQL engine.

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
        global PG_DB_NAME
        PG_DB_NAME = scheme.get("db_name")
    if hasattr(scheme, "db_user"):
        global PG_DB_USER
        PG_DB_USER = scheme.get("db_user")
    if hasattr(scheme, "db_pwd"):
        global PG_DB_PWD
        PG_DB_PWD = scheme.get("db_pwd")
    if hasattr(scheme, "db_host"):
        global PG_DB_HOST
        PG_DB_HOST = scheme.get("db_host")
    if hasattr(scheme, "db_port"):
        if scheme.get("db_port").isnumeric():
            global PG_DB_PORT
            PG_DB_PORT = int(scheme.get("db_port"))
        else:
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@PG_PORT"))

    if mandatory:
        assert_connection_params(errors)


def assert_connection_params(errors: list[str]) -> None:
    """
    Assert that the parameters for connecting with the PostgreSQL engine have been provided.

    The *errors* argument will contain the appropriate messages regarding missing parameters.

    :param errors: incidental error messages
    """
    if not PG_DB_NAME:
        errors.append(validate_format_error(112, "@PG_NAME"))
    if not PG_DB_USER:
        errors.append(validate_format_error(112, "@PG_USER"))
    if not PG_DB_PWD:
        errors.append(validate_format_error(112, "@PG_PWD"))
    if not PG_DB_HOST:
        errors.append(validate_format_error(112, "@PG_HOST"))
    if not PG_DB_PORT:
        errors.append(validate_format_error(112, "@PG_PORT"))


def build_engine(errors: list[str]) -> sqlalchemy.engine:

    # initialize the return variable
    result: sqlalchemy.engine = None

    conn_string: str = (
        f"postgresql+psycopg2://{PG_DB_USER}:"
        f"{PG_DB_PWD}@{PG_DB_HOST}:{PG_DB_PORT}/{PG_DB_NAME}"
    )
    try:
        result = sqlalchemy.create_engine(url=conn_string)
    except Exception as e:
        errors.append(db_except_msg(e, PG_DB_NAME, PG_DB_HOST))

    return result
