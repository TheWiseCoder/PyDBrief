import sqlalchemy
from pypomes_core import validate_format_error

MSQL_DB_NAME: str | None = None
MSQL_DB_USER: str | None = None
MSQL_DB_PWD: str | None = None
MSQL_DB_HOST: str | None = None
MSQL_DB_PORT: int | None = None


def set_connection_params(errors: list[str],
                          scheme: dict,
                          mandatory: bool) -> None:
    """
    Establish the parameters for connection to the SQLServer engine.

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
        global MSQL_DB_NAME
        MSQL_DB_NAME = scheme.get("db_name")
    if hasattr(scheme, "db_user"):
        global MSQL_DB_USER
        MSQL_DB_USER = scheme.get("db_user")
    if hasattr(scheme, "db_pwd"):
        global MSQL_DB_PWD
        MSQL_DB_PWD = scheme.get("db_pwd")
    if hasattr(scheme, "db_host"):
        global MSQL_DB_HOST
        MSQL_DB_HOST = scheme.get("db_host")
    if hasattr(scheme, "db_port"):
        if scheme.get("db_port").isnumeric():
            global MSQL_DB_PORT
            MSQL_DB_PORT = int(scheme.get("db_port"))
        else:
            # 128: Invalid value {}: must be type {}
            errors.append(validate_format_error(128, "int", "@MSQL_PORT"))

    if mandatory:
        assert_connection_params(errors)


def assert_connection_params(errors: list[str]) -> bool:
    """
    Assert that the parameters for connecting with the PostgreSQL engine have been provided.

    The *errors* argument will contain the appropriate messages regarding missing parameters.

    :param errors: incidental error messages
    :return: 'True' if all parameters have been provided, 'False' otherwise
    """
    if not MSQL_DB_NAME:
        errors.append(validate_format_error(112, "@MSQL_NAME"))
    if not MSQL_DB_USER:
        errors.append(validate_format_error(112, "@MSQL_USER"))
    if not MSQL_DB_PWD:
        errors.append(validate_format_error(112, "@MSQL_PWD"))
    if not MSQL_DB_HOST:
        errors.append(validate_format_error(112, "@MSQL_HOST"))
    if not MSQL_DB_PORT:
        errors.append(validate_format_error(112, "@MSQL_PORT"))

    return len(errors) == 0


def build_engine(errors: list[str]) -> sqlalchemy.engine:

    return None
