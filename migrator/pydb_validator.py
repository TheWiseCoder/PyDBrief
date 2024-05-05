from pypomes_core import validate_format_error
from pypomes_db import (
    db_setup, db_get_engines, db_get_params, db_assert_connection
)

from . import (
    pydb_common, pydb_oracle, pydb_postgres, pydb_sqlserver  # py_mysql,
)


def validate_rdbms_dual(errors: list[str],
                        scheme: dict) -> tuple[str, str]:

    engines: list[str] = db_get_engines()
    source_rdbms: str = scheme.get("from-rdbms")
    if source_rdbms not in engines:
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, source_rdbms,
                                            "unknown or unconfigured RDMS engine", "@from-rdbms"))
    target_rdbms: str = scheme.get("to-rdbms")
    if target_rdbms not in engines:
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, target_rdbms,
                                            "unknown or unconfigured RDMS engine", "@to-rdbms"))

    if source_rdbms and source_rdbms == target_rdbms:
        # 116: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(116, source_rdbms, "'from-rdbms' and 'to-rdbms'"))

    return source_rdbms, target_rdbms


def assert_connection_params(errors: list[str],
                             scheme: str | dict):

    # obtain the RDBMS name
    rdbms: str = scheme if isinstance(scheme, str) else scheme.get("rdbms")
    if not db_get_params(rdbms):
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, rdbms, "unknown or unconfigured RDMS engine"))


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
    from_rdbms: str = scheme.get("from-rdbms")
    to_rdbms: str = scheme.get("to-rdbms")
    opt_scheme: dict = {}
    opt_scheme.update(scheme)
    opt_scheme["rdbms"] = from_rdbms
    assert_connection_params(errors, opt_scheme)
    opt_scheme["rdbms"] = to_rdbms
    assert_connection_params(errors, opt_scheme)

    # verify if actual connection is possible
    if len(errors) == 0:
        db_assert_connection(errors=errors,
                             engine=from_rdbms)
        db_assert_connection(errors=errors,
                             engine=to_rdbms)


def get_migration_context(scheme: dict) -> dict:

    result: dict = {
        "from": db_get_params(scheme.get("from-rdbms")),
        "to": db_get_params(scheme.get("to-rdbms"))
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

    rdbms: str = scheme if isinstance(scheme, str) else scheme.get("rdms")
    result: dict = db_get_params(rdbms)
    result["rdms"] = rdbms
    if not result:
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, rdbms,
                                            "unknown or unconfigured RDMS engine", "@rdbms"))

    return result


def set_connection_params(errors: list[str],
                          scheme: dict) -> None:

    if not db_setup(engine=scheme.get("engine"),
                    db_name=scheme.get("db-name"),
                    db_host=scheme.get("db-host"),
                    db_port=scheme.get("db-port"),
                    db_user=scheme.get("db-user"),
                    db_pwd=scheme.get("db-pwd"),
                    db_client=scheme.get("db-client"),
                    db_driver=scheme.get("db-driver")):
        # 120: Argumento(s) inválido(s), inconsistente(s) ou não fornecido(s)
        errors.append(validate_format_error(120))
