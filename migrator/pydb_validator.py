from pathlib import Path
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
                                            "unknown or unconfigured RDBMS engine", "@from-rdbms"))
    target_rdbms: str = scheme.get("to-rdbms")
    if target_rdbms not in engines:
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, target_rdbms,
                                            "unknown or unconfigured RDBMS engine", "@to-rdbms"))

    if source_rdbms and source_rdbms == target_rdbms:
        # 116: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(116, source_rdbms, "'from-rdbms' and 'to-rdbms'"))

    return source_rdbms, target_rdbms


def assert_connection_params(errors: list[str],
                             scheme: str | dict):

    # obtain the RDBMS name
    rdbms: str = scheme if isinstance(scheme, str) else scheme.get("rdbms")
    if rdbms not in db_get_engines():
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, rdbms, "unknown or unconfigured RDMS engine"))


def assert_migration(errors: list[str],
                     scheme: dict) -> None:

    if pydb_common.MIGRATION_BATCH_SIZE < 1000 or \
       pydb_common.MIGRATION_BATCH_SIZE > 10000000:
        # 127: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(127, pydb_common.MIGRATION_BATCH_SIZE,
                                            [1000, 10000000], "@batch-size"))

    if pydb_common.MIGRATION_CHUNK_SIZE < 1024 or \
       pydb_common.MIGRATION_CHUNK_SIZE > 16777216:
        # 127: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(127, pydb_common.MIGRATION_CHUNK_SIZE,
                                            [1024, 16777216], "@batch-size"))

    if pydb_common.MIGRATION_MAX_PROCESSES < 1 or \
       pydb_common.MIGRATION_MAX_PROCESSES > 1000:
        # 127: Invalid value {}: must be in the range {}
        errors.append(validate_format_error(127, pydb_common.MIGRATION_MAX_PROCESSES,
                                            [1, 100], "@max-processes"))

    if (pydb_common.MIGRATION_TEMP_FOLDER and
       not Path(pydb_common.MIGRATION_TEMP_FOLDER).is_dir()):
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, pydb_common.MIGRATION_TEMP_FOLDER, "not a valid folder",
                                            "@temp-folder"))

    # retrieve the source and target RDBMS engines
    from_rdbms: str = scheme.get("from-rdbms")
    to_rdbms: str = scheme.get("to-rdbms")

    # assert the connection parameters for origin and destination RDBMS engines
    assert_connection_params(errors, from_rdbms)
    assert_connection_params(errors, to_rdbms)

    # verify if actual connection is possible
    if len(errors) == 0:
        db_assert_connection(errors=errors,
                             engine=from_rdbms)
        db_assert_connection(errors=errors,
                             engine=to_rdbms)


def get_migration_context(scheme: dict) -> dict:

    # obtain the source RDBMS parameters
    from_rdbms: str = scheme.get("from-rdbms")
    from_params = db_get_params(from_rdbms)
    if isinstance(from_params, dict):
        from_params["rdbms"] = from_rdbms

    # obtain the target RDBMS parameters
    to_rdbms: str = scheme.get("to-rdbms")
    to_params = db_get_params(to_rdbms)
    if isinstance(to_params, dict):
        to_params["rdbms"] = to_rdbms

    # build the return data
    result: dict = {
        "configuration": pydb_common.get_migration_params(),
        "from": from_params,
        "to": to_params
    }

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

    rdbms: str = scheme if isinstance(scheme, str) else scheme.get("rdbms")
    result: dict = db_get_params(rdbms)
    if isinstance(result, dict):
        result["rdbms"] = rdbms
    else:
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, rdbms,
                                            "unknown or unconfigured RDBMS engine", "@rdbms"))

    return result


def set_connection_params(errors: list[str],
                          scheme: dict) -> None:

    param: str = scheme.get("db-port")
    db_port: int = int(param) if isinstance(param, str) and param.isnumeric() else None

    if db_port is None or db_port < 1:
        # 119: Invalid value {}: {}
        errors.append(validate_format_error(119, db_port,
                                            "must be positive integer", "@db-port"))
    elif not db_setup(engine=scheme.get("rdbms"),
                      db_name=scheme.get("db-name"),
                      db_host=scheme.get("db-host"),
                      db_port=db_port,
                      db_user=scheme.get("db-user"),
                      db_pwd=scheme.get("db-pwd"),
                      db_client=scheme.get("db-client"),
                      db_driver=scheme.get("db-driver")):
        # 120: Argumento(s) inválido(s), inconsistente(s) ou não fornecido(s)
        errors.append(validate_format_error(120))
