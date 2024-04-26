import sqlalchemy

import pydb_oracle
import pydb_postgres
import pydb_sqlserver
from .pydb_common import validate_rdbms


def set_connection_params(errors: list[str],
                          scheme: dict,
                          mandatory: bool) -> None:

    rdbms: str = validate_rdbms(errors, scheme, "rdbms")

    # configure the engine
    if len(errors) == 0:
        match rdbms:
            case "oracle":
                pydb_oracle.set_connection_params(errors, scheme, mandatory)
            case "postgres":
                pydb_postgres.set_connection_params(errors, scheme, mandatory)
            case "sqlserver":
                pydb_sqlserver.set_connection_params(errors, scheme, mandatory)


def build_engine(errors: list[str],
                 rdbms: str) -> sqlalchemy.engine:
    # initialize return variable
    result: sqlalchemy.engine = None

    match rdbms:
        case "oracle":
            result = pydb_oracle.build_engine(errors)
        case "postgres":
            result = pydb_postgres.build_engine(errors)
        case "sqlserver":
            result = pydb_sqlserver.build_engine(errors)

    return result


def migrate_table(errors: list[str],
                  source_engine: sqlalchemy.engine,
                  target_engine: sqlalchemy.engine,
                  table: str) -> None:
    pass


def migrate_tables(errors: list[str],
                   source_rdbms: str,
                   target_rdbms: str,
                   tables: list[str]) -> None:

    # obtem the engines
    source_engine: sqlalchemy.engine = build_engine(errors, source_rdbms)
    target_engine: sqlalchemy.engine = build_engine(errors, target_rdbms)

    if source_engine and target_engine:
        for table in tables:
            migrate_table(errors, source_engine, target_engine, table)

