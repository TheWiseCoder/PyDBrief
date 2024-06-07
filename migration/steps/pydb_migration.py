import sys
from logging import Logger, WARNING, DEBUG
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import db_get_connection_string, db_execute
from sqlalchemy import (
    Engine, Table, TextClause, Column, Type,
    create_engine, inspect, text, Result, RootTransaction
)
from typing import Any

from migration import pydb_validator, pydb_types, pydb_common


def migrate_schema(errors: list[str],
                   target_rdbms: str,
                   target_schema: str,
                   target_engine: Engine,
                   target_tables: list[Table],
                   logger: Logger) -> str:

    # initialize the return variable
    result: str | None = None

    # create an inspector into the target engine
    target_inspector = inspect(subject=target_engine,
                               raiseerr=True)

    # obtain the target schema's internal name
    for schema_name in target_inspector.get_schema_names():
        # is this the target schema ?
        if target_schema.lower() == schema_name.lower():
            # yes, use the actual name with its case imprint
            result = schema_name
            break

    # does the target schema already exist ?
    if result:
        # yes, drop existing tables (must be done in reverse order)
        for target_table in reversed(target_tables):
            table_name: str = f"{target_schema}.{target_table.name}"
            if target_rdbms == "oracle":
                # oracle has no 'IF EXISTS' clause
                drop_stmt: str = (f"IF OBJECT_ID({table_name}, 'U') "
                                  f"IS NOT NULL DROP TABLE {table_name} CASCADE CONSTRAINTS;")
                db_execute(errors=errors,
                           exc_stmt=drop_stmt,
                           engine="oracle",
                           logger=logger)
            else:
                drop_stmt: str = f"DROP TABLE IF EXISTS {table_name} CASCADE"
                engine_exc_stmt(errors, target_rdbms,
                                target_engine, drop_stmt, logger)
    else:
        # no, create the target schema
        if target_rdbms == "oracle":
            stmt: str = f"CREATE USER {target_schema} IDENTIFIED BY {target_schema}"
        else:
            conn_params: dict = pydb_validator.get_connection_params(errors=errors,
                                                                     scheme=target_rdbms)
            stmt = f"CREATE SCHEMA {target_schema} AUTHORIZATION {conn_params.get('user')}"
        engine_exc_stmt(errors=errors,
                        rdbms=target_rdbms,
                        engine=target_engine,
                        stmt=stmt,
                        logger=logger)

        # SANITY CHECK: it has happened that a schema creation failed, with no errors reported
        if len(errors) == 0:
            target_inspector = inspect(subject=target_engine,
                                       raiseerr=True)
            for schema_name in target_inspector.get_schema_names():
                # is this the target schema ?
                if target_schema.lower() == schema_name.lower():
                    # yes, use the actual name with its case imprint
                    result = schema_name
                    break

    return result


def migrate_tables(errors: list[str],
                   source_rdbms: str,
                   target_rdbms: str,
                   source_schema: str,
                   data_tables: list[str],
                   target_tables: list[Table],
                   foreign_columns: dict[str, Type],
                   logger: Logger) -> dict:

    # iinitialize the return variable
    result: dict = {}

    # establish the migration equivalences
    (native_ordinal, reference_ordinal, nat_equivalences) = \
        pydb_types.establish_equivalences(source_rdbms=source_rdbms,
                                          target_rdbms=target_rdbms)

    # setup target tables
    for target_table in target_tables:

        # proceed, if 'target_table' is a migration candidate
        if not data_tables or target_table in data_tables:

            # build the list of migrated columns for this table
            table_columns: dict = {}
            # noinspection PyProtectedMember
            columns: list[Column] = target_table.c._all_columns
            for column in columns:
                table_columns[column.name] = {
                    "source-type": str(column.type)
                }

            # migrate the columns
            setup_target_table(errors=errors,
                               table_columns=columns,
                               source_rdbms=source_rdbms,
                               target_rdbms=target_rdbms,
                               native_ordinal=native_ordinal,
                               reference_ordinal=reference_ordinal,
                               nat_equivalences=nat_equivalences,
                               foreign_columns=foreign_columns,
                               logger=logger)

            # register the new column properties
            for column in columns:
                features: list[str] = []
                if hasattr(column, "identity") and column.identity:
                    features.append("identity")
                if hasattr(column, "primary_key") and column.primary_key:
                    features.append("primary key")
                if hasattr(column, "unique") and column.unique:
                    features.append("unique")
                if hasattr(column, "nullable") and column.nullable:
                    features.append("nullable")
                table_columns[column.name]["target-type"] = str(column.type)
                table_columns[column.name]["features"] = features

            # register the migrated table
            migrated_table: dict = {
                "columns": table_columns,
                "plain-count": 0,
                "plain-status": "none",
                "lob-count": 0,
                "lob-status": "none"
            }
            result[target_table.name] = migrated_table

            # issue warning if no primary key was found for table
            no_pk: bool = True
            for column_data in table_columns.values():
                if "primary key" in (column_data.get("features") or []):
                    no_pk = False
                    break
            if no_pk:
                pydb_common.log(logger=logger,
                                level=WARNING,
                                msg=(f"RDBMS {source_rdbms}, "
                                     f"table {source_schema}.{target_table}, "
                                     f"no primary key column found"))
    return result


def build_engine(errors: list[str],
                 rdbms: str,
                 logger: Logger) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    # obtain the connection string
    conn_str: str = db_get_connection_string(engine=rdbms)

    # build the engine
    try:
        result = create_engine(url=conn_str)
        pydb_common.log(logger=logger,
                        level=DEBUG,
                        msg=f"RDBMS {rdbms}, created migration engine")
    except Exception as e:
        exc_err = str_sanitize(exc_format(exc=e,
                               exc_info=sys.exc_info()))
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102, exc_err))

    return result


def setup_target_table(errors: list[str],
                       table_columns: list[Column],
                       source_rdbms: str,
                       target_rdbms: str,
                       native_ordinal: int,
                       reference_ordinal: int,
                       nat_equivalences: list[tuple],
                       foreign_columns: dict[str, Type],
                       logger: Logger) -> None:

    # set the target columns
    for table_column in table_columns:
        # convert the type
        target_type: Any = pydb_types.migrate_column(source_rdbms=source_rdbms,
                                                     target_rdbms=target_rdbms,
                                                     native_ordinal=native_ordinal,
                                                     reference_ordinal=reference_ordinal,
                                                     source_column=table_column,
                                                     nat_equivalences=nat_equivalences,
                                                     foreign_columns=foreign_columns,
                                                     logger=logger)

        # wrap-up the column migration
        try:
            # set column's new type
            table_column.type = target_type

            # remove the server default value
            if hasattr(table_column, "server_default"):
                table_column.server_default = None

            # convert the default value - TODO: write a decent default value conversion function
            if hasattr(table_column, "default") and \
               table_column.default is not None and \
               table_column.lower() in ["sysdate", "systime"]:
                table_column.default = None
        except Exception as e:
            exc_err = str_sanitize(exc_format(exc=e,
                                              exc_info=sys.exc_info()))
            # 102: Unexpected error: {}
            errors.append(validate_format_error(102, exc_err))


def engine_exc_stmt(errors: list[str],
                    rdbms: str,
                    engine: Engine,
                    stmt: str,
                    logger: Logger) -> Result:

    result: Result | None = None
    exc_stmt: TextClause = text(stmt)
    try:
        with engine.connect() as conn:
            trans: RootTransaction = conn.begin()
            result = conn.execute(statement=exc_stmt)
            trans.commit()
            pydb_common.log(logger=logger,
                            level=DEBUG,
                            msg=f"RDBMS {rdbms}, sucessfully executed {stmt}")
    except Exception as e:
        exc_err = str_sanitize(exc_format(exc=e,
                                          exc_info=sys.exc_info()))
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102, exc_err))

    return result
