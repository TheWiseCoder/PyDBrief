import sys
from logging import Logger, WARNING
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import db_has_table
from sqlalchemy import Engine, Inspector, Table, Column, Constraint, inspect
from sqlalchemy.sql.elements import Type
from typing import Any

from migration import pydb_validator, pydb_types, pydb_common
from .pydb_engine import excecute_stmt
from .pydb_database import get_view_dependencies


def migrate_schema(errors: list[str],
                   target_rdbms: str,
                   target_schema: str,
                   target_engine: Engine,
                   target_tables: list[Table],
                   schema_views: list[str],
                   logger: Logger) -> str:

    # initialize the return variable
    result: str | None = None

    # create an inspector into the target engine
    target_inspector: Inspector = inspect(subject=target_engine,
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
                if target_table.name.lower() in schema_views:
                    drop_stmt: str = (f"IF OBJECT_ID({table_name}, 'U') "
                                      f"IS NOT NULL DROP VIEW {table_name};")
                else:
                    drop_stmt: str = (f"IF OBJECT_ID({table_name}, 'U') "
                                      f"IS NOT NULL DROP TABLE {table_name} CASCADE CONSTRAINTS;")
            elif target_table.name.lower() in schema_views:
                drop_stmt: str = f"DROP VIEW IF EXISTS {table_name}"
            else:
                drop_stmt: str = f"DROP TABLE IF EXISTS {table_name} CASCADE"
            excecute_stmt(errors=errors,
                          rdbms=target_rdbms,
                          engine=target_engine,
                          stmt=drop_stmt,
                          logger=logger)
    else:
        # no, create the target schema
        if target_rdbms == "oracle":
            stmt: str = f"CREATE USER {target_schema} IDENTIFIED BY {target_schema}"
        else:
            conn_params: dict = pydb_validator.get_connection_params(errors=errors,
                                                                     scheme=target_rdbms)
            stmt = f"CREATE SCHEMA {target_schema} AUTHORIZATION {conn_params.get('user')}"
        excecute_stmt(errors=errors,
                      rdbms=target_rdbms,
                      engine=target_engine,
                      stmt=stmt,
                      logger=logger)

        # SANITY CHECK: it has happened that a schema creation failed, with no errors reported
        if not errors:
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
                   target_tables: list[Table],
                   schema_views: list[str],
                   external_columns: dict[str, Type],
                   logger: Logger) -> dict:

    # iinitialize the return variable
    result: dict = {}

    # establish the migration equivalences
    (native_ordinal, reference_ordinal, nat_equivalences) = \
        pydb_types.establish_equivalences(source_rdbms=source_rdbms,
                                          target_rdbms=target_rdbms)

    # setup target tables
    is_view: bool
    for target_table in target_tables:
        # determine if table is a view
        is_view: bool = target_table.name.lower() in schema_views

        # build the list of migrated columns for this table
        table_columns: dict = {}
        # noinspection PyProtectedMember
        columns: list[Column] = target_table.c._all_columns
        # register the source column types
        for column in columns:
            table_columns[column.name] = {
                "source-type": str(column.type)
            }
        # migrate the columns
        setup_table_columns(errors=errors,
                            table_columns=columns,
                            source_rdbms=source_rdbms,
                            target_rdbms=target_rdbms,
                            native_ordinal=native_ordinal,
                            reference_ordinal=reference_ordinal,
                            nat_equivalences=nat_equivalences,
                            external_columns=external_columns,
                            logger=logger)
        # make sure table does not have duplicate constraints
        constraint_names: list[str] = []
        excess_constraints: list[Constraint] = []
        for constraint in target_table.constraints:
            if constraint.name in constraint_names:
                excess_constraints.append(constraint)
            else:
                constraint_names.append(constraint.name)
        for excess_constraint in excess_constraints:
            target_table.constraints.remove(excess_constraint)

        # register the target column properties
        for column in columns:
            features: list[str] = []
            if hasattr(column, "identity") and column.identity:
                features.append("identity")
            if hasattr(column, "primary_key") and column.primary_key:
                features.append("primary key")
            if (hasattr(column, "foreign_keys") and
               isinstance(column.foreign_keys, set) and
               len(column.foreign_keys) > 0):
                features.append("foreign key")
            if hasattr(column, "unique") and column.unique:
                features.append("unique")
            if hasattr(column, "nullable") and column.nullable:
                features.append("nullable")
            if features:
                table_columns[column.name]["features"] = features
            table_columns[column.name]["target-type"] = str(column.type)

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


def setup_table_columns(errors: list[str],
                        table_columns: list[Column],
                        source_rdbms: str,
                        target_rdbms: str,
                        native_ordinal: int,
                        reference_ordinal: int,
                        nat_equivalences: list[tuple],
                        external_columns: dict[str, Type],
                        logger: Logger) -> None:

    # set the target columns
    for table_column in table_columns:
        try:
            # convert the type
            target_type: Any = pydb_types.migrate_column(source_rdbms=source_rdbms,
                                                         target_rdbms=target_rdbms,
                                                         native_ordinal=native_ordinal,
                                                         reference_ordinal=reference_ordinal,
                                                         source_column=table_column,
                                                         nat_equivalences=nat_equivalences,
                                                         external_columns=external_columns,
                                                         logger=logger)
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


def assert_views(errors: list[str],
                 source_rdbms: str,
                 source_schema: str,
                 target_rdbms: str,
                 target_schema: str,
                 views: list[str],
                 tables: list[str],
                 logger: Logger) -> list[str]:

    # initialize the return variable
    result: list[str] = []

    for view in views:
        op_errors: list[str] = []
        dependencies: list[str] = get_view_dependencies(errors=op_errors,
                                                        rdbms=source_rdbms,
                                                        schema=source_schema,
                                                        view_name=view,
                                                        logger=logger)
        if op_errors:
            errors.extend(op_errors)
        else:
            for dependency in dependencies:
                if dependency not in tables and not \
                   db_has_table(errors=op_errors,
                                table_name=dependency,
                                schema=target_schema,
                                engine=target_rdbms,
                                logger=logger):
                    result.append(view)

    return result
