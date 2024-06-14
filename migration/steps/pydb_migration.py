import sys
from logging import Logger, WARNING
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import db_get_view_script, db_execute
from sqlalchemy import Engine, Inspector, Table, Column, Constraint, inspect
from sqlalchemy.sql.elements import Type
from typing import Any, Literal

from migration import pydb_common
from migration.pydb_types import migrate_table_column, establish_equivalences
from .pydb_database import create_schema, drop_table, drop_view


def migrate_schema(errors: list[str],
                   target_rdbms: str,
                   target_schema: str,
                   target_engine: Engine,
                   target_tables: list[Table],
                   plain_views: list[str],
                   mat_views: list[str],
                   process_views: bool,
                   process_mviews: bool,
                   logger: Logger) -> str:

    # initialize the return variable
    result: str | None = None

    # create an inspector into the target engine
    target_inspector: Inspector = inspect(subject=target_engine,
                                          raiseerr=True)

    # obtain the target schema's internal name
    for schema_name in target_inspector.get_schema_names():
        # is this the target schema ?
        if target_schema == schema_name.lower():
            # yes, use the actual name with its case imprint
            result = schema_name
            break

    # does the target schema already exist ?
    if result:
        # yes, drop existing tables (must be done in reverse order)
        for target_table in reversed(target_tables):
            full_name: str = f"{target_schema}.{target_table.name}"
            if process_views and target_table.name in plain_views or \
               process_mviews and target_table.name in mat_views:
                drop_view(errors=errors,
                          view_name=full_name,
                          view_type="M" if target_table.name in mat_views else "P",
                          rdbms=target_rdbms,
                          logger=logger)
            else:
                drop_table(errors=errors,
                           table_name=full_name,
                           rdbms=target_rdbms,
                           logger=logger)
    else:
        # no, create the target schema
        create_schema(errors=errors,
                      schema=target_schema,
                      rdbms=target_rdbms,
                      logger=logger)
        # SANITY CHECK: it has happened that a schema creation failed, with no errors reported
        if not errors:
            target_inspector = inspect(subject=target_engine,
                                       raiseerr=True)
            for schema_name in target_inspector.get_schema_names():
                # is this the target schema ?
                if target_schema == schema_name.lower():
                    # yes, use the actual name with its case imprint
                    result = schema_name
                    break

    return result


def migrate_tables(errors: list[str],
                   source_rdbms: str,
                   target_rdbms: str,
                   source_schema: str,
                   target_tables: list[Table],
                   external_columns: dict[str, Type],
                   logger: Logger) -> dict:

    # iinitialize the return variable
    result: dict = {}

    # establish the migration equivalences
    (native_ordinal, reference_ordinal, nat_equivalences) = \
        establish_equivalences(source_rdbms=source_rdbms,
                               target_rdbms=target_rdbms)

    # setup target tables
    is_view: bool
    for target_table in target_tables:
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
                features.append("primary-key")
            if (hasattr(column, "foreign_keys") and
               isinstance(column.foreign_keys, set) and
               len(column.foreign_keys) > 0):
                features.append("foreign-key")
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
            target_type: Any = migrate_table_column(source_rdbms=source_rdbms,
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


def migrate_view(errors: list[str],
                 view_name: str,
                 view_type: Literal["M", "P"],
                 source_rdbms: str,
                 source_schema: str,
                 target_rdbms: str,
                 target_schema: str,
                 logger: Logger) -> None:

    # obtain the script used to create the view
    full_name: str = f"{target_schema}.{view_name}"
    view_script: str = db_get_view_script(errors=errors,
                                          view_type=view_type,
                                          view_name=full_name,
                                          engine=source_rdbms,
                                          logger=logger)
    # has the script been retrieved ?
    if view_script:
        # yes, create the view in the target schema
        view_script = view_script.lower().replace(f"{source_schema}.", f"{target_schema}.")\
                                         .replace(f'"{source_schema}".', f'"{target_schema}".')
        if source_rdbms == "oracle":
            # purge Oracle-specific clauses
            view_script = view_script.replace("force editionable ", "")
        db_execute(errors=errors,
                   exc_stmt=view_script,
                   engine=target_rdbms)
        # errors ?
        if errors:
            # yes, insert a leading explanatory error message
            err_msg: str = f"Failed: '{str_sanitize(view_script)}'"
            # 101: {}
            errors.insert(0, validate_format_error(101, err_msg))
    else:
        # no, report the problem
        # 102: Unexpected error: {}
        errors.append(validate_format_error(102,
                                            "unable to retrieve creation script "
                                            f"for view {full_name} in RDBMS {target_rdbms}"))
