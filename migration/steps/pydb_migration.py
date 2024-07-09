import sys
from collections.abc import Iterable
from logging import Logger, INFO, WARNING
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import (
    db_get_view_script, db_execute, db_drop_table, db_drop_view
)
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, Column, ForeignKey,
    Constraint, CheckConstraint, ForeignKeyConstraint, inspect
)
from sqlalchemy.sql.elements import Type
from typing import Any, Literal

from migration import pydb_common
from migration.pydb_types import migrate_column, establish_equivalences
from .pydb_database import create_schema


def prune_metadata(source_schema: str,
                   source_metadata: MetaData,
                   plain_views: list[str],
                   mat_views: list[str],
                   include_tables: list[str],
                   exclude_tables: list[str],
                   include_views: list[str],
                   exclude_columns: list[str],
                   exclude_constraints: list[str],
                   process_indexes: bool,
                   logger: Logger) -> None:

    # build list of prunable tables
    prunable_tables: set[str] = set([column[:(column + ".").index(".")]
                                     for column in exclude_columns])

    # build list of migration candidates
    source_tables: list[Table] = list(source_metadata.tables.values())
    target_tables: list[Table] = []

    # traverse list of candidate tables
    for source_table in source_tables:
        table_name: str = source_table.name.lower()

        # verify whether 'source_table' is to migrate
        if (table_name not in exclude_tables and
            (table_name in include_tables or
             table_name in include_views or
             (not include_tables and source_table.schema == source_schema and
              table_name not in plain_views and table_name not in mat_views))):
            # yes, proceed
            target_tables.append(source_table)

            # remove indexes, if applicable
            if not process_indexes:
                source_table.indexes.clear()

            # prune table, if applicable
            if source_table.name in prunable_tables:
                excluded_columns: list[Column] = []
                # noinspection PyProtectedMember
                # look for columns to exclude
                for column in source_table._columns:
                    if f"{source_table.name}.{column.name}" in exclude_columns:
                        excluded_columns.append(column)
                # traverse the list of columns to exclude, if any
                for excluded_column in excluded_columns:
                    # noinspection PyProtectedMember
                    # remove the column from table's metadata and log the event
                    source_table._columns.remove(excluded_column)
                    pydb_common.log(logger=logger,
                                    level=INFO,
                                    msg=(f"Column '{excluded_column.name}' "
                                         f"removed from table '{source_table.name}'"))

            # mark constraints as tainted:
            #   - duplicate CK constraints in table
            #   - constraints listed in 'exclude-constraints'
            table_constraints: list[str] = []
            tainted_constraints: list[Constraint] = []
            for constraint in source_table.constraints:
                if constraint.name in exclude_constraints or \
                   constraint.name in table_constraints:
                    tainted_constraints.append(constraint)
                elif isinstance(constraint, CheckConstraint):
                    table_constraints.append(constraint.name)

            # drop the tainted constraints
            for tainted_constraint in tainted_constraints:
                source_table.constraints.remove(tainted_constraint)
                if isinstance(tainted_constraint, ForeignKeyConstraint):
                    # directly removing a foreign key is not available in SqlAlchemy:
                    # - after being removed from 'source_table.constraints', it reappears
                    # - nullifying its 'constraint' attribute has the desired effect
                    # - removing it from 'column.foreign_keys' prevents 'column'
                    #   from being flagged later as having a 'foreign-key' feature
                    foreign_key: ForeignKey | None = None
                    # noinspection PyProtectedMember
                    for column in source_table._columns:
                        for fk in column.foreign_keys:
                            if fk.name == tainted_constraint.name:
                                foreign_key = fk
                                break
                        if foreign_key:
                            foreign_key.constraint = None
                            column.foreign_keys.remove(foreign_key)
                            break

                # log the constraint removal
                pydb_common.log(logger=logger,
                                level=INFO,
                                msg=(f"Constraint '{tainted_constraint.name}' "
                                     f"removed from table '{source_table.name}'"))
        else:
            # 'source_table' is not to migrate, remove it from metadata
            source_metadata.remove(table=source_table)


def migrate_schema(errors: list[str],
                   target_rdbms: str,
                   target_schema: str,
                   target_engine: Engine,
                   target_tables: list[Table],
                   plain_views: list[str],
                   mat_views: list[str],
                   logger: Logger) -> str:

    # initialize the return variable
    result: str | None = None

    # create an inspector into the target RDBMS
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
            if target_table.name in plain_views or \
               target_table.name in mat_views:
                db_drop_view(errors=errors,
                             view_name=full_name,
                             view_type="M" if target_table.name in mat_views else "P",
                             engine=target_rdbms,
                             logger=logger)
            else:
                db_drop_table(errors=errors,
                              table_name=full_name,
                              engine=target_rdbms,
                              logger=logger)
    else:
        # no, create the target schema
        op_errors: list[str] = []
        create_schema(errors=op_errors,
                      schema=target_schema,
                      rdbms=target_rdbms,
                      logger=logger)
        # SANITY CHECK: errorless schema creation failure has happened
        if op_errors:
            errors.extend(op_errors)
        else:
            # refresh the target RDBMS inspector
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
                   target_schema: str,
                   target_tables: list[Table],
                   external_columns: dict[str, Type],
                   logger: Logger) -> dict:

    # iinitialize the return variable
    result: dict = {}

    # assign the target schema to all migration candidate tables
    # (to all tables at once, before their individual transformations)
    for target_table in target_tables:
        target_table.schema = target_schema

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
        columns: Iterable[Column] = target_table._columns

        # register the source column types
        for column in columns:
            table_columns[column.name] = {
                "source-type": str(column.type)
            }
        # migrate the columns
        setup_columns(errors=errors,
                      table_columns=columns,
                      source_rdbms=source_rdbms,
                      target_rdbms=target_rdbms,
                      source_schema=source_schema,
                      target_schema=target_schema,
                      native_ordinal=native_ordinal,
                      reference_ordinal=reference_ordinal,
                      nat_equivalences=nat_equivalences,
                      external_columns=external_columns,
                      logger=logger)

        # register the target column properties
        for column in columns:
            table_columns[column.name]["target-type"] = str(column.type)
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
            if "primary-key" in column_data.get("features", []):
                no_pk = False
                break
        if no_pk:
            pydb_common.log(logger=logger,
                            level=WARNING,
                            msg=(f"RDBMS {source_rdbms}, "
                                 f"table {source_schema}.{target_table}, "
                                 f"no primary key column found"))
    return result


def setup_columns(errors: list[str],
                  table_columns: Iterable[Column],
                  source_rdbms: str,
                  target_rdbms: str,
                  source_schema: str,
                  target_schema: str,
                  native_ordinal: int,
                  reference_ordinal: int,
                  nat_equivalences: list[tuple],
                  external_columns: dict[str, Type],
                  logger: Logger) -> None:

    # set the target columns
    for table_column in table_columns:
        try:
            # convert the type
            target_type: Any = migrate_column(source_rdbms=source_rdbms,
                                              target_rdbms=target_rdbms,
                                              source_schema=source_schema,
                                              target_schema=target_schema,
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

    # initialize the local error messages list
    op_errors: list[str] = []

    # obtain the script used to create the view
    view_script: str = db_get_view_script(errors=op_errors,
                                          view_type=view_type,
                                          view_name=f"{source_schema}.{view_name}",
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
        db_execute(errors=op_errors,
                   exc_stmt=view_script,
                   engine=target_rdbms)
        # errors ?
        if op_errors:
            # yes, insert a leading explanatory error message
            err_msg: str = f"FAILED: '{str_sanitize(view_script)}'. REASON: {op_errors[-1]}"
            # 101: {}
            op_errors[-1] = validate_format_error(101, err_msg)
    else:
        # no, report the problem
        # 102: Unexpected error: {}
        op_errors.append(validate_format_error(102,
                                               "unable to retrieve creation script "
                                               f"for view '{source_rdbms}.{source_schema}.{view_name}'"))
    # register local errors
    errors.extend(op_errors)
