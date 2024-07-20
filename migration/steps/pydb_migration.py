from collections.abc import Iterable
from logging import Logger, INFO, WARNING
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import db_drop_table, db_drop_view
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, Column, Index,
    Constraint, CheckConstraint, ForeignKey, ForeignKeyConstraint, inspect
)
from sqlalchemy.sql.elements import Type
from sys import exc_info
from typing import Any

from migration.pydb_common import log
from migration.pydb_types import migrate_column, establish_equivalences
from .pydb_database import schema_create


def prune_metadata(source_schema: str,
                   source_metadata: MetaData,
                   process_indexes: bool,
                   schema_views: list[str],
                   include_relations: list[str],
                   exclude_relations: list[str],
                   exclude_columns: list[str],
                   exclude_constraints: list[str],
                   logger: Logger) -> None:

    # build list of prunable tables
    prunable_tables: set[str] = set([column[:(column + ".").index(".")]
                                     for column in exclude_columns])

    # build list of migration candidates
    source_tables: list[Table] = list(source_metadata.tables.values())

    # traverse list of candidate tables
    for source_table in source_tables:
        table_name: str = source_table.name

        # verify whether relation 'source_table' complies with these conditions for migration:
        #   - relation is not listed in 'exclude_relations' AND
        #   - relation is not listed in 'schema_views' AND
        #   - relation is listed in 'include_relations' OR
        #     - 'include_relations' is empty AND schemas agree
        if (table_name not in exclude_relations and
            table_name not in schema_views and
            (table_name in include_relations or
             (not include_relations and source_table.schema == source_schema))):

            # handle indexes for 'source_table'
            if process_indexes:
                # build list of tainted indexes
                tainted_indexes: list[Index] = []
                for index in source_table.indexes:
                    # 'index' is tainted if:
                    #   - 'index' is listed in 'exclude-relations' OR
                    #   - 'included-relations' is not empty AND 'index' is not listed therein
                    if index.name in exclude_relations or \
                       (include_relations and index.name not in include_relations):
                        tainted_indexes.append(index)
                # remove tainted indexes
                if len(tainted_indexes) == len(source_table.indexes):
                    source_table.indexes.clear()
                else:
                    for tainted_index in tainted_indexes:
                        source_table.indexes.remove(tainted_index)
            else:
                source_table.indexes.clear()

            # prune table
            if source_table.name in prunable_tables:
                excluded_columns: list[Column] = []
                # noinspection PyProtectedMember
                # look for columns to exclude
                for column in source_table._columns:
                    if f"{source_table.name}.{column.name}" in exclude_columns:
                        excluded_columns.append(column)
                # traverse the list of columns to exclude
                for excluded_column in excluded_columns:
                    # noinspection PyProtectedMember
                    # remove the column from table's metadata and log the event
                    source_table._columns.remove(excluded_column)
                    log(logger=logger,
                        level=INFO,
                        msg=(f"Column '{excluded_column.name}' "
                             f"removed from table '{source_table.name}'"))

            # mark these constraints as tainted:
            #   - duplicate CK constraints in table
            #   - constraints listed in 'exclude-constraints'
            table_cks: list[str] = []
            tainted_constraints: list[Constraint] = []
            for constraint in source_table.constraints:
                if constraint.name in table_cks or \
                   constraint.name in exclude_constraints:
                    tainted_constraints.append(constraint)
                elif isinstance(constraint, CheckConstraint):
                    table_cks.append(constraint.name)

            # drop the tainted constraints
            for tainted_constraint in tainted_constraints:
                source_table.constraints.remove(tainted_constraint)
                # FK constraints require special handling
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
                log(logger=logger,
                    level=INFO,
                    msg=(f"Constraint '{tainted_constraint.name}' "
                         f"removed from table '{source_table.name}'"))
        else:
            # 'source_table' is not a table to migrate, remove it from metadata
            source_metadata.remove(table=source_table)


def setup_schema(errors: list[str],
                 target_rdbms: str,
                 target_schema: str,
                 target_engine: Engine,
                 target_tables: list[Table],
                 target_views: list[str],
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
        # yes, drop existing tables and views
        for target_view in target_views:
            table_name: str = f"{target_schema}.{target_view}"
            db_drop_view(errors=errors,
                         view_name=table_name,
                         view_type="M" if target_view in mat_views else "P",
                         engine=target_rdbms,
                         logger=logger)

        # tables must be dropped in reverse order
        for target_table in reversed(target_tables):
            table_name: str = f"{target_schema}.{target_table.name}"
            db_drop_table(errors=errors,
                          table_name=table_name,
                          engine=target_rdbms,
                          logger=logger)
    else:
        # no, create the target schema
        op_errors: list[str] = []
        schema_create(errors=op_errors,
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


def setup_tables(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 source_schema: str,
                 target_schema: str,
                 target_tables: list[Table],
                 override_columns: dict[str, Type],
                 logger: Logger) -> dict[str, Any]:

    # iinitialize the return variable
    result: dict[str, Any] = {}

    # assign the target schema to all migration candidate tables
    # (to all tables at once, before their individual transformations)
    for target_table in target_tables:
        target_table.schema = target_schema

    # establish the migration equivalences
    (native_ordinal, reference_ordinal, nat_equivalences) = \
        establish_equivalences(source_rdbms=source_rdbms,
                               target_rdbms=target_rdbms)
    # setup target tables
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
                      override_columns=override_columns,
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
            log(logger=logger,
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
                  override_columns: dict[str, Type],
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
                                              override_columns=override_columns,
                                              logger=logger)
            # set column's new type
            table_column.type = target_type

            # remove the server default value
            if hasattr(table_column, "server_default"):
                table_column.server_default = None

            # convert the default value - TODO: write a decent default value conversion function
            if hasattr(table_column, "default") and \
               table_column.default in ["sysdate", "systime"]:
                table_column.default = None
        except Exception as e:
            exc_err = str_sanitize(exc_format(exc=e,
                                              exc_info=exc_info()))
            # 102: Unexpected error: {}
            errors.append(validate_format_error(102,
                                                exc_err))
