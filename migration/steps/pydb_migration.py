from collections.abc import Iterable
from logging import Logger
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import DbEngine, db_drop_table, db_drop_view
from pypomes_s3 import S3Engine
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, Column, Index,
    Constraint, CheckConstraint, ForeignKey, ForeignKeyConstraint, inspect
)
from sqlalchemy.sql.elements import Type
from sys import exc_info
from typing import Any

from migration.pydb_database import schema_create
from migration.pydb_types import LOBS, is_lob_column, migrate_column


def prune_metadata(source_schema: str,
                   source_metadata: MetaData,
                   process_indexes: bool,
                   schema_views: list[str],
                   include_relations: list[str],
                   exclude_relations: list[str],
                   exclude_columns: list[str],
                   exclude_constraints: list[str],
                   step_metadata: bool,
                   logger: Logger) -> None:

    # build list of prunable tables
    prunable_tables: set[str] = {column[:(column + ".").index(".")]
                                 for column in exclude_columns}

    # build list of migration candidates
    source_tables: list[Table] = list(source_metadata.tables.values())

    # traverse list of candidate tables
    for source_table in source_tables:
        table_name: str = source_table.name

        # verify whether relation 'source_table' complies with these conditions for migration:
        #   - relation is not listed in 'exclude_relations' AND
        #   - relation is not listed in 'schema_views' AND
        #   - relation is listed in 'include_relations' OR
        #   - 'include_relations' is empty AND schemas agree
        if (table_name not in exclude_relations and
            table_name not in schema_views and
            (table_name in include_relations or
             (not include_relations and source_table.schema == source_schema))):

            # prune table
            if source_table.name in prunable_tables:
                # look for columns to exclude
                # noinspection PyProtectedMember
                # ruff: noqa: SLF001 (checks for accesses on "private" class members)
                excluded_columns: list[Column] = [column for column in source_table._columns
                                                  if f"{source_table.name}.{column.name}" in exclude_columns]
                # traverse the list of columns to exclude
                for excluded_column in excluded_columns:
                    # remove the column from table's metadata and log the event
                    # noinspection PyProtectedMember
                    # ruff: noqa: SLF001 (checks for accesses on "private" class members)
                    source_table._columns.remove(excluded_column)
                    logger.info(msg=f"Column '{excluded_column.name}' "
                                    f"removed from table '{source_table.name}'")
            if not step_metadata:
                # nothing else to do here for 'table_name', as metadata are not being migrated
                continue

            # handle indexes for 'source_table'
            if process_indexes:
                # build list of tainted indexes - 'index' is tainted if:
                #   - 'index' is listed in 'exclude_relations' OR
                #   - 'included_relations' is not empty AND 'index' is not listed therein
                tainted_indexes: list[Index] = [index for index in source_table.indexes
                                                if index.name in exclude_relations or
                                                (include_relations and index.name not in include_relations)]
                # remove tainted indexes
                if len(tainted_indexes) == len(source_table.indexes):
                    source_table.indexes.clear()
                else:
                    for tainted_index in tainted_indexes:
                        source_table.indexes.remove(tainted_index)
            else:
                source_table.indexes.clear()

            # mark these constraints as tainted:
            #   - duplicate CK constraints in table
            #     (prevent error 'check constraint already exists')
            #   - constraints listed in 'exclude_constraints'
            table_cks: list[str] = []
            tainted_constraints: list[Constraint] = []
            for constraint in source_table.constraints:
                if constraint.name in table_cks or \
                   constraint.name in exclude_constraints:
                    if constraint not in tainted_constraints:
                        tainted_constraints.append(constraint)
                elif isinstance(constraint, CheckConstraint):
                    table_cks.append(constraint.name)

            # drop the tainted constraints
            for tainted_constraint in tainted_constraints:
                source_table.constraints.remove(tainted_constraint)
                # FK constraints require special handling
                if isinstance(tainted_constraint, ForeignKeyConstraint):
                    # directly removing a foreign key is not available in SqlAlchemy:
                    #   - after being removed from 'source_table.constraints', it reappears
                    #   - nullifying its 'constraint' attribute has the desired effect
                    #   - removing it from 'column.foreign_keys' prevents 'column'
                    #     from being flagged later as having a 'foreign-key' feature
                    foreign_key: ForeignKey | None = None
                    # noinspection PyProtectedMember
                    # ruff: noqa: SLF001 (checks for accesses on "private" class members)
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
                logger.info(msg=f"Constraint '{tainted_constraint.name}' "
                                f"removed from table '{source_table.name}'")
        else:
            # 'source_table' is not a table to migrate, remove it from metadata
            source_metadata.remove(table=source_table)


def setup_schema(target_rdbms: DbEngine,
                 target_schema: str,
                 target_engine: Engine,
                 target_tables: list[Table],
                 target_views: list[str],
                 mat_views: list[str],
                 errors: list[str],
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
            db_drop_view(view_name=table_name,
                         view_type="M" if target_view in mat_views else "P",
                         engine=target_rdbms,
                         errors=errors,
                         logger=logger)

        # tables must be dropped in reverse order
        for target_table in reversed(target_tables):
            table_name: str = f"{target_schema}.{target_table.name}"
            db_drop_table(table_name=table_name,
                          engine=target_rdbms,
                          errors=errors,
                          logger=logger)
    else:
        # no, create the target schema
        op_errors: list[str] = []
        schema_create(schema=target_schema,
                      rdbms=target_rdbms,
                      errors=op_errors,
                      logger=logger)
        # SANITY CHECK: errorless schema creation failure might happen
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


def setup_tables(source_rdbms: DbEngine,
                 target_rdbms: DbEngine,
                 source_schema: str,
                 target_schema: str,
                 target_s3: S3Engine,
                 target_tables: list[Table],
                 override_columns: dict[str, Type],
                 step_metadata: bool,
                 migration_warnings: list[str],
                 errors: list[str],
                 logger: Logger) -> dict[str, Any]:

    # iinitialize the return variable
    result: dict[str, Any] = {}

    # assign the target schema to all migration candidate tables
    # (to all tables at once, before their individual transformations)
    for target_table in target_tables:
        target_table.schema = target_schema

    # setup target tables
    for target_table in target_tables:

        # initialize the local errors list
        op_errors: list[str] = []
        # build the list of migrated columns for this table
        table_columns: dict = {}
        # noinspection PyProtectedMember
        # ruff: noqa: SLF001 (checks for accesses on "private" class members)
        columns: Iterable[Column] = target_table._columns

        # register the source column types and prepare for S3 migration
        s3_columns: list[Column] = []
        for column in columns:
            column_type: str = str(column.type)
            table_columns[column.name] = {
                "source-type": column_type
            }
            # mark LOB column for S3 migration
            if target_s3 and is_lob_column(col_type=column_type):
                s3_columns.append(column)
                table_columns[column.name]["target-type"] = target_s3.value

        # remove the S3-targeted LOB columns
        for s3_column in s3_columns:
            # noinspection PyProtectedMember
            # ruff: noqa: SLF001 (checks for accesses on "private" class members)
            target_table._columns.remove(s3_column)

        # migrate the columns
        if step_metadata:
            setup_columns(target_columns=columns,
                          source_rdbms=source_rdbms,
                          target_rdbms=target_rdbms,
                          override_columns=override_columns,
                          migration_warnings=migration_warnings,
                          errors=op_errors,
                          logger=logger)
            errors.extend(op_errors)

        # register the target column properties
        for column in columns:
            table_columns[column.name]["target-type"] = str(column.type)
            features: list[str] = []
            if hasattr(column, "identity") and column.identity:
                if "identity" in features:
                    err_msg: str = (f"Table {source_rdbms}.{source_schema}.{target_table.name} "
                                    "has more than one identity column")
                    logger.error(msg=err_msg)
                    # 102: Unexpected error: {}
                    op_errors.append(validate_format_error(102,
                                                           err_msg))
                else:
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
        if not op_errors:
            migrated_table: dict = {
                "columns": table_columns,
                "plain-count": 0,
                "plain-duration": "0h0m0s",
                "plain-status": "none",
                "lob-count": 0,
                "lob-bytes": 0,
                "lob-duration": "0h0m0s",
                "lob-status": "none"
            }
            result[target_table.name] = migrated_table

    return result


def setup_columns(target_columns: Iterable[Column],
                  source_rdbms: DbEngine,
                  target_rdbms: DbEngine,
                  override_columns: dict[str, Type],
                  migration_warnings: list[str],
                  errors: list[str],
                  logger: Logger) -> None:

    # set the target columns
    for target_column in target_columns:
        try:
            # convert the type
            target_type: Any = migrate_column(source_rdbms=source_rdbms,
                                              target_rdbms=target_rdbms,
                                              ref_column=target_column,
                                              override_columns=override_columns,
                                              migration_warnings=migration_warnings,
                                              errors=errors,
                                              logger=logger)
            if not errors:
                # set column's new type
                target_column.type = target_type
                # adjust column's nullability
                if hasattr(target_column, "nullable") and target_column.type in LOBS:
                    target_column.nullable = True

                # remove the server default value
                if hasattr(target_column, "server_default"):
                    target_column.server_default = None

                # convert the default value - TODO: write a decent default value conversion function
                if hasattr(target_column, "default") and \
                   target_column.default in ["sysdate", "systime"]:
                    target_column.default = None
        except Exception as e:
            exc_err = str_sanitize(source=exc_format(exc=e,
                                                     exc_info=exc_info()))
            # 102: Unexpected error: {}
            errors.append(validate_format_error(102,
                                                exc_err))
