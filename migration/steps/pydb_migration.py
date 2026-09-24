import re
from collections.abc import Iterable
from logging import Logger
from pypomes_core import (
    str_as_list, str_find_char, str_is_int, str_is_float,
    exc_format, str_sanitize, validate_format_error
)
from pypomes_db import (
    DbEngine, db_drop_table, db_drop_view, db_convert_default
)
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, Column, Index, Constraint,
    CheckConstraint, ForeignKey, ForeignKeyConstraint, DefaultClause, TextClause,
    inspect, text
)
from sqlalchemy.sql.elements import Type
from sys import exc_info
from typing import Any

from migration.pydb_database import schema_create
from migration.pydb_types import is_lob_column, migrate_column, name_to_type
from entities.migration import Migration, MigStep
from entities.migration_table import MigrationTable
from entities.session import Session


def prune_metadata(migration: Migration,
                   session: Session,
                   source_metadata: MetaData,
                   schema_views: list[str],
                   logger: Logger) -> None:

    # build list of prunable tables
    migration_tables: list[MigrationTable] = migration.get_migration_tables() or []
    prunable_tables: list[MigrationTable] = [t for t in migration_tables if t.ds_exclude_columns]

    # build list of migration candidates
    source_tables: list[Table] = list(source_metadata.tables.values())

    # traverse list of candidate tables
    for source_table in source_tables:
        table_name: str = source_table.name

        # verify whether relation 'source_table' complies with these conditions for migration:
        #   - relation is not listed in 'schema_views' AND
        #   - schemas agree AND
        #   - relation is asserted in 'assert_relation()'
        if (table_name not in schema_views and
            source_table.schema == session.nm_source_schema and
            assert_relation(migration=migration,
                            relation=table_name)):
            # prune table
            prunable_table: MigrationTable = next((t for t in prunable_tables if t.nm_table == table_name), None)
            if prunable_table:
                # look for columns to exclude
                # noinspection PyProtectedMember
                # ruff: noqa: SLF001 (checks for accesses on "private" class members)
                excluded_columns: list[Column] = [column for column in source_table._columns
                                                  if column.name in (prunable_table.ds_exclude_columns or "")]
                # traverse the list of columns to exclude
                for excluded_column in excluded_columns:
                    # remove the column from table's metadata and log the event
                    # noinspection PyProtectedMember
                    # ruff: noqa: SLF001 (checks for accesses on "private" class members)
                    source_table._columns.remove(excluded_column)
                    logger.info(msg=f"Column '{excluded_column.name}' "
                                    f"removed from table '{source_table.name}'")
            if migration.cd_step != MigStep.MIGRATE_METADATA:
                # nothing else to do here for 'table_name', as metadata are not being migrated
                continue

            # handle indexes for 'source_table'
            if migration.is_process_indexes:
                # build list of tainted indexes - 'index' is tainted if not asserted
                #   - 'index' is listed in 'exclude_relations' OR
                #   - 'included_relations' is not empty AND 'index' is not listed therein
                tainted_indexes: list[Index] = [index for index in source_table.indexes
                                                if not assert_relation(migration=migration,
                                                                       relation=index.name)]
                # remove tainted indexes
                if len(tainted_indexes) == len(source_table.indexes):
                    source_table.indexes.clear()
                else:
                    for tainted_index in tainted_indexes:
                        source_table.indexes.remove(tainted_index)
            else:
                source_table.indexes.clear()

            if prunable_table:
                # mark these constraints as tainted:
                #   - duplicate CK constraints in table
                #     (prevent error 'check constraint already exists')
                #   - constraints listed in 'exclude_constraints'
                table_cks: list[str] = []
                tainted_constraints: list[Constraint] = []
                for constraint in source_table.constraints:
                    if constraint.name in table_cks or \
                       constraint.name in (prunable_table.ds_exclude_constraints or ""):
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


def setup_schema(target_db: DbEngine | str,
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
                         engine=target_db,
                         errors=errors)

        # tables must be dropped in reverse order
        for target_table in reversed(target_tables):
            table_name: str = f"{target_schema}.{target_table.name}"
            db_drop_table(table_name=table_name,
                          engine=target_db,
                          errors=errors)
    else:
        # no, create the target schema
        curr_errors: list[str] = []
        schema_create(schema=target_schema,
                      db_engine=target_db,
                      errors=curr_errors,
                      logger=logger)
        # SANITY CHECK: errorless schema creation failure might happen
        if curr_errors:
            errors.extend(curr_errors)
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


def setup_tables(migration: Migration,
                 session: Session,
                 target_tables: list[Table],
                 migration_warnings: list[str],
                 errors: list[str],
                 logger: Logger) -> dict[str, Any]:

    # iinitialize the return variable
    result: dict[str, Any] = {}

    # assign the target schema to all migration candidate tables
    # (to all tables at once, before their individual transformations)
    for target_table in target_tables:
        target_table.schema = session.nm_target_schema

    # setup target tables
    for target_table in target_tables:
        # obtain the corresponding MigrationTable instance
        migration_table: MigrationTable = \
            next((t for t in (migration.get_migration_tables() or []) if t.nm_table == target_table.name), None)
        # initialize the local errors list
        curr_errors: list[str] = []
        # build the list of migrated columns for this table
        table_display: dict[str, Any] = {}
        # noinspection PyProtectedMember
        # ruff: noqa: SLF001 (checks for accesses on "private" class members)
        columns: Iterable[Column] = target_table._columns

        # register the source column types and prepare for S3 migration
        s3_columns: list[Column] = []
        for column in columns:
            column_type: str = str(column.type)
            table_display[column.name] = {
                "source-type": column_type
            }
            # mark LOB column for S3 migration
            if session.id_target_s3 and is_lob_column(col_type=column_type):
                s3_columns.append(column)
                table_display[column.name]["target-type"] = session.get_target_s3().cd_type

        # remove the S3-targeted LOB columns
        for s3_column in s3_columns:
            # noinspection PyProtectedMember
            # ruff: noqa: SLF001 (checks for accesses on "private" class members)
            target_table._columns.remove(s3_column)

        # migrate the columns
        if migration.cd_step == MigStep.MIGRATE_METADATA:
            setup_columns(migration=migration,
                          session=session,
                          target_columns=columns,
                          migration_table=migration_table,
                          migration_warnings=migration_warnings,
                          table_display=table_display,
                          errors=curr_errors,
                          logger=logger)
        if not curr_errors:
            # register the target column properties
            for column in columns:
                features: list[str] = []
                if hasattr(column, "identity") and column.identity:
                    if "identity" in features:
                        err_msg: str = (f"Table {session.get_source_db().cd_engine}."
                                        f"{session.nm_source_schema}.{target_table.name} "
                                        "has more than one identity column")
                        logger.error(msg=err_msg)
                        # 102: Unexpected error: {}
                        curr_errors.append(validate_format_error(102,
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
                    table_display[column.name]["features"] = features

        # register the migrated table
        if curr_errors:
            errors.extend(curr_errors)
        else:
            migrated_table: dict = {
                "columns": table_display,
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


def setup_columns(migration: Migration,
                  session: Session,
                  target_columns: Iterable[Column],
                  migration_table: MigrationTable | None,
                  migration_warnings: list[str],
                  table_display: dict[str, Any],
                  errors: list[str],
                  logger: Logger) -> None:

    # set the target columns
    override_columns: list[str] = str_as_list(migration_table.ds_override_columns)
    for target_column in target_columns:
        try:
            # convert the type
            override_type: Type | None = None
            for override_column in override_columns:
                if override_column.startswith(target_column.name + "="):
                    override_type = name_to_type(type_name=override_column[override_column.index("=")+1:],
                                                 db_type=session.get_target_db().cd_type)
                    break
            target_type: Any = migrate_column(migration=migration,
                                              session=session,
                                              ref_column=target_column,
                                              override_type=override_type,
                                              migration_warnings=migration_warnings,
                                              fk_stack=[],
                                              errors=errors,
                                              logger=logger)
            if errors:
                break

            # set column's new type
            target_column.type = target_type
            table_display[target_column.name]["target-type"] = str(target_column.type)
            column_name: str = f"{target_column.table.name}.{target_column.name}"
            logger.debug(msg=f"Rdbms {session.get_target_db().cd_type}, type {target_column.type} "
                             f"in {column_name} converted to {target_type}")

            # set LOB column's nullability
            if hasattr(target_column, "nullable") and \
               is_lob_column(col_type=str(target_column.type)):
                target_column.nullable = True

            # convert column's default value
            if hasattr(target_column, "server_default") and target_column.server_default is not None:
                if column_name in migration_table.ds_omit_defaults:
                    target_column.server_default = None
                elif isinstance(target_column.server_default, DefaultClause):
                    def_orig: Any = target_column.server_default.arg
                    def_save: str = def_orig.text \
                        if isinstance(def_orig, TextClause) else str(def_orig)
                    def_val: str = def_save.strip()
                    if def_val.lower() == "null":
                        # default 'null' must be set as column property
                        target_column.server_default = None
                    else:
                        # remove control chars in default values (known bug in some older DB engines)
                        if any(ord(ch) < 32 for ch in def_val):
                            def_val = "".join(ch if ord(ch) > 31 else "" for ch in def_val)
                        def_conv: str = db_convert_default(value=def_val,
                                                           source_engine=session.get_source_db().cd_engine,
                                                           target_engine=session.get_target_db().cd_engine)
                        if def_conv:
                            if def_conv != def_save:
                                target_column.server_default = DefaultClause(arg=text(text=def_conv))
                            if def_conv.startswith("'") and def_conv.endswith("'"):
                                def_conv = def_val[1:-1]
                            elif str_is_int(def_conv):
                                def_conv: int = int(def_conv)
                            elif str_is_float(def_conv):
                                def_conv: float = float(def_conv)
                            table_display[target_column.name]["default-value"] = def_conv
                        else:
                            warn_msg: str = ("Unable to convert the default value "
                                             f"'{def_val}' for column {column_name}")
                            migration_warnings.append(warn_msg)
                            logger.warning(msg=warn_msg)
                            target_column.server_default = None
        except Exception as e:
            exc_err = str_sanitize(exc_format(exc=e,
                                              exc_info=exc_info()))
            # 102: Unexpected error: {}
            errors.append(validate_format_error(102,
                                                exc_err))


def assert_relation(migration: Migration,
                    relation: str) -> bool:

    # initialize the return variable
    result: bool = True

    # process list of excludes
    excludes: list[str] = str_as_list(migration.ds_exclude_relations)
    if excludes and relation not in excludes:
        for exclude in excludes:
            if (str_find_char(exclude, ".^*+?[]()|\\{}") >= 0 and
                re.search(pattern=exclude.replace("$", "\\$"),
                          string=relation)):
                result = False
                break

    # process list of includes
    includes: list[str] = str_as_list(migration.ds_include_relations)
    if result and includes:
        # relation was not excluded, so process list of includes
        result = relation in includes
        if not result:
            for include in includes:
                if (str_find_char(include, ".^*+?[]()|\\{}") >= 0 and
                    re.search(pattern=include.replace("$", "\\$"),
                              string=relation)):
                    result = True
                    break
    return result
