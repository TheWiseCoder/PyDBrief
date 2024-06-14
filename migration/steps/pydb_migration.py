import sys
from logging import Logger, WARNING
from pypomes_core import exc_format, str_sanitize, validate_format_error
from pypomes_db import (
    db_execute, db_get_views, db_get_view_script
)
from sqlalchemy import MetaData, Engine, Inspector, Table, Column, Constraint, inspect
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql.elements import Type
from typing import Any, Literal

from migration import pydb_types, pydb_common
from .pydb_database import create_schema, drop_table, drop_view


def migrate_schema(errors: list[str],
                   target_rdbms: str,
                   target_schema: str,
                   target_engine: Engine,
                   target_tables: list[Table],
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
            drop_table(errors=errors,
                       table_name=f"{target_schema}.{target_table.name}",
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
        pydb_types.establish_equivalences(source_rdbms=source_rdbms,
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


def migrate_schema_views(errors: list[str],
                         source_inspector: Inspector,
                         source_engine: Engine,
                         target_engine: Engine,
                         source_schema: str,
                         target_schema: str,
                         process_views: bool,
                         process_mviews: bool) -> None:

    # obtain the list of plain and materialized views
    source_views: list[str] = []
    if process_views:
        source_views.extend(source_inspector.get_view_names())
    if process_mviews:
        source_views.extend(source_inspector.get_materialized_view_names())

    # obtain the source schema metadata
    source_metadata: MetaData = MetaData(schema=source_schema)
    try:
        source_metadata.reflect(bind=source_engine,
                                schema=source_schema,
                                views=True,
                                only=source_views)
    except SAWarning as e:
        # - unable to fully reflect the schema
        # - this error will cause the migration to be aborted,
        #   as SQLAlchemy will not be able to find the schema tables
        exc_err = str_sanitize(exc_format(exc=e,
                                          exc_info=sys.exc_info()))
        # 104: The operation {} returned the error {}
        errors.append(validate_format_error(104, "views-reflection", exc_err))

    # any errors ?
    if not errors:
        # no, proceed with the appropriate tables
        sorted_tables: list[Table] = []
        try:
            sorted_tables: list[Table] = source_metadata.sorted_tables
        except SAWarning as e:
            # - unable to organize the views in the proper sequence:
            #   probably, cross-dependencies between views
            # - this error will cause the views migration to be aborted,
            #   as SQLAlchemy would not be able to compile the migrated schema
            exc_err = str_sanitize(exc_format(exc=e,
                                              exc_info=sys.exc_info()))
            # 104: The operation {} returned the error {}
            errors.append(validate_format_error(104, "views-migration", exc_err))

        # any errors ?
        if not errors:
            # no, proceed
            for sorted_table in reversed(sorted_tables):
                if sorted_table.schema == source_schema:
                    sorted_table.schema = target_schema
                else:
                    return source_metadata.remove(sorted_table)
            try:
                # migrate the schema
                source_metadata.create_all(bind=target_engine,
                                           checkfirst=False)
            except Exception as e:
                # unable to fully compile the schema
                exc_err = str_sanitize(exc_format(exc=e,
                                                  exc_info=sys.exc_info()))
                # 104: The operation {} returned the error {}
                errors.append(validate_format_error(104, "views-construction", exc_err))


def migrate_schema_vixxs(errors: list[str],
                         source_rdbms: str,
                         source_schema: str,
                         target_rdbms: str,
                         target_schema: str,
                         view_type: Literal["M", "P"],
                         logger: Logger) -> None:

    # obtain the list of views in source schema
    source_views: list[str] = db_get_views(errors=errors,
                                           view_type=view_type,
                                           schema=source_schema,
                                           engine=source_rdbms,
                                           logger=logger) or []
    # traverse the views list
    for source_view in source_views:
        op_errors: list[str] = []
        target_view: str = source_view.lower().replace(source_schema, target_schema, 1)
        # drop view in target schema
        drop_view(errors=op_errors,
                  view_name=target_view,
                  rdbms=target_rdbms,
                  logger=logger)
        # errors ?
        if not op_errors:
            # no, obtain the script used to create the view
            view_script: str = db_get_view_script(errors=op_errors,
                                                  view_type=view_type,
                                                  view_name=source_view,
                                                  engine=source_rdbms,
                                                  logger=logger)
            # errors ?
            if not op_errors:
                # no, create the view in the target schema
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
                    err_msg: str = f"Failed: '{str_sanitize(view_script)}'"
                    # 101: {}
                    op_errors.insert(0, validate_format_error(101, err_msg))
        # register eventual local errors
        errors.extend(op_errors)
