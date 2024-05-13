import sys
import warnings
from datetime import datetime
from logging import DEBUG, INFO, Logger
from pypomes_core import (
    DATETIME_FORMAT_INV, validate_format_error, exc_format
)
from pypomes_db import (
    db_connect, db_execute, db_migrate_data, db_migrate_lobs
)
from sqlalchemy import text  # from 'sqlalchemy._elements._constructors', but invisible
from sqlalchemy.engine.base import Engine, RootTransaction
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.result import Result
from sqlalchemy.exc import SAWarning
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.schema import Column, MetaData, Table
from typing import Any

from . import (
    pydb_common, pydb_types, pydb_validator,
    pydb_oracle, pydb_postgres, pydb_sqlserver  # , pydb_mysql
)

# treat warnings as errors
warnings.filterwarnings("error")


# this is the entry point for the migration process
def migrate(errors: list[str],
            source_rdbms: str,
            target_rdbms: str,
            source_schema: str,
            target_schema: str,
            data_tables: list[str],
            logger: Logger | None) -> dict:

    started: datetime = datetime.now()
    pydb_common.log(logger, INFO,
                    "Started migrating the metadata")
    migrated_tables: list[dict] = migrate_metadata(errors, source_rdbms, target_rdbms,
                                                   source_schema, target_schema, data_tables, logger)
    pydb_common.log(logger, INFO,
                    "Finished migrating the metadata")

    # obtain source and target connections
    op_errors: list[str] = []
    source_conn: Any = db_connect(errors=op_errors,
                                  engine=source_rdbms,
                                  logger=logger)
    target_conn: Any = db_connect(errors=op_errors,
                                  engine=target_rdbms,
                                  logger=logger)

    if source_conn and target_conn:
        # disable target RDBMS restrictions to speed-up bulk copying
        disable_session_restrictions(op_errors, target_rdbms, target_conn, logger)
        if not op_errors:
            # migrate the plain data
            pydb_common.log(logger, INFO,
                            "Started migrating the plain data")
            migrate_plain(op_errors, source_rdbms, target_rdbms, source_schema,
                          target_schema, source_conn, target_conn, migrated_tables, logger)
            errors.extend(op_errors)
            pydb_common.log(logger, INFO,
                            "Finished migrating the plain data")

            # migrate the LOBs
            pydb_common.log(logger, INFO,
                            "Started migrating the LOBs")
            op_errors = []
            migrate_lobs(op_errors, source_rdbms, target_rdbms, source_schema,
                         target_schema, source_conn, target_conn, migrated_tables, logger)
            errors.extend(op_errors)
            pydb_common.log(logger, INFO,
                            "Finished migrating the LOBs")

            # restore target RDBMS restrictions delaying bulk copying
            op_errors = []
            restore_session_restrictions(op_errors, target_rdbms, target_conn, logger)
            errors.extend(op_errors)

        # close source and target connections
        source_conn.close()
        target_conn.close()

    finished: datetime = datetime.now()

    return {
        "started": started.strftime(DATETIME_FORMAT_INV),
        "finished": finished.strftime(DATETIME_FORMAT_INV),
        "source": {
            "rdbms": source_rdbms,
            "schema": source_schema
        },
        "target": {
            "rdbms": target_rdbms,
            "schema": target_schema
        },
        "migrated-tables": migrated_tables
    }


# structure of the migration data returned:
# [
#   {
#      "table": <table-name>,
#      "columns": [
#        {
#          "name": <column-name>,
#          "source-type": <column-type>,
#          "target-type": <column-type>,
#          "features": [
#            "identity",
#            "primary_key"
#          ]
#        },
#        ...
#      ],
#      "count": <number-of-tuples-migrated>,
#      "status": "none" | "full" | "partial"
#   }
# ]
def migrate_metadata(errors: list[str],
                     source_rdbms: str,
                     target_rdbms: str,
                     source_schema: str,
                     target_schema: str,
                     data_tables: list[str],
                     logger: Logger | None) -> list[dict]:

    # iinitialize the return variable
    result: list[dict] = []

    # create engines
    source_engine: Engine = build_engine(errors, source_rdbms, logger)
    target_engine: Engine = build_engine(errors, target_rdbms, logger)

    # were both engines created ?
    if source_engine and target_engine:
        # yes, proceed
        from_schema: str | None = None

        # obtain the source schema's internal name
        inspector: Inspector = inspect(subject=source_engine,
                                       raiseerr=True)
        for schema_name in inspector.get_schema_names():
            # is this the source schema ?
            if source_schema.lower() == schema_name.lower():
                # yes, use the actual name with its case imprint
                from_schema = schema_name
                break

        # does the source schema exist ?
        if from_schema:
            # yes, proceed
            source_metadata: MetaData = MetaData(schema=from_schema)
            try:
                source_metadata.reflect(bind=source_engine,
                                        schema=from_schema)
            except SAWarning as e:
                # - unable to fully reflect the schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                err_msg = exc_format(exc=e,
                                     exc_info=sys.exc_info())
                # 106: The operation {} returned the error {}
                errors.append(validate_format_error(106, "schema-reflection", err_msg))

            # build list of migration candidates
            unlisted_tables: list[str] = []
            if data_tables:
                table_names: list[str] = [table.name for table in source_metadata.sorted_tables]
                for spare_table in data_tables:
                    if spare_table not in table_names:
                        unlisted_tables.append(spare_table)

            # proceed, if all tables were found
            if len(unlisted_tables) == 0:
                # purge the source metadata from spare tables, if applicable
                if data_tables:
                    # build the list of spare tables in source metadata
                    spare_tables: list[Table] = []
                    for spare_table in source_metadata.sorted_tables:
                        if spare_table.name not in data_tables:
                            spare_tables.append(spare_table)
                    # remove the spare tables from the source metadata
                    for spare_table in spare_tables:
                        source_metadata.remove(table=spare_table)
                    # make sure spare tables will not hang around
                    del spare_tables

                # proceed with the appropriate tables
                source_tables: list[Table]
                try:
                    source_tables = source_metadata.sorted_tables
                except SAWarning as e:
                    # - unable to organize the tables in the proper sequence:
                    #   probably, cross-dependencies between tables, caused by mutually dependent FKs
                    # - this error will cause the migration to be aborted,
                    #   as SQLAlchemy will not be able to compile the migrated schema
                    err_msg = exc_format(exc=e,
                                         exc_info=sys.exc_info())
                    # 106: The operation {} returned the error {}
                    errors.append(validate_format_error(106, "schema-migration", err_msg))
                    source_tables = list(source_metadata.tables.values())

                to_schema: str | None = None
                inspector = inspect(subject=target_engine,
                                    raiseerr=True)
                # obtain the target schema's internal name
                for schema_name in inspector.get_schema_names():
                    # is this the target schema ?
                    if target_schema.lower() == schema_name.lower():
                        # yes, use the actual name with its case imprint
                        to_schema = schema_name
                        break

                # does the target schema already exist ?
                if to_schema:
                    # yes, drop existing tables (must be done in reverse order)
                    for source_table in reversed(source_tables):
                        table_name: str = f"{to_schema}.{source_table.name}"
                        if target_rdbms == "oracle":
                            # oracle has no 'IF EXISTS' clause
                            drop_stmt: str = (f"IF OBJECT_ID({table_name}, 'U') "
                                              f"IS NOT NULL DROP TABLE {table_name};")
                            db_execute(errors=errors,
                                       exc_stmt=drop_stmt,
                                       engine="oracle",
                                       logger=logger)
                        else:
                            drop_stmt: str = f"DROP TABLE IF EXISTS {table_name}"
                            engine_exc_stmt(errors, target_rdbms,
                                            target_engine, drop_stmt, logger)
                else:
                    # no, create the target schema
                    if target_rdbms == "oracle":
                        stmt: str = f"CREATE USER {target_schema} IDENTIFIED BY {target_schema}"
                    else:
                        conn_params: dict = pydb_validator.get_connection_params(errors, target_rdbms)
                        stmt = f"CREATE SCHEMA {target_schema} AUTHORIZATION {conn_params.get('user')}"
                    engine_exc_stmt(errors, target_rdbms, target_engine, stmt, logger)

                    # SANITY CHECK: it has happened that a schema creation failed, with no errors reported
                    if len(errors) == 0:
                        inspector = inspect(subject=target_engine,
                                            raiseerr=True)
                        for schema_name in inspector.get_schema_names():
                            # is this the target schema ?
                            if target_schema.lower() == schema_name.lower():
                                # yes, use the actual name with its case imprint
                                to_schema = schema_name
                                break

                # does the target schema exist now ?
                if to_schema:
                    # yes, establish the migration equivalences
                    (native_ordinal, reference_ordinal) = \
                        pydb_types.establish_equivalences(source_rdbms, target_rdbms)

                    # setup target tables
                    for source_table in source_tables:

                        # build the list of migrated columns for this table
                        table_columns: list[dict] = []
                        # noinspection PyProtectedMember
                        columns: list[Column] = source_table.c._all_columns
                        for column in columns:
                            column_data: dict = {
                                "name": column.name,
                                "source-type": str(column.type)
                            }
                            table_columns.append(column_data)

                        # migrate the columns
                        setup_target_table(errors, columns,
                                           source_rdbms, target_rdbms,
                                           native_ordinal, reference_ordinal, logger)
                        source_table.schema = to_schema

                        # register the mew column properties
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
                            for table_column in table_columns:
                                if table_column["name"] == column.name:
                                    table_column["target-type"] = str(column.type)
                                    if features:
                                        table_column["features"] = features
                                    break

                        # register the migrated table
                        migrated_table: dict = {
                            "table": source_table.name,
                            "columns": table_columns,
                            "count": 0,
                            "status": "none"
                        }
                        result.append(migrated_table)

                        # issue warning if no primary key was found for table
                        no_pk: bool = True
                        for column in table_columns:
                            if "primary key" in (column.get("features") or []):
                                no_pk = False
                                break
                        if no_pk:
                            logger.warning((f"RDBMS {source_rdbms}, "
                                            f"table {source_schema}.{source_table}, "
                                            f"no primary key column found"))

                    # create tables in target schema
                    try:
                        source_metadata.create_all(bind=target_engine,
                                                   checkfirst=False)
                    except Exception as e:
                        # unable to fully compile the schema - the migration is now doomed
                        err_msg = exc_format(exc=e,
                                             exc_info=sys.exc_info())
                        # 106: The operation {} returned the error {}
                        errors.append(validate_format_error(106, "schema-construction", err_msg))
                else:
                    # 104: Unexpected error: {}
                    errors.append(validate_format_error(104,
                                                        f"unable to create schema in RDBMS {target_rdbms}",
                                                        "@to-schema"))
            else:
                # tables not found, report them
                bad_tables: str = ", ".join(unlisted_tables)
                errors.append(validate_format_error(119, bad_tables,
                                                    f"not found in {source_rdbms}/{source_schema}",
                                                    "@tables"))
        else:
            # 119: Invalid value {}: {}
            errors.append(validate_format_error(119, source_schema,
                                                f"schema not found in RDBMS {source_rdbms}",
                                                "@from-schema"))
    return result


def migrate_plain(errors: list[str],
                  source_rdbms: str,
                  target_rdbms: str,
                  source_schema: str,
                  target_schema: str,
                  source_conn: Any,
                  target_conn: Any,
                  migrated_tables: list[dict],
                  logger: Logger | None) -> None:

    # traverse list of migrated tables to copy the plain data
    for migrated_table in migrated_tables:
        source_table: str = f"{source_schema}.{migrated_table.get('table')}"
        target_table: str = f"{target_schema}.{migrated_table.get('table')}"

        # exclude LOB (large binary objects) types from the column names list
        table_columns: list[dict] = migrated_table.get("columns")
        column_names: list[str] = [column.get("name") for column in table_columns
                                   if not pydb_types.is_lob(column.get("source-type"))]

        count: int = db_migrate_data(errors=errors,
                                     source_engine=source_rdbms,
                                     source_table=source_table,
                                     source_columns=column_names,
                                     target_engine=target_rdbms,
                                     target_table=target_table,
                                     source_conn=source_conn,
                                     target_conn=target_conn,
                                     batch_size=pydb_common.MIGRATION_BATCH_SIZE,
                                     logger=logger)
        if errors:
            status: str = "partial" if count else "none"
        else:
            status: str = "full"
        migrated_table["status"] = status
        migrated_table["count"] = count
        logger.debug(msg=f"Migrated {count} tuples, status '{status}', "
                         f"from {source_rdbms}.{source_table} "
                         f"to {target_rdbms}.{target_table}")


def migrate_lobs(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 source_schema: str,
                 target_schema: str,
                 source_conn: Any,
                 target_conn: Any,
                 migrated_tables: list[dict],
                 logger: Logger | None) -> None:

    # traverse list of migrated tables to copy the LOBs
    for migrated_table in migrated_tables:

        # organize the information
        source_table: str = f"{source_schema}.{migrated_table.get('table')}"
        target_table: str = f"{target_schema}.{migrated_table.get('table')}"
        table_pks: list[str] = []
        table_lobs: list[str] = []
        for column in migrated_table.get("columns"):
            if pydb_types.is_lob(column.get("source-type")):
                table_lobs.append(column.get("name"))
            features: list[str] = column.get("features") or []
            if "primary key" in features:
                table_pks.append(column.get("name"))

        # can only migrate LOBs if table has primary keys
        if table_pks:
            # process the existing LOB columns
            for table_lob in table_lobs:
                db_migrate_lobs(errors=errors,
                                source_engine=source_rdbms,
                                source_table=source_table,
                                source_column=table_lob,
                                pk_columns=table_pks,
                                target_engine=target_rdbms,
                                target_table=target_table,
                                source_conn=source_conn,
                                target_conn=target_conn,
                                chunk_size=pydb_common.MIGRATION_CHUNK_SIZE,
                                logger=logger)


def build_engine(errors: list[str],
                 rdbms: str,
                 logger: Logger) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    # obtain the connection string
    conn_str: str = pydb_validator.get_connection_string(rdbms)

    # build the engine
    try:
        result = create_engine(url=conn_str)
        pydb_common.log(logger, DEBUG,
                        f"RDBMS {rdbms}, created migration engine")
    except Exception as e:
        params: dict = pydb_validator.get_connection_params(errors, rdbms)
        errors.append(pydb_common.db_except_msg(e, params.get("name"), params.get("host")))

    return result


def setup_target_table(errors: list[str],
                       table_columns: list[Column],
                       source_rdbms: str,
                       target_rdbms: str,
                       native_ordinal: int,
                       reference_ordinal: int,
                       logger: Logger) -> None:

    # set the target columns
    # noinspection PyProtectedMember
    for table_column in table_columns:
        # convert the type
        target_type: Any = pydb_types.migrate_type(source_rdbms, target_rdbms,
                                                   native_ordinal, reference_ordinal, table_column, logger)

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
            err_msg = exc_format(exc=e,
                                 exc_info=sys.exc_info())
            # 104: Unexpected error: {}
            errors.append(validate_format_error(104, err_msg))


def disable_session_restrictions(errors: list[str],
                                 rdbms: str,
                                 conn: Any,
                                 logger: Logger) -> None:

    # disable session restrictions to speed-up bulk copy
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            pydb_oracle.disable_session_restrictions(errors, conn, logger)
        case "postgres":
            pydb_postgres.disable_session_restrictions(errors, conn, logger)
        case "sqlserver":
            pydb_sqlserver.disable_session_restrictions(errors, conn, logger)

    pydb_common.log(logger, DEBUG,
                    f"RDBMS {rdbms}, disabled session restrictions to speed-up bulk copying")


def restore_session_restrictions(errors: list[str],
                                 rdbms: str,
                                 conn: Any,
                                 logger: Logger) -> None:

    # restore session restrictions delaying bulk copy
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            pydb_oracle.restore_session_restrictions(errors, conn, logger)
        case "postgres":
            pydb_postgres.restore_session_restrictions(errors, conn, logger)
        case "sqlserver":
            pydb_sqlserver.restore_session_restrictions(errors, conn, logger)

    pydb_common.log(logger, DEBUG,
                    f"RDBMS {rdbms}, restored session restrictions delaying bulk copying")


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
            pydb_common.log(logger, DEBUG,
                            f"RDBMS {rdbms}, sucessfully executed {stmt}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))

    return result
