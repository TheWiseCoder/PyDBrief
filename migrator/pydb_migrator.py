import sys
from logging import DEBUG, INFO, Logger
from pypomes_core import validate_format_error, exc_format
from sqlalchemy import text  # from 'sqlalchemy._elements._constructors', but invisible
from sqlalchemy.engine.base import Engine, RootTransaction
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.reflection import Inspector, Sequence
from sqlalchemy.engine.result import Result
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.elements import TextClause
# from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import MetaData, Table
from typing import Any

from . import (
    pydb_common, pydb_types, pydb_validator,
    pydb_oracle, pydb_postgres, pydb_sqlserver  # , pydb_mysql
)


# this is the entry point for the migration process
def migrate_data(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 source_schema: str,
                 target_schema: str,
                 data_tables: list[str],
                 logger: Logger | None) -> dict:

    # iinitialize the return variable
    result: dict | None = None

    # create engines
    source_engine: Engine = build_engine(errors, source_rdbms, logger)
    target_engine: Engine = build_engine(errors, target_rdbms, logger)

    # were both engines created ?
    if source_engine is not None and target_engine is not None:
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
            source_metadata.reflect(bind=source_engine,
                                    schema=from_schema)

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
                source_tables: list[Table] = source_metadata.sorted_tables
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
                        # build DROP TABLE statement
                        drop_stmt: str = f"DROP TABLE IF EXISTS {to_schema}.{source_table.name}"
                        # drop the table
                        engine_exc_stmt(errors, target_rdbms,
                                        target_engine, drop_stmt, logger)
                else:
                    # no, create the target schema
                    conn_params: dict = pydb_validator.get_connection_params(errors, target_rdbms)
                    stmt: str = f"CREATE SCHEMA {target_schema} AUTHORIZATION {conn_params.get('user')}"
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
                    all_columns: dict = {}
                    for source_table in source_tables:
                        columns: list[str] = setup_target_table(errors, source_rdbms,
                                                                target_rdbms, native_ordinal,
                                                                reference_ordinal, source_table, logger)
                        all_columns[source_table.name] = columns
                        source_table.schema = to_schema

                    # create tables in target schema
                    try:
                        source_metadata.create_all(bind=target_engine,
                                                   checkfirst=False)
                    except Exception as e:
                        err_msg = exc_format(exc=e,
                                             exc_info=sys.exc_info())
                        # 104: Unexpected error: {}
                        errors.append(validate_format_error(104, err_msg))

                    # errors ?
                    if len(errors) == 0:
                        # no, copy the data
                        # engine_disable_restrictions(errors, target_rdbms, target_engine, logger)
                        migrated_tables: list[dict] = []
                        for source_table in source_tables:
                            # engine_unlog_table(errors, target_rdbms, to_schema,
                            #                    target_engine, source_table, logger)
                            # obtain SELECT statement and copy the table data
                            offset: int = 0
                            migrated_count: int = 0
                            source_columns: list[str] = all_columns.get(source_table.name)
                            op_errors: len(str) = []
                            sel_stmt: str = build_select_query(source_rdbms, from_schema,
                                                               source_table, source_columns, offset,
                                                               pydb_common.MIGRATION_BATCH_SIZE, logger)
                            bulk_data: Sequence = engine_bulk_fetch(errors, source_rdbms,
                                                                    source_engine, sel_stmt, logger)
                            while bulk_data:
                                # insert the current chunk of data
                                bulk_rows: list[dict] = structure_data(bulk_data, source_columns)
                                engine_bulk_insert(op_errors, target_rdbms,
                                                   target_engine, source_table, bulk_rows, logger)
                                # errors ?
                                if len(op_errors) > 0:
                                    # yes, register it and skip to next table
                                    errors.extend(op_errors)
                                    break
                                migrated_count += len(bulk_rows)

                                # fetch the next chunk of data
                                op_errors = []
                                offset += len(bulk_rows)
                                sel_stmt = build_select_query(source_rdbms, from_schema,
                                                              source_table, source_columns, offset,
                                                              pydb_common.MIGRATION_BATCH_SIZE, logger)
                                bulk_data = engine_bulk_fetch(errors, source_rdbms,
                                                         source_engine, sel_stmt, logger)
                            migrated_tables.append({
                                "table": source_table.name,
                                "tuples": migrated_count,
                                "status": "full migration" if len(op_errors) == 0 else "partial migration"
                            })
                        result = {"migrated-tables": migrated_tables}
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
                        f"RDBMS {rdbms}, created engine")
    except Exception as e:
        params: dict = pydb_validator.get_connection_params(errors, rdbms)
        errors.append(pydb_common.db_except_msg(e, params.get("name"), params.get("host")))

    return result


def setup_target_table(errors: list[str],
                       source_rdbms: str,
                       target_rdbms: str,
                       native_ordinal: int,
                       reference_ordinal: int,
                       source_table: Table,
                       logger: Logger) -> list[str]:

    # initialize the return variable
    result: list[str] = []

    # set the target columns
    # noinspection PyProtectedMember
    for column in source_table.c._all_columns:
        # convert the type
        target_type: Any = pydb_types.migrate_type(source_rdbms, target_rdbms,
                                                   native_ordinal, reference_ordinal, column, logger)
        # is the column a large binary ?
        if not pydb_types.is_large_binary(column):
            # no, register it
            result.append(column.name)

        # wrap-up the column migration
        try:
            # set column's new type
            column.type = target_type

            # remove the server default value
            if hasattr(column, "server_default"):
                column.server_default = None

            # convert the default value - TODO: write a decent default value conversion function
            if hasattr(column, "default") and \
               column.default is not None and \
               column.lower() in ["sysdate", "systime"]:
                column.default = None
        except Exception as e:
            err_msg = exc_format(exc=e,
                                 exc_info=sys.exc_info())
            # 104: Unexpected error: {}
            errors.append(validate_format_error(104, err_msg))

    return result


def build_select_query(rdbms: str,
                       schema: str,
                       table: Table,
                       columns: list[str],
                       offset: int,
                       batch_size: int,
                       logger: Logger) -> str:

    # initialize the return variable
    result: str | None = None

    # obtain names of columns
    columns_str: str = ", ".join(columns)

    # build the SELECT query
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            result = pydb_oracle.build_select_query(schema, table.name,
                                                    columns_str, offset, batch_size)
        case "postgres":
            result = pydb_postgres.build_select_query(schema, table.name,
                                                      columns_str, offset, batch_size)
        case "sqlserver":
            result = pydb_sqlserver.build_select_query(schema, table.name,
                                                       columns_str, offset, batch_size)

    pydb_common.log(logger, DEBUG,
                    f"RDBMS {rdbms}, built query {result}")

    return result


def structure_data(data: Sequence,
                   table_columns: list[str]) -> list[dict]:

    # initialize the return variable
    result: list[dict] = []

    # traverse the data
    for values in data:
        row: dict = {}
        # build the record
        for inx, table_column in enumerate(table_columns):
            row[table_column] = values[inx]
        # register the record
        result.append(row)

    return result


def engine_disable_restrictions(errors: list[str],
                                rdbms: str,
                                engine: Engine,
                                logger: Logger) -> None:

    # obtain the statement
    stmts: list[str] | None = None
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            pass
        case "postgres":
            stmts = pydb_postgres.get_disable_restriction_stmts()
        case "sqlserver":
            pass

    for stmt in stmts or []:
        engine_exc_stmt(errors, rdbms, engine, stmt, logger)


def engine_unlog_table(errors: list[str],
                       rdbms: str,
                       schema: str,
                       engine: Engine,
                       table: Table,
                       logger: Logger) -> None:

    # obtain the statement
    stmt: str | None = None
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            stmt = pydb_oracle.get_table_unlog_stmt(schema, table.name)
        case "postgres":
            stmt = pydb_postgres.get_table_unlog_stmt(schema, table.name)
        case "sqlserver":
            # table logging cannot be disable in SQLServer
            pass

    # does the RDMS support table unlogging ?
    if stmt:
        # yes, unlog the table
        engine_exc_stmt(errors, rdbms, engine, stmt, logger)


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


def engine_bulk_fetch(errors: list[str],
                      rdbms: str,
                      engine: Engine,
                      sel_stmt: str,
                      logger: Logger) -> Sequence:

    result: Sequence | None = None
    exc_stmt: TextClause = text(sel_stmt)
    try:
        with engine.connect() as conn:
            trans: RootTransaction = conn.begin()
            reply: Result = conn.execute(exc_stmt)
            result: Sequence = reply.fetchall()
            trans.commit()
            pydb_common.log(logger, INFO,
                            f"RDBMS {rdbms}, retrieved {len(result)} tuples with {sel_stmt}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))

    return result


def engine_bulk_insert(errors: list[str],
                       rdbms: str,
                       engine: Engine,
                       table: Table,
                       data: list[dict],
                       logger: Logger) -> None:

    try:
        with engine.connect() as conn:
            trans: RootTransaction = conn.begin()
            conn.execute(table.insert(), data)
            trans.commit()
            pydb_common.log(logger, INFO,
                            f"RDBMS {rdbms}, inserted {len(data)} tuples in table {table.name}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))


def session_exc_stmt(errors: list[str],
                     rdbms: str,
                     session: Session,
                     stmt: str,
                     logger: Logger) -> Result:

    result: Result | None = None
    exc_stmt: TextClause = text(stmt)
    try:
        result = session.execute(statement=exc_stmt)
        pydb_common.log(logger, DEBUG,
                        f"RDBMS {rdbms}, sucessfully executed {stmt}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))

    return result
