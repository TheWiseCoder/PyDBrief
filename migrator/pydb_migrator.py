import sys
from logging import DEBUG, INFO, Logger
from pypomes_core import validate_format_error, exc_format
from sqlalchemy import text  # from 'sqlalchemy._elements._constructors', but invisible
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.reflection import Inspector, Sequence
from sqlalchemy.engine.result import Result
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.orm import sessionmaker
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
    result: dict = {}

    # initialize the Oracle Client, if applicable
    if source_rdbms == "oracle" or target_rdbms == "oracle":
        pydb_oracle.initialize(errors)

    # create engines
    source_engine: Engine | None = None
    target_engine: Engine | None = None
    if len(errors) == 0:
        source_engine = build_engine(errors, source_rdbms, logger)
        target_engine = build_engine(errors, target_rdbms, logger)

    # were both engines created ?
    if source_engine is not None and target_engine is not None:
        # yes, proceed
        # noinspection PyPep8Naming
        SourceSession = sessionmaker(source_engine)
        source_session: Session = SourceSession()

        # noinspection PyPep8Naming
        TargetSession = sessionmaker(target_engine)
        target_session: Session = TargetSession()

        # verify if schema exists in source RDBMS
        inspector: Inspector = inspect(source_engine)
        if source_schema not in inspector.get_schema_names():
            # 119: Invalid value {}: {}
            errors.append(validate_format_error(119, source_schema,
                                                f"schema not found in RDBMS {source_rdbms}",
                                                "@from-schema"))
        # errors ?
        if len(errors) == 0:
            # no, proceed
            source_metadata: MetaData = MetaData(schema=source_schema)
            source_metadata.reflect(bind=source_engine,
                                    schema=source_schema)
            source_tables: list[Table] = source_metadata.sorted_tables

            # build list of migration candidates
            not_found: list[str] = []
            if data_tables:
                for source_table in source_tables:
                    if source_table.name not in data_tables:
                        not_found.append(source_table.name)

            # report tables not found
            if len(not_found) > 0:
                bad_tables: str = ", ".join(not_found)
                errors.append(validate_format_error(119, bad_tables,
                                                    f"not found in {source_rdbms}/{source_schema}",
                                                    "@tables"))
            # errors ?
            if len(errors) == 0:
                # no, proceed
                inspector = inspect(target_engine)
                # does the target schema already exist in the target RDBMS ?
                if target_schema in inspector.get_schema_names():
                    # yes, drop existing tables (must be done in reverse order)
                    for source_table in reversed(source_tables):
                        # build DROP TABLE statement
                        drop_stmt: str = f"DROP TABLE IF EXISTS {target_schema}.{source_table.name};"
                        # drop the table
                        session_exc_stmt(errors, target_rdbms,
                                         target_session, drop_stmt, logger)
                else:
                    # no, create the target schema
                    conn_params: dict = pydb_validator.get_connection_params(errors, target_rdbms)
                    stmt: str = f"CREATE SCHEMA {target_schema} AUTHORIZATION {conn_params.get('user')}"
                    session_exc_stmt(errors, target_rdbms, target_session, stmt, logger)

                # errors ?
                if len(errors) == 0:
                    # establish the migration equivalences
                    (native_ordinal, reference_ordinal) = \
                        pydb_types.establish_equivalences(source_rdbms, target_rdbms)

                    # setup target tables
                    for source_table in source_tables:
                        setup_target_table(errors, source_rdbms, target_rdbms,
                                           native_ordinal, reference_ordinal, source_table, logger)
                        source_table.schema = target_schema

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
                        for source_table in source_tables:
                            # session_unlog_table(errors, target_rdbms, target_schema,
                            #                     target_session, source_table, logger)
                            # obtain SELECT statement and copy the table data
                            offset: int = 0
                            sel_stmt: str = build_select_query(source_rdbms, source_schema,
                                                               source_table, offset,
                                                               pydb_common.MIGRATION_BATCH_SIZE, logger)
                            data: Sequence = session_bulk_fetch(errors, source_rdbms,
                                                                source_session, sel_stmt, logger)
                            while data:
                                # insert the current chunk of data
                                session_bulk_insert(errors, target_rdbms,
                                                    target_session, source_table, data, logger)
                                # fetch the next chunk of data
                                offset += pydb_common.MIGRATION_BATCH_SIZE
                                sel_stmt = build_select_query(source_rdbms, source_schema,
                                                              source_table, offset,
                                                              pydb_common.MIGRATION_BATCH_SIZE, logger)
                                data = session_bulk_fetch(errors, source_rdbms,
                                                          source_session, sel_stmt, logger)

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
                       logger: Logger) -> None:

    # clear the indices and constraints
    source_table.indexes.clear()
    source_table.constraints.clear()

    # set the target columns
    # noinspection PyProtectedMember
    for column in source_table.c._all_columns:
        # convert the type
        target_type: Any = pydb_types.migrate_type(source_rdbms, target_rdbms,
                                                   native_ordinal, reference_ordinal, column, logger)
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


def build_select_query(rdbms: str,
                       schema: str,
                       table: Table,
                       offset: int,
                       batch_size: int,
                       logger: Logger) -> str:

    # initialize the return variable
    result: str | None = None

    # obtain names of columns (column names are quoted to avoid conflicts with keywords
    columns_list: list[str] = [f'"{column}"' for column in table.columns.keys()]
    columns_str: str = ", ".join(columns_list)

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


def session_unlog_table(errors: list[str],
                        rdbms: str,
                        schema: str,
                        session: Session,
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
        session_exc_stmt(errors, rdbms, session, stmt, logger)


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


def session_bulk_fetch(errors: list[str],
                       rdbms: str,
                       session: Session,
                       sel_stmt: str,
                       logger: Logger) -> Sequence:

    result: Sequence | None = None
    exc_stmt: TextClause = text(sel_stmt)
    try:
        reply: Result = session.execute(exc_stmt)
        result: Sequence = reply.fetchall()
        pydb_common.log(logger, INFO,
                        f"RDBMS {rdbms}, retrieved {len(result)} tuples with {sel_stmt}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))

    return result


def session_bulk_insert(errors: list[str],
                        rdbms: str,
                        session: Session,
                        table: Table,
                        data: Sequence,
                        logger: Logger) -> None:

    try:
        session.execute(table.insert(), data)
        pydb_common.log(logger, INFO,
                        f"RDBMS {rdbms}, inserted {len(data)} tuples in table {table.name}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))
