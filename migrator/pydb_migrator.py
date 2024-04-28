import sys
from pypomes_core import validate_format_error, exc_format
from sqlalchemy import text  # from 'sqlalchemy._elements._constructors', but invisible
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine.result import Result
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.ddl import CreateSchema
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import MetaData, Table, Column
from typing import Any

from . import (
    pydb_common, pydb_validator,
    pydb_oracle, pydb_postgres, pydb_sqlserver
)


def build_engine(errors: list[str],
                 rdbms: str) -> Engine:

    # initialize the return variable
    result: Engine | None = None

    # obtain the connection string
    conn_str: str = pydb_validator.get_connection_string(rdbms)

    # build the engine
    try:
        result = create_engine(url=conn_str)
    except Exception as e:
        params: dict = pydb_validator.get_connection_params(errors, rdbms)
        errors.append(pydb_common.db_except_msg(e, params.get("name"), params.get("host")))

    return result


def create_schema(errors: list[str],
                  schema: str,
                  session: Session) -> None:
    try:
        session.execute(statement=CreateSchema(schema))
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))


def create_table(errors: list[str],
                 table: Table,
                 source_metadata: MetaData,
                 target_engine: Engine) -> None:

    # clear the indices and constraints
    table.indexes.clear()
    table.constraints.clear()

    # set the data type
    for column in table.columns:

def build_select_query(rdbms: str,
                       schema: str,
                       table: Table,
                       offset: int) -> str:

    # initialize the return variable
    result: str | None = None

    # obtain names of columns (column names are quoted to avoid conflicts with keywords
    columns_list: list[str] = [f'"{column}"' for column in table.columns.keys()]
    columns_str: str = ", ".join(columns_list)

    # build the SELECT query
    match rdbms:
        case "oracle":
            result = pydb_oracle.build_select_query(schema, table.name, columns_str,
                                                    offset, pydb_common.MIGRATION_BATCH_SIZE)
        case "postgres":
            result = pydb_postgres.build_select_query(schema, table.name, columns_str,
                                                      offset, pydb_common.MIGRATION_BATCH_SIZE)
        case "sqlserver":
            result = pydb_sqlserver.build_select_query(schema, table.name, columns_str,
                                                       offset, pydb_common.MIGRATION_BATCH_SIZE)

    return result

def get_column_type(rdbms: str, column:)


def session_unlog_table(errors: list[str],
                        rdbms: str,
                        session: Session,
                        table: Table) -> None:

    # obtain the statement
    stmt: str | None = None
    match rdbms:
        case "oracle":
            stmt = pydb_oracle.get_table_unlog_stmt(table.name)
        case "postgres":
            stmt = pydb_postgres.get_table_unlog_stmt(table.name)
        case "sqlserver":
            stmt = pydb_sqlserver.get_table_unlog_stmt(table.name)

    # does the RDMS support table unlogging ?
    if stmt:
        # yes, unlog the table
        session_exc_stmt(errors, session, stmt)


def session_exc_stmt(errors: list[str],
                     session: Session,
                     stmt: str) -> Result:

    result: Result | None = None
    exc_stmt: TextClause = text(stmt)
    try:
        result = session.execute(statement=exc_stmt)
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))

    return result


def session_bulk_fetch(errors: list[str],
                       session: Session,
                       sel_stmt: str) -> Any:

    result: Any = None
    exc_stmt: TextClause = text(sel_stmt)
    try:
        reply: Result = session.execute(exc_stmt)
        result = reply.fetchall()
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))

    return result


def session_bulk_insert(errors: list[str],
                        session: Session,
                        table: Table,
                        data: any) -> None:

    try:
        session.execute(table.insert(), data)
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))


def migrate_data(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 schema: str,
                 data_tables: list[str],
                 drop_tables: bool) -> dict:

    # iinitialize the return variable
    result: dict | None = None

    # create engines
    source_engine: Engine = build_engine(errors, source_rdbms)
    target_engine: Engine = build_engine(errors, target_rdbms)

    # were both engines created ?
    if source_engine is not None and target_engine is not None:
        # yes, proceed
        # noinspection PyPep8Naming
        SourceSession = sessionmaker(source_engine)
        source_session: Session = SourceSession()

        # noinspection PyPep8Naming
        TargetSession = sessionmaker(source_engine)
        target_session: Session = TargetSession()

        # verify if schema exists in source RDBMS
        inspector: Inspector = inspect(source_engine)
        schemas: list[str] = inspector.get_schema_names()
        if schema in schemas:
            # create schema in target RDBMS, if appropriate
            inspector = inspect(target_engine)
            schemas = inspector.get_schema_names()
            if schema not in schemas:
                create_schema(errors, schema, target_session)

        # errors ?
        if len(errors) == 0:
            # no, proceed
            source_metadata: MetaData = MetaData(schema=schema)
            source_metadata.reflect(bind=source_engine,
                                    schema=schema)
            sorted_tables: list[Table] = source_metadata.sorted_tables

            # build list of migration candidates
            source_tables: list[Table]
            if data_tables:
                source_tables = []
                for sorted_table in sorted_tables:
                    if sorted_table.name in data_tables:
                        source_tables.append(sorted_table)
            else:
                source_tables = sorted_tables

            # drop tables in target RDBMS ?
            if drop_tables:
                # yes (must be done in reverse order)
                for source_table in reversed(source_tables):
                    # build DROP TABLE statement
                    drop_stmt: str = f"DROP TABLE IF EXISTS {schema}.{source_table.name};"
                    session_exc_stmt(errors, target_session, drop_stmt)

            # process tables
            for source_table in source_tables:
                # create table in target RDBMS
                create_table(errors, source_table, source_metadata, target_engine)

                session_unlog_table(errors, target_rdbms, target_session, source_table)
                # obtain SELECT statement and fetch the data

                # copy data
                offset: int = 0
                sel_stmt: str = build_select_query(source_rdbms, schema, source_table, offset)
                data: Any = session_bulk_fetch(errors, source_session, sel_stmt)
                while data:
                    # insert the chunk of data chunk
                    session_bulk_insert(errors, target_session, source_table, data)
                    # fetch the next chunk of data
                    offset += pydb_common.MIGRATION_BATCH_SIZE
        else:
            # 121: Invalid value {} {}
            errors.append(validate_format_error(121, schema,
                                                f"(inexiste no RDBMS {source_rdbms}", "@schema"))









    return result
