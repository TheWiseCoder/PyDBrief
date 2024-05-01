import sys
from logging import DEBUG, INFO, Logger
from pypomes_core import validate_format_error, exc_format
from sqlalchemy import text  # from 'sqlalchemy._elements._constructors', but invisible
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.create import create_engine
from sqlalchemy.engine.reflection import Inspector, Sequence
from sqlalchemy.engine.result import Result
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.ddl import CreateSchema
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.schema import MetaData, Table

from . import (
    pydb_common, pydb_types, pydb_validator,
    pydb_oracle, pydb_postgres, pydb_sqlserver  # , pydb_mysql
)


# this is the entry point for the migration process
def migrate_data(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 schema: str,
                 data_tables: list[str],
                 drop_tables: bool,
                 logger: Logger = None) -> dict:

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
                create_schema(errors, target_rdbms, schema, target_session, logger)

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
                    session_exc_stmt(errors, target_rdbms,
                                     target_session, drop_stmt, logger)

            # setup target tables
            for source_table in source_tables:
                # create table in target RDBMS
                setup_target_table(errors, source_rdbms,
                                   target_rdbms, source_table, logger)
            source_metadata.create_all(bind=target_engine,
                                       checkfirst=False)

            # copy the data
            for source_table in source_tables:
                session_unlog_table(errors, target_rdbms,
                                    target_session, source_table, logger)
                # obtain SELECT statement and copy the table data
                offset: int = 0
                sel_stmt: str = build_select_query(source_rdbms, schema,
                                                   source_table, offset, logger)
                data: Sequence = session_bulk_fetch(errors, source_rdbms,
                                                    source_session, sel_stmt, logger)
                while data:
                    # insert the current chunk of data
                    session_bulk_insert(errors, target_rdbms,
                                        target_session, source_table, data, logger)
                    # fetch the next chunk of data
                    offset += pydb_common.MIGRATION_BATCH_SIZE
                    sel_stmt = build_select_query(source_rdbms, schema,
                                                  source_table, offset, logger)
                    data = session_bulk_fetch(errors, source_rdbms,
                                              source_session, sel_stmt, logger)
        else:
            # 119: Invalid value {}: {}
            errors.append(validate_format_error(119, schema,
                                                f"schema inexistente no RDBMS {source_rdbms}",
                                                "@schema"))

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


def create_schema(errors: list[str],
                  rdbms: str,
                  schema: str,
                  session: Session,
                  logger: Logger) -> None:
    try:
        session.execute(statement=CreateSchema(schema))
        pydb_common.log(logger, DEBUG,
                        f"RDBMS {rdbms}, created schema {schema}")
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))


def setup_target_table(errors: list[str],
                       source_rdbms: str,
                       target_rdbms: str,
                       source_table: Table,
                       logger: Logger) -> None:

    # clear the indices and constraints
    source_table.indexes.clear()
    source_table.constraints.clear()

    # set the target columns
    for column in source_table.columns():
        # convert the type
        target_type = pydb_types.convert_type(source_rdbms, target_rdbms, column, logger)
        # convert the default value - TODO: write a decent default value conversion function
        curr_default: str = source_table.c[column.name].default
        new_default: str | None = None
        if curr_default.lower() not in ["sysdate", "systime"]:
            new_default = curr_default
        #
        try:
            source_table.c[column.name].type = target_type
            source_table.c[column.name].default = new_default
        except Exception as e:
            err_msg = exc_format(exc=e,
                                 exc_info=sys.exc_info())
            # 104: Unexpected error: {}
            errors.append(validate_format_error(104, err_msg))


def build_select_query(rdbms: str,
                       schema: str,
                       table: Table,
                       offset: int,
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
            result = pydb_oracle.build_select_query(schema, table.name, columns_str,
                                                    offset, pydb_common.MIGRATION_BATCH_SIZE)
        case "postgres":
            result = pydb_postgres.build_select_query(schema, table.name, columns_str,
                                                      offset, pydb_common.MIGRATION_BATCH_SIZE)
        case "sqlserver":
            result = pydb_sqlserver.build_select_query(schema, table.name, columns_str,
                                                       offset, pydb_common.MIGRATION_BATCH_SIZE)

    pydb_common.log(logger, DEBUG,
                    f"RDBMS {rdbms}, built query {result}")

    return result


def session_unlog_table(errors: list[str],
                        rdbms: str,
                        session: Session,
                        table: Table,
                        logger: Logger) -> None:

    # obtain the statement
    stmt: str | None = None
    match rdbms:
        case "mysql":
            pass
        case "oracle":
            stmt = pydb_oracle.get_table_unlog_stmt(table.name)
        case "postgres":
            stmt = pydb_postgres.get_table_unlog_stmt(table.name)
        case "sqlserver":
            stmt = pydb_sqlserver.get_table_unlog_stmt(table.name)

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
    except Exception as e:
        err_msg = exc_format(exc=e,
                             exc_info=sys.exc_info())
        pydb_common.log(logger, INFO,
                        f"RDBMS {rdbms}, inserted {len(data)} tuples in table {table.name}")
        # 104: Unexpected error: {}
        errors.append(validate_format_error(104, err_msg))
