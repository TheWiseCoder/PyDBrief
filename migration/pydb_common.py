from datetime import datetime
from pathlib import Path
from pypomes_core import TZ_LOCAL
from pypomes_db import db_execute
from urlobject import URLObject
from typing import Any

from app_constants import PYDB_DB_ENGINE
from entities.database import Database
from entities.migration import Migration
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_span import MigrationSpan
from entities.migration_work import MigrationWork
from entities.session import Session


def build_channel_data(channel_size: int,
                       table_count: int,
                       offset_count: int,
                       limit_count: int) -> list[tuple[int, int]]:

    # initialize the return variable
    result: list[tuple[int, int]] = []

    # 'limit_count' might be 0, 'table_count' is always greater than 0
    if limit_count == 0 or limit_count > table_count:
        limit_count = table_count

    # normalize 'channel_size'
    channel_size = min(channel_size, table_count, limit_count)

    # allocate sizes and offsets for multi-thread use
    total_count: int = 0
    while total_count + channel_size <= limit_count:
        result.append((offset_count, channel_size))
        total_count += channel_size
        offset_count += channel_size

    # process remaining rows
    remainder: int = limit_count - total_count
    if remainder > 0:
        # a new channel is used, if the remaining size is greater than 10% of the channel size
        if 10 * remainder > channel_size:
            result.append((offset_count, remainder))
        # otherwise, the remaining rows are added to the last channel
        else:
            result[-1] = (result[-1][0], result[-1][1] + remainder)

    return result


def build_lob_prefix(session: Session,
                     target_table: str,
                     column_name: str) -> Path:

    database: Database = session.get_target_db()
    url: URLObject = URLObject(database.nm_host)
    # 'url.hostname' returns 'None' for 'localhost'
    host: str = f"{database.cd_type}@{url.hostname or str(url)}"
    target_schema, table_name = target_table.split(sep=".")
    return Path(host,
                database.cd_name,
                target_schema,
                table_name,
                column_name)


def execute_sql(migration: Migration,
                db_engine: str,
                sql_text: str,
                db_conn: Any = None) -> None:

    sql_stmts: list[str] = sql_text.split(sep="//")
    for sql_stmt in sql_stmts:
        errors: list[str] = []
        db_execute(exc_stmt=sql_stmt,
                   engine=db_engine,
                   connection=db_conn,
                   errors=errors)
        for error in errors:
            MigrationIssue.new_issue(id_migration=migration.id,
                                     cd_type=IssueType.ERROR,
                                     ds_issue=f"SQL: {sql_stmt}; Error: {error}",
                                     db_engine=PYDB_DB_ENGINE)


def get_migration_work(migration: Migration,
                       table: str,
                       db_conn: Any = None,
                       errors: list[str] = None) -> MigrationWork | None:
    """
    Retrieve anr return the *MigrationWork* instance corresponding to the given *migration* and *table*

    If no instance is found, a new one is created and persisted in the DB state store.

    :param migration: the reference *Migration* instance
    :param table: simple name of the reference table
    :param db_conn: the optional database connection
    :param errors: incidental errors list
    :return: the *migrationWork* instance requested, or *None* on DB access error
    """
    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    result: MigrationWork = MigrationWork.get_instance(
        where_data={MigrationWork.Db.ID_MIGRATION: migration.id,
                    MigrationWork.Db.NM_TABLE: table},
        db_engine=PYDB_DB_ENGINE,
        db_conn=db_conn,
        errors=errors)

    if not errors and not result:
        result = MigrationWork()
        result.id_migration = migration.id
        result.nm_table = table
        result.ts_start = datetime.now(tz=TZ_LOCAL)
        result.insert(db_engine=PYDB_DB_ENGINE,
                      db_conn=db_conn,
                      errors=errors)

    return result if not errors else None


def assert_migration_work(migration: Migration,
                          table: str,
                          db_conn: Any = None,
                          errors: list[str] = None) -> None:

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    migration_work: MigrationWork = get_migration_work(migration=migration,
                                                       table=table,
                                                       db_conn=db_conn,
                                                       errors=errors)
    if not errors:
        migration_spans: list[MigrationSpan] = migration_work.get_migration_spans(refresh=True,
                                                                                  db_conn=db_conn,
                                                                                  errors=errors)
        is_finished: bool = True
        for migration_span in migration_spans:
            if not migration_span.is_done:
                is_finished = False
                break
        if is_finished:
            migration_work.ts_finish = datetime.now(tz=TZ_LOCAL)
            migration_work.update(db_engine=PYDB_DB_ENGINE,
                                  db_conn=db_conn,
                                  errors=errors)


def get_migration_span(migration_work: MigrationWork,
                       first_row: int,
                       last_row: int,
                       db_conn: Any = None,
                       errors: list[str] = None) -> MigrationSpan | None:

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    result: MigrationSpan = MigrationSpan.get_instance(
        where_data={MigrationSpan.Db.ID_MIGRATION_WORK: migration_work.id,
                    MigrationSpan.Db.NR_FIRST_ROW: first_row},
        db_engine=PYDB_DB_ENGINE,
        db_conn=db_conn,
        errors=errors)

    if not errors and not result:
        result = MigrationSpan()
        result.id_migration_work = migration_work.id
        result.nr_first_row = first_row
        result.nr_last_row = last_row
        result.insert(db_engine=PYDB_DB_ENGINE,
                      db_conn=db_conn,
                      errors=errors)

    return result if not errors else None


def assert_migration_span(migration_work: MigrationWork,
                          first_row: int,
                          last_row: int,
                          is_done: bool,
                          db_conn: Any = None,
                          errors: list[str] = None) -> None:

    # make sure to have an errors list
    if not isinstance(errors, list):
        errors = []

    migration_span: MigrationSpan = get_migration_span(migration_work=migration_work,
                                                       first_row=first_row,
                                                       last_row=last_row,
                                                       db_conn=db_conn,
                                                       errors=errors)
    if not errors:
        migration_span.nr_last_row = last_row
        migration_span.is_done = is_done
        migration_span.update(db_engine=PYDB_DB_ENGINE,
                              db_conn=db_conn,
                              errors=errors)
