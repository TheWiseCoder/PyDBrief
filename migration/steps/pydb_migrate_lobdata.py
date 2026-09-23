import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from logging import Logger
from pathlib import Path
from pypomes_core import TZ_LOCAL, str_as_list, timestamp_duration
from pypomes_db import (
    db_connect, db_count, db_close,
    db_migrate_lobs, db_table_exists, db_drop_table,
    db_bulk_insert, db_create_session_table, db_get_session_table_prefix
)
from pypomes_s3 import s3_get_client, s3_item_exists
from typing import Any

from app_constants import InputParam
from entities.migration import Migration, MigStep
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_table import MigrationTable
from entities.session import Session, sessions_aborting
from migration.pydb_common import build_channel_data, build_lob_prefix
from migration.pydb_types import is_lob_column
from migration.steps.pydb_to_s3 import s3_migrate_lobs

# structure of the thread registry:
# lobdata_registry: dict[int, dict[str, Any]] = {
#   <mother-thread> = {
#     "child-threads": [
#       <child-thread>,
#       ...
#     ],
#     "source-table-name": {
#       "table-count": <int>,
#       "table-bytes": <int>,
#       "errors": [
#         <error>,
#         ...
#       ]
#     },
#   },
#   ...
# }
lobdata_registry: dict[int, dict[str, Any]] = {}
lobdata_lock: threading.Lock = threading.Lock()


def migrate_lobdata(migration: Migration,
                    session: Session,
                    migration_threads: list[int],
                    migrated_tables: dict[str, Any],
                    migration_warnings: list[str],
                    errors: list[str],
                    logger: Logger) -> tuple[int, int]:

    # initialize the return variables
    result_count: int = 0
    result_bytes: int = 0

    # add to the thread register
    mother_thread: int = threading.get_ident()
    migration_threads.append(mother_thread)
    with lobdata_lock:
        lobdata_registry[mother_thread] = {
            "child-threads": []
        }

    # retrieve the source and target RDBMS engines, and the channel count
    source_db: str = session.get_source_db().cd_engine
    target_db: str = session.get_target_db().cd_engine
    target_s3: str = session.get_target_s3().cd_engine if session.id_target_s3 else None

    # traverse list of migrated tables to copy the LOB data
    for table_name, table_data in migrated_tables.items():

        # verify whether current migration is marked for abortion
        if session.cd_session in sessions_aborting:
            sessions_aborting.remove(session.cd_session)
            break

        source_schema: str = session.nm_source_schema
        source_table: str = f"{source_schema}.{table_name}"
        target_schema: str = session.nm_target_schema
        target_table: str = f"{target_schema}.{table_name}"
        with lobdata_lock:
            lobdata_registry[mother_thread][source_table] = {
                "table-count": 0,
                "table-bytes": 0,
                "errors": []
            }

        # obtain the corresponding MigrationTable instance
        migration_table: MigrationTable = \
            next((t for t in (migration.get_migration_tables() or []) if t.nm_table == table_name), None)

        # obtain limit and offset
        limit_count: int = (migration_table.nr_incremental_count if migration_table else 0) or 0
        offset_count: int = (migration_table.nr_incremental_offset if migration_table else 0) or 0

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
        lob_columns: list[tuple[str, str]] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            # migrating to S3 requires that the lob column be mapped in 'named-lobdata'
            if is_lob_column(col_type=column_type):
                reference_column: str | None = None
                # determine if lobdata in 'lob_column' has its filename defined in 'named-lobdata'
                named_lobdata: list[str] = str_as_list(migration_table.ds_named_lobdata)
                for item in named_lobdata:
                    # format of item is '<column-name>=<named-column>[.<filetype>]'
                    if item.startswith(f"{column_name}="):
                        reference_column = item[item.index("=")+1:]
                        break
                if session.id_target_s3:
                    warn_msg: str | None = None
                    if reference_column == column_name:
                        warn_msg = "mapped to itself"
                        reference_column = None
                    elif not reference_column:
                        warn_msg = "not mapped"
                    if warn_msg:
                        warn_msg = (f"Column {source_db}.{source_table}.{column_name} "
                                    f"{warn_msg} in '{InputParam.NAMED_LOBDATA}'")
                        migration_warnings.append(warn_msg)
                        logger.warning(msg=warn_msg)
                        MigrationIssue.new_issue(id_migration=migration.id,
                                                 cd_type=IssueType.WARNING,
                                                 ds_issue=warn_msg)
                lob_columns.append((column_name, reference_column))
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        if lob_columns:
            # specific condition for migrating table LOBs to database
            if not session.id_target_s3 and not db_table_exists(table_name=target_table,
                                                                engine=target_db,
                                                                errors=errors):
                # target table could not be found (might be due to error)
                warn_msg: str = ("Unable to migrate LOBs, "
                                 f"table {target_db}.{target_table} was not found")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_type=IssueType.WARNING,
                                         ds_issue=warn_msg)
                # skip table migration
                continue

            # start migrating the source table LOBs
            started: datetime = datetime.now(tz=TZ_LOCAL)
            status: str = "ok"
            migrate_lob_columns(migration=migration,
                                session=session,
                                mother_thread=mother_thread,
                                source_table=source_table,
                                target_table=target_table,
                                lob_columns=lob_columns,
                                pk_columns=pk_columns,
                                lob_tuples=None,
                                offset_count=offset_count,
                                limit_count=limit_count,
                                chunk_size=migration_table.nr_chunk_size,
                                migration_warnings=migration_warnings,
                                errors=errors,
                                logger=logger)
            with lobdata_lock:
                lob_count: int = lobdata_registry[mother_thread][source_table]["table-count"]
                lob_bytes: int = lobdata_registry[mother_thread][source_table]["table-bytes"]
                op_errors: list[str] = lobdata_registry[mother_thread][source_table]["errors"]
                if op_errors:
                    status = "error"
                    for op_error in op_errors:
                        errors.append(op_error)
                        MigrationIssue.new_issue(id_migration=migration.id,
                                                 cd_type=IssueType.ERROR,
                                                 ds_issue=op_error)

            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            mins: float = (finished - started).total_seconds() / 60
            performance: str = (f"{lob_count/mins:.2f} LOBs/min, "
                                f"{lob_bytes/(mins * 1024 ** 2):.2f} MBytes/min")
            table_data.update({
                "lob-status": status,
                "lob-count": lob_count,
                "lob-bytes": lob_bytes,
                "lob-duration": duration,
                "lob-performance": performance
            })
            target: str = f"S3 storage '{target_s3}'" if target_s3 else f"{target_db}.{table_name}"
            logger.debug(msg=f"Migrated {lob_count} LOBs, {lob_bytes} bytes, in {duration} ({performance}), "
                             f"from {source_db}.{table_name} to {target}, status {status}")
            result_count += lob_count
            result_bytes += lob_bytes

    with lobdata_lock:
        migration_threads.extend(lobdata_registry[mother_thread]["child-threads"])
        lobdata_registry.pop(mother_thread)

    return result_count, result_bytes


def migrate_lob_columns(migration: Migration,
                        session: Session,
                        mother_thread: int,
                        source_table: str,
                        target_table: str,
                        pk_columns: list[str],
                        lob_columns: list[tuple[str, str]],
                        lob_tuples: dict[str, list[str]] | None,
                        offset_count: int,
                        limit_count: int,
                        chunk_size: int,
                        migration_warnings: list[str],
                        errors: list[str],
                        logger: Logger) -> None:

    # retrieve needed specs
    channel_count: int = migration.nr_lobdata_channels
    channel_size: int = migration.nr_lobdata_channel_size
    source_db: str = session.get_source_db().cd_engine
    target_s3: str = session.get_target_s3().cd_engine if session.id_target_s3 else None

    # process the existing LOB columns
    for lob_column, reference_column in lob_columns:

        # verify whether current migration is marked for abortion
        if session.cd_session in sessions_aborting:
            sessions_aborting.remove(session.cd_session)
            break

        where_clause: str | list[str]
        table_count: int
        lob_prefix: Path | None = None
        forced_filetype: str | None = None

        # specific handlings for migrating 'lob_column' to S3
        if session.id_target_s3:
            if not reference_column and not pk_columns:
                warn_msg: str = (f"Column {source_db}.{source_table}.{lob_column} "
                                 "is not eligible for LOB migration to S3 "
                                 f"(not mapped in '{InputParam.NAMED_LOBDATA}', and no PKs in table)")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_type=IssueType.WARNING,
                                         ds_issue=warn_msg)
                # skip current table migration
                continue

            # define a forced file type
            if reference_column:
                pos: int = reference_column.rfind(".")
                if pos > 0:
                    # 'forced_filetype' includes the leading dot ('.')
                    forced_filetype = reference_column[pos:]
                    reference_column = reference_column[:pos]

            # obtain an S3 prefix for storing the lobdata
            if migration.cd_step == MigStep.CORRELATE_LOBDATA or not migration.is_flatten_storage:
                lob_prefix = build_lob_prefix(session=session,
                                              target_table=target_table,
                                              column_name=reference_column or lob_column)
                # skip nonempty S3 prefixes
                if (migration.cd_step != MigStep.CORRELATE_LOBDATA and
                    migration.is_skip_nonempty and
                    s3_item_exists(identifier=lob_prefix.as_posix(),
                                   errors=errors)):
                    warn_msg: str = ("Skipped migrating LOBs in column "
                                     f"{source_db}.{source_table}.{lob_column}: "
                                     f"folder {target_s3}.{lob_prefix.as_posix()} is not empty")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)
                    MigrationIssue.new_issue(id_migration=migration.id,
                                             cd_type=IssueType.WARNING,
                                             ds_issue=warn_msg)
                    # skip column migration
                    continue

        # count migrateable tuples on source table for 'lob_column'
        if lob_tuples is None:
            where_clause = f"{lob_column} IS NOT NULL"
            table_count = (db_count(table=source_table,
                                    where_clause=where_clause,
                                    engine=source_db,
                                    errors=errors) or 0) - offset_count
        else:
            # 'where_clause' will have the list of 'reference_column' values indicating the LOBs to be migrated
            where_clause = lob_tuples.get(reference_column)
            table_count = len(where_clause)

        # migrate the LOBs in 'lob_column'
        if not errors and table_count > 0:
            # build migration channel data ([(offset, limit),...])
            channel_data: list[tuple[int, int]] = build_channel_data(channel_size=channel_size,
                                                                     table_count=table_count,
                                                                     offset_count=offset_count,
                                                                     limit_count=limit_count)
            max_workers: int = min(channel_count, len(channel_data))
            tot_count: int = sum(i[1] for i in channel_data)
            target: str = f"S3 storage '{target_s3}'" \
                if target_s3 else f"{session.get_target_db().cd_engine}.{target_table}.{lob_column}"
            logger.debug(msg=f"Started migrating {tot_count} LOBs from "
                             f"{source_db}.{source_table}.{lob_column} to {target}, "
                             f"using {max_workers} channels")
            if max_workers == 1:
                # execute single task in current thread
                if session.id_target_s3:
                    # migration target is S3
                    _s3_migrate_lobs(migration=migration,
                                     session=session,
                                     mother_thread=mother_thread,
                                     source_table=source_table,
                                     target_table=target_table,
                                     lob_prefix=lob_prefix,
                                     lob_column=lob_column,
                                     pk_columns=pk_columns or [reference_column],
                                     where_clause=where_clause,
                                     offset_count=channel_data[0][0],
                                     limit_count=tot_count,
                                     forced_filetype=forced_filetype,
                                     reference_column=reference_column,
                                     chunk_size=chunk_size,
                                     migration_warnings=migration_warnings,
                                     logger=logger)
                else:
                    # migration target is database
                    _db_migrate_lobs(session=session,
                                     mother_thread=mother_thread,
                                     source_table=source_table,
                                     lob_column=lob_column,
                                     pk_columns=pk_columns or [reference_column],
                                     target_table=target_table,
                                     where_clause=where_clause,
                                     offset_count=channel_data[0][0],
                                     limit_count=tot_count,
                                     chunk_size=chunk_size)
            else:
                # execute tasks concurrently
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    task_futures: list[Future] = []
                    for channel_datum in channel_data:
                        if target_s3:
                            # migration target is S3
                            future: Future = executor.submit(_s3_migrate_lobs,
                                                             migration=migration,
                                                             session=session,
                                                             mother_thread=mother_thread,
                                                             source_table=source_table,
                                                             target_table=target_table,
                                                             lob_prefix=lob_prefix,
                                                             lob_column=lob_column,
                                                             pk_columns=pk_columns or [reference_column],
                                                             where_clause=where_clause,
                                                             offset_count=channel_datum[0],
                                                             limit_count=channel_datum[1],
                                                             forced_filetype=forced_filetype,
                                                             reference_column=reference_column,
                                                             chunk_size=chunk_size,
                                                             migration_warnings=migration_warnings,
                                                             logger=logger)
                        else:
                            # migration target is database
                            future: Future = executor.submit(_db_migrate_lobs,
                                                             session=session,
                                                             mother_thread=mother_thread,
                                                             source_table=source_table,
                                                             lob_column=lob_column,
                                                             pk_columns=pk_columns or [reference_column],
                                                             target_table=target_table,
                                                             where_clause=where_clause,
                                                             offset_count=channel_datum[0],
                                                             limit_count=channel_datum[1],
                                                             chunk_size=chunk_size)
                        task_futures.append(future)

                    # wait for all task futures to complete, then shutdown down the executor
                    futures.wait(fs=task_futures)
                    executor.shutdown(wait=False)


def _db_migrate_lobs(session: Session,
                     mother_thread: int,
                     source_table: str,
                     lob_column: str,
                     pk_columns: list[str],
                     target_table: str,
                     where_clause: str,
                     offset_count: int,
                     limit_count: int,
                     chunk_size: int) -> None:

    # register the operation thread (might be same as the mother thread)
    with lobdata_lock:
        lobdata_registry[mother_thread]["child-threads"].append(threading.get_ident())

    lob_count: int = 0
    lob_bytes: int = 0
    errors: list[str] = []

    totals: tuple[int, int] = db_migrate_lobs(source_engine=session.get_source_db().cd_engine,
                                              source_table=source_table,
                                              source_lob_column=lob_column,
                                              source_pk_columns=pk_columns,
                                              target_engine=session.get_target_db().cd_engine,
                                              target_table=target_table,
                                              where_clause=where_clause,
                                              offset_count=offset_count,
                                              limit_count=limit_count,
                                              chunk_size=chunk_size,
                                              errors=errors)
    if not errors:
        lob_count = totals[0]
        lob_bytes = totals[1]

    with lobdata_lock:
        if errors:
            lobdata_registry[mother_thread][source_table]["errors"].extend(errors)
        else:
            lobdata_registry[mother_thread][source_table]["table-count"] += lob_count
            lobdata_registry[mother_thread][source_table]["table-bytes"] += lob_bytes


def _s3_migrate_lobs(migration: Migration,
                     session: Session,
                     mother_thread: int,
                     source_table: str,
                     target_table: str,
                     lob_prefix: Path,
                     lob_column: str,
                     pk_columns: list[str],
                     where_clause: str | list[str],
                     offset_count: int,
                     limit_count: int,
                     forced_filetype: str,
                     reference_column: str,
                     chunk_size: int,
                     migration_warnings: list[str],
                     logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    with lobdata_lock:
        lobdata_registry[mother_thread]["child-threads"].append(threading.get_ident())

    # obtain an S3 client
    errors: list[str] = []
    s3_client = s3_get_client(engine=session.get_target_db().cd_engine,
                              errors=errors)
    if s3_client:
        db_conn: Any = None
        temp_table: str | None = None
        if isinstance(where_clause, list):
            # obtain a database connection
            source_db: str = session.get_source_db().cd_engine
            db_conn = db_connect(engine=source_db,
                                 errors=errors)
            if db_conn:
                # 'where_clause' is a list of 'reference_column' values indicating the LOBs to migrate
                temp_table = f"{session.nm_source_schema}." + \
                             f"{db_get_session_table_prefix(engine=source_db)}T_{lob_column}"[:30]
                temp_column: str = f"id_{reference_column}"[:30]
                db_create_session_table(engine=source_db,
                                        connection=db_conn,
                                        table_name=temp_table,
                                        column_data=[f"{temp_column} VARCHAR2(64) PRIMARY KEY"],
                                        errors=errors)
                if not errors:
                    # no offset/limit apply herefrom, as 'where_clause' precisely filters the appropriate LOBs
                    offset_count = 0
                    limit_count = 0
                    where_clause = f"{reference_column} IN (SELECT {temp_column} FROM {temp_table})"

                    # insert the exact sublist of LOBs to be migrated by this thread
                    insert_vals: list[tuple] = [tuple(where_clause[offset_count:offset_count+limit_count])] \
                        if limit_count else [tuple(where_clause[offset_count:])]
                    db_bulk_insert(target_table=temp_table,
                                   insert_attrs=[temp_column],
                                   insert_vals=insert_vals,
                                   engine=source_db,
                                   connection=db_conn,
                                   errors=errors)
        if not errors:
            # 'target_table' is documentational, only
            totals: tuple[int, int] = s3_migrate_lobs(migration=migration,
                                                      session=session,
                                                      db_conn=db_conn,
                                                      s3_client=s3_client,
                                                      target_table=target_table,
                                                      source_table=source_table,
                                                      lob_prefix=lob_prefix,
                                                      lob_column=lob_column,
                                                      pk_columns=pk_columns,
                                                      where_clause=where_clause,
                                                      offset_count=offset_count,
                                                      limit_count=limit_count,
                                                      forced_filetype=forced_filetype,
                                                      reference_column=reference_column,
                                                      chunk_size=chunk_size,
                                                      migration_warnings=migration_warnings,
                                                      errors=errors,
                                                      logger=logger)
            with lobdata_lock:
                if errors:
                    for error in errors:
                        MigrationIssue.new_issue(id_migration=migration.id,
                                                 cd_type=IssueType.ERROR,
                                                 ds_issue=error)
                    lobdata_registry[mother_thread][source_table]["errors"].extend(errors)
                else:
                    lobdata_registry[mother_thread][source_table]["table-count"] += totals[0]
                    lobdata_registry[mother_thread][source_table]["table-bytes"] += totals[1]
        if db_conn:
            db_drop_table(table_name=temp_table,
                          connection=db_conn)
            db_close(connection=db_conn)
