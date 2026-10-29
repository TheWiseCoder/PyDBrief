import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from logging import Logger
from pathlib import Path
from pypomes_core import TZ_LOCAL, str_as_list, timestamp_duration
from pypomes_cs import cs_get_client, cs_item_exists
from pypomes_db import (
    db_connect, db_commit_and_close, db_rollback_and_close, db_count,
    db_migrate_lobs, db_table_exists, db_drop_table,
    db_bulk_insert, db_create_session_table, db_get_session_table_prefix
)
from typing import Any

from app_constants import PYDB_DBCTRL_ENGINE, InputParam, MigState, MigStep
from entities.migration import SPAN_CHANNEL_COUNT, SPAN_LOBDATA_SIZE, Migration, minded_migrations
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_span import MigrationSpan
from entities.migration_table import MigrationTable, SPAN_CHUNK_SIZE
from entities.migration_work import MigrationWork
from entities.session import Session
from migration.pydb_common import (
    build_channel_data, build_lob_prefix, get_migration_work, get_migration_span
)
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


def migrate_lobdata(session: Session,
                    mig_step: MigStep,
                    migration: Migration,
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
        mig_key: str = f"{mig_step}-{migration.id}"
        if minded_migrations.get(mig_key) == MigState.ABORTING:
            break

        source_schema: str = session.nm_source_schema
        target_schema: str = session.nm_target_schema
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"
        with lobdata_lock:
            lobdata_registry[mother_thread][source_table] = {
                "table-count": 0,
                "table-bytes": 0,
                "errors": []
            }

        # obtain the corresponding MigrationWork instance
        migration_work: MigrationWork = get_migration_work(migration=migration,
                                                           step=mig_step,
                                                           table=table_name,
                                                           errors=errors)
        if not errors:
            # obtain the corresponding MigrationTable instance
            migration_table: MigrationTable = MigrationTable.for_table(
                table=table_name,
                migration_tables=migration.get_migration_tables()
            ) or MigrationTable()
            # obtain limit and offset
            chunk_size: int = migration_table.nr_chunk_size or SPAN_CHUNK_SIZE[2]
            limit_count: int = migration_table.nr_incremental_count or 0
            offset_count: int = migration_table.nr_incremental_offset or 0

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
                                                     cd_step=mig_step,
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
                                             cd_step=mig_step,
                                             cd_type=IssueType.WARNING,
                                             ds_issue=warn_msg)
                    # skip table migration
                    continue

                # start migrating the source table LOBs
                started: datetime = datetime.now(tz=TZ_LOCAL)
                status: str = "ok"
                migrate_lob_columns(session=session,
                                    mig_step=mig_step,
                                    migration=migration,
                                    migration_work=migration_work,
                                    mother_thread=mother_thread,
                                    lob_columns=lob_columns,
                                    pk_columns=pk_columns,
                                    lob_tuples=None,
                                    offset_count=offset_count,
                                    limit_count=limit_count,
                                    chunk_size=chunk_size,
                                    migration_warnings=migration_warnings,
                                    errors=errors,
                                    logger=logger)
                with lobdata_lock:
                    lob_count: int = lobdata_registry[mother_thread][source_table]["table-count"]
                    lob_bytes: int = lobdata_registry[mother_thread][source_table]["table-bytes"]
                    curr_errors: list[str] = lobdata_registry[mother_thread][source_table]["errors"]
                    if curr_errors:
                        status = "error"
                        errors.extend(curr_errors)
                        MigrationIssue.new_issues(id_migration=migration.id,
                                                  cd_step=mig_step,
                                                  cd_type=IssueType.ERROR,
                                                  ds_issues=curr_errors)

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


def migrate_lob_columns(session: Session,
                        mig_step: MigStep,
                        migration: Migration,
                        migration_work: MigrationWork,
                        mother_thread: int,
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
    source_table: str = f"{session.nm_source_schema}.{migration_work.nm_table}"
    target_table: str = f"{session.nm_target_schema}.{migration_work.nm_table}"
    channel_count: int = migration.nr_channel_count or SPAN_CHANNEL_COUNT[2]
    channel_size: int = migration.nr_lobdata_size or SPAN_LOBDATA_SIZE[2]
    source_db: str = session.get_source_db().cd_engine
    target_s3: str = session.get_target_s3().cd_engine if session.id_target_s3 else None

    # process the existing LOB columns
    for lob_column, reference_column in lob_columns:

        # verify whether current migration is marked for abortion
        mig_key: str = f"{mig_step}-{migration.id}"
        if minded_migrations.get(mig_key) == MigState.ABORTING:
            break

        where_clause: str | list[str]
        table_count: int
        lob_prefix: Path | None = None
        forced_filetype: str | None = None

        # specific handlings for migrating 'lob_column' to S3
        if target_s3:
            if not reference_column and not pk_columns:
                warn_msg: str = (f"Column {source_db}.{source_table}.{lob_column} "
                                 "is not eligible for LOB migration to S3 "
                                 f"(not mapped in '{InputParam.NAMED_LOBDATA}', and no PKs in table)")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_step=mig_step,
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
            if mig_step == MigStep.CORRELATE_LOBDATA or not migration.is_flatten_storage:
                lob_prefix = build_lob_prefix(session=session,
                                              target_table=target_table,
                                              column_name=reference_column or lob_column)
                # skip nonempty S3 prefixes
                if (mig_step != MigStep.CORRELATE_LOBDATA and
                    migration.is_skip_nonempty and
                    cs_item_exists(identifier=lob_prefix.as_posix(),
                                   errors=errors)):
                    warn_msg: str = ("Skipped migrating LOBs in column "
                                     f"{source_db}.{source_table}.{lob_column}: "
                                     f"folder {target_s3}.{lob_prefix.as_posix()} is not empty")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)
                    MigrationIssue.new_issue(id_migration=migration.id,
                                             cd_step=mig_step,
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
                             f"in {len(channel_data)} steps, using {max_workers} channels")
            if max_workers == 1:
                # execute single task in current thread
                for channel_datum in channel_data:
                    if target_s3:
                        # migration target is S3
                        _s3_migrate_lobs(session=session,
                                         mig_step=mig_step,
                                         migration=migration,
                                         migration_work=migration_work,
                                         mother_thread=mother_thread,
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
                        _db_migrate_lobs(session=session,
                                         mig_step=mig_step,
                                         migration=migration,
                                         migration_work=migration_work,
                                         mother_thread=mother_thread,
                                         lob_column=lob_column,
                                         pk_columns=pk_columns or [reference_column],
                                         where_clause=where_clause,
                                         offset_count=channel_datum[0],
                                         limit_count=channel_datum[1],
                                         chunk_size=chunk_size,
                                         logger=logger)
            else:
                # execute tasks concurrently
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    task_futures: list[Future] = []
                    for channel_datum in channel_data:
                        if target_s3:
                            # migration target is S3
                            future: Future = executor.submit(_s3_migrate_lobs,
                                                             session=session,
                                                             mig_step=mig_step,
                                                             migration=migration,
                                                             migration_work=migration_work,
                                                             mother_thread=mother_thread,
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
                                                             mig_step=mig_step,
                                                             migration=migration,
                                                             migration_work=migration_work,
                                                             mother_thread=mother_thread,
                                                             lob_column=lob_column,
                                                             pk_columns=pk_columns or [reference_column],
                                                             where_clause=where_clause,
                                                             offset_count=channel_datum[0],
                                                             limit_count=channel_datum[1],
                                                             chunk_size=chunk_size,
                                                             logger=logger)
                        task_futures.append(future)

                    # wait for all task futures to complete, then shutdown down the executor
                    futures.wait(fs=task_futures)
                    executor.shutdown(wait=False)


def _db_migrate_lobs(session: Session,
                     mig_step: MigStep,
                     migration: Migration,
                     migration_work: MigrationWork,
                     mother_thread: int,
                     lob_column: str,
                     pk_columns: list[str],
                     where_clause: str,
                     offset_count: int,
                     limit_count: int,
                     chunk_size: int,
                     logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    with lobdata_lock:
        lobdata_registry[mother_thread]["child-threads"].append(threading.get_ident())

    errors: list[str] = []
    source_db: str = session.get_source_db().cd_engine
    target_db: str = session.get_target_db().cd_engine
    source_table: str = f"{session.nm_source_schema}.{migration_work.nm_table}"
    target_table: str = f"{session.nm_target_schema}.{migration_work.nm_table}"

    # determine whether the migration needs to be carried out
    lob_count: int = 0
    lob_bytes: int = 0
    migration_span: MigrationSpan = get_migration_span(migration_work=migration_work,
                                                       first_row=offset_count,
                                                       errors=errors)
    if not errors and not migration_span.is_done:

        # obtain target database connection
        db_conn: Any = db_connect(engine=target_db,
                                  errors=errors)
        if db_conn:
            totals: tuple[int, int] = db_migrate_lobs(source_engine=source_db,
                                                      source_table=source_table,
                                                      source_lob_column=lob_column,
                                                      source_pk_columns=pk_columns,
                                                      target_engine=target_db,
                                                      target_table=target_table,
                                                      where_clause=where_clause,
                                                      offset_count=offset_count,
                                                      limit_count=limit_count,
                                                      chunk_size=chunk_size,
                                                      target_conn=db_conn,
                                                      log_trigger=int(limit_count / 2),
                                                      errors=errors)
            # acknowledge the migration
            if not errors:
                # obtain state DB connection
                pydb_conn: Any = db_connect(engine=PYDB_DBCTRL_ENGINE,
                                            errors=errors)
                if pydb_conn:
                    lob_count = totals[0]
                    lob_bytes = totals[1]
                    migration_span.nr_row_count = lob_count
                    migration_span.is_done = True
                    migration_span.update(db_engine=PYDB_DBCTRL_ENGINE,
                                          db_conn=pydb_conn,
                                          errors=errors)
                    if not errors:
                        migration_work.nr_row_count += lob_count
                        migration_work.update(db_engine=PYDB_DBCTRL_ENGINE,
                                              db_conn=pydb_conn,
                                              errors=errors)
                    if errors:
                        db_rollback_and_close(connection=pydb_conn,
                                              engine=PYDB_DBCTRL_ENGINE)
                    else:
                        db_commit_and_close(connection=pydb_conn,
                                            engine=PYDB_DBCTRL_ENGINE,
                                            errors=errors)
            if errors:
                db_rollback_and_close(connection=db_conn,
                                      engine=target_db)
            else:
                db_commit_and_close(connection=db_conn,
                                    engine=target_db,
                                    errors=errors)
    elif migration_span.is_done:
        op = "migration" if mig_step == MigStep.MIGRATE_LOBDATA else "correlation"
        logger.debug(f"LOB {op} from {source_db}.{source_table}.{lob_column} to "
                     f"{target_db}.{target_table}, "
                     f"offset {offset_count}, limit {limit_count}, already done")

    with lobdata_lock:
        if errors:
            MigrationIssue.new_issues(id_migration=migration.id,
                                      cd_step=mig_step,
                                      cd_type=IssueType.ERROR,
                                      ds_issues=errors)
            lobdata_registry[mother_thread][source_table]["errors"].extend(errors)
        else:
            lobdata_registry[mother_thread][source_table]["table-count"] += lob_count
            lobdata_registry[mother_thread][source_table]["table-bytes"] += lob_bytes


def _s3_migrate_lobs(session: Session,
                     mig_step: MigStep,
                     migration: Migration,
                     migration_work: MigrationWork,
                     mother_thread: int,
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

    errors: list[str] = []
    source_table: str = f"{session.nm_source_schema}.{migration_work.nm_table}"
    target_table: str = f"{session.nm_target_schema}.{migration_work.nm_table}"

    # register the operation thread (might be same as the mother thread)
    with lobdata_lock:
        lobdata_registry[mother_thread]["child-threads"].append(threading.get_ident())

    lob_count: int = 0
    lob_bytes: int = 0
    source_db: str = session.get_source_db().cd_engine
    target_s3: str = session.get_target_s3().cd_engine

    # obtain database connection
    source_conn: Any = db_connect(engine=source_db,
                                  errors=errors)
    if source_conn:
        # determine whether the migration needs to be carried out
        migration_span: MigrationSpan = get_migration_span(migration_work=migration_work,
                                                           first_row=offset_count,
                                                           errors=errors)
        if not errors and not migration_span.is_done:
            # obtain an S3 client
            s3_client = cs_get_client(engine=session.get_target_s3().cd_engine,
                                      errors=errors)
            if s3_client:
                temp_table: str | None = None
                if isinstance(where_clause, list):
                    # 'where_clause' is a list of 'reference_column' values indicating the LOBs to migrate
                    temp_table = f"{session.nm_source_schema}." + \
                                 f"{db_get_session_table_prefix(engine=source_db)}T_{lob_column}"[:30]
                    temp_column: str = f"id_{reference_column}"[:30]
                    db_create_session_table(engine=source_db,
                                            connection=source_conn,
                                            table_name=temp_table,
                                            column_data=[f"{temp_column} VARCHAR2(64) PRIMARY KEY"],
                                            errors=errors)
                    if errors:
                        temp_table = None
                    else:
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
                                       connection=source_conn,
                                       errors=errors)
                if not errors:
                    # 'target_table' is documentational, only
                    totals: tuple[int, int] = s3_migrate_lobs(migration=migration,
                                                              session=session,
                                                              mig_step=mig_step,
                                                              source_conn=source_conn,
                                                              s3_client=s3_client,
                                                              source_table=source_table,
                                                              target_table=target_table,
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
                    # acknowledge the migration
                    if not errors:
                        # obtain state DB connection
                        pydb_conn: Any = db_connect(engine=PYDB_DBCTRL_ENGINE,
                                                    errors=errors)
                        if pydb_conn:
                            lob_count = totals[0]
                            lob_bytes = totals[1]
                            migration_span.nr_row_count = lob_count
                            migration_span.is_done = True
                            migration_span.update(db_engine=PYDB_DBCTRL_ENGINE,
                                                  db_conn=pydb_conn,
                                                  errors=errors)
                            if not errors:
                                migration_work.nr_row_count += lob_count
                                migration_work.update(db_engine=PYDB_DBCTRL_ENGINE,
                                                      db_conn=pydb_conn,
                                                      errors=errors)
                            if errors:
                                db_rollback_and_close(connection=pydb_conn,
                                                      engine=PYDB_DBCTRL_ENGINE)
                            else:
                                db_commit_and_close(connection=pydb_conn,
                                                    engine=PYDB_DBCTRL_ENGINE,
                                                    errors=errors)
                if temp_table:
                    # disregard errors on dropping table
                    db_drop_table(table_name=temp_table,
                                  engine=source_db,
                                  connection=source_conn)
        elif migration_span.is_done:
            logger.debug(f"LOB migration from {source_db}.{source_table}.{lob_column} to "
                         f"{target_s3}, offset {offset_count}, limit {limit_count}, already done")
        if errors:
            db_rollback_and_close(connection=source_conn,
                                  engine=source_db)
        else:
            db_commit_and_close(connection=source_conn,
                                engine=source_db,
                                errors=errors)
    with lobdata_lock:
        if errors:
            MigrationIssue.new_issues(id_migration=migration.id,
                                      cd_step=mig_step,
                                      cd_type=IssueType.ERROR,
                                      ds_issues=errors)
            lobdata_registry[mother_thread][source_table]["errors"].extend(errors)
        else:
            lobdata_registry[mother_thread][source_table]["table-count"] += lob_count
            lobdata_registry[mother_thread][source_table]["table-bytes"] += lob_bytes
