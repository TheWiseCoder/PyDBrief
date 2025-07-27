import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from enum import StrEnum
from logging import Logger
from pathlib import Path
from pypomes_core import TIMEZONE_LOCAL, timestamp_duration
from pypomes_db import (
    DbEngine, db_connect, db_count,
    db_migrate_lobs, db_table_exists, db_bulk_insert,
    db_create_session_table, db_get_session_table_prefix
)
from pypomes_s3 import S3Engine, s3_item_exists, s3_get_client
from typing import Any

from app_constants import (
    MigConfig, MigMetric, MigSpec, MigSpot, MigStep
)
from migration.pydb_common import build_channel_data, build_lob_prefix
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob
from migration.steps.pydb_database import session_disable_restrictions
from migration.steps.pydb_s3 import s3_migrate_lobs

# structure of the thread register:
# lobdata_register: dict[int, dict[str, Any]] = {
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
lobdata_register: dict[int, dict[str, Any]] = {}
lobdata_lock: threading.Lock = threading.Lock()


def migrate_lob_tables(errors: list[str],
                       session_id: str,
                       incremental_migrations: dict[str, tuple[int, int]],
                       migration_warnings: list[str],
                       migration_threads: list[int],
                       migrated_tables: dict[str, Any],
                       logger: Logger) -> tuple[int, int]:

    # initialize the return variables
    result_count: int = 0
    result_bytes: int = 0

    # add to the thread register
    mother_thread: int = threading.get_ident()
    migration_threads.append(mother_thread)
    global lobdata_register
    with lobdata_lock:
        lobdata_register[mother_thread] = {
            "child-threads": []
        }

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]

    # retrieve the source and target DB and S3 engines
    source_db: DbEngine = session_spots[MigSpot.FROM_RDBMS]
    target_db: DbEngine = session_spots[MigSpot.TO_RDBMS]
    target_s3: S3Engine = session_spots[MigSpot.TO_S3]

    # traverse list of migrated tables to copy the LOB data
    for table_name, table_data in migrated_tables.items():

        # error may come from previous iteration
        if errors or assert_session_abort(errors=errors,
                                          session_id=session_id,
                                          logger=logger):
            # abort the lobdata migration
            break

        source_schema: str = session_specs[MigSpec.FROM_SCHEMA]
        source_table: str = f"{source_schema}.{table_name}"
        target_schema: str = session_specs[MigSpec.TO_SCHEMA]
        target_table: str = f"{target_schema}.{table_name}"
        with lobdata_lock:
            lobdata_register[mother_thread][source_table] = {
                "table-count": 0,
                "table-bytes": 0,
                "errors": []
            }

        # obtain limit and offset
        limit_count: int = 0
        offset_count: int = 0
        if table_name in incremental_migrations:
            limit_count, offset_count = incremental_migrations.get(table_name)

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
        lob_columns: list[tuple[str, str]] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            # migrating to S3 requires the lob column be mapped in 'named-lobdata'
            if is_lob(column_type):
                reference_column: str | None = None
                # determine if lobdata in 'lob_column' has its filename defined 'named-lobdata'
                for item in (session_specs[MigSpec.NAMED_LOBDATA] or []):
                    # format of item is '<table-name>.<column-name>=<named-column>[.<filetype>]'
                    if item.startswith(f"{table_name}.{column_name}="):
                        reference_column = item[item.index("=")+1:]
                        break
                lob_columns.append((column_name, reference_column))
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        if lob_columns:
            # specific condition for migrating table LOBs to database
            if not target_s3 and not db_table_exists(errors=errors,
                                                     table_name=target_table,
                                                     engine=target_db,
                                                     logger=logger):
                # target table could not be found (might be due to error)
                warn_msg: str = ("Unable to migrate LOBs, "
                                 f"table {target_db}.{target_table} was not found")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                # skip table migration
                continue

            # start migrating the source table LOBs
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            status: str = "ok"
            migrate_lob_columns(errors=errors,
                                mother_thread=mother_thread,
                                session_id=session_id,
                                source_db=source_db,
                                target_db=target_db,
                                target_s3=target_s3,
                                source_table=source_table,
                                target_table=target_table,
                                lob_columns=lob_columns,
                                pk_columns=pk_columns,
                                lob_tuples={},
                                offset_count=offset_count,
                                limit_count=limit_count,
                                migration_warnings=migration_warnings,
                                logger=logger)
            with lobdata_lock:
                lob_count: int = lobdata_register[mother_thread][source_table]["table-count"]
                lob_bytes: int = lobdata_register[mother_thread][source_table]["table-bytes"]
                op_errors: list[str] = lobdata_register[mother_thread][source_table]["errors"]
                if op_errors:
                    status = "error"
                    errors.extend(op_errors)

            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            secs: float = (finished - started).total_seconds()
            table_data.update({
                "lob-status": status,
                "lob-count": lob_count,
                "lob-bytes": lob_bytes,
                "lob-duration": duration,
                "lob-performance": f"{lob_count/secs:.2f} LOBs/s, {lob_bytes/secs:.2f} bytes/s"
            })
            target: str = f"S3 storage '{target_s3}'" if target_s3 else target_db
            logger.debug(msg=f"Migrated {lob_count} LOBs ({lob_bytes} bytes) in table "
                             f"{table_name}, from {source_db} to {target}, "
                             f"status {status}, duration {duration}")
            result_count += lob_count
            result_bytes += lob_bytes

    with lobdata_lock:
        migration_threads.extend(lobdata_register[mother_thread]["child-threads"])
        lobdata_register.pop(mother_thread)

    return result_count, result_bytes


def migrate_lob_columns(errors: list[str],
                        mother_thread: int,
                        session_id: str,
                        source_db: DbEngine,
                        target_db: DbEngine,
                        target_s3: S3Engine,
                        source_table: str,
                        target_table: str,
                        pk_columns: list[str],
                        lob_columns: list[tuple[str, str]],
                        lob_tuples: dict[str, list[str]],
                        offset_count: int,
                        limit_count: int,
                        migration_warnings: list[str],
                        logger: Logger) -> None:

    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_metrics: dict[MigMetric, Any] = session_registry[MigConfig.METRICS]
    channel_count: int = session_metrics[MigMetric.LOBDATA_CHANNELS]
    channel_size: int = session_metrics[MigMetric.LOBDATA_CHANNEL_SIZE]
    chunk_size: int = session_metrics[MigMetric.CHUNK_SIZE]

    # process the existing LOB columns
    for lob_column, reference_column in lob_columns:

        where_clause: Any
        table_count: int
        lob_prefix: Path | None = None
        forced_filetype: str | None = None

        # specific handlings for migrating 'lob_column' to S3
        if target_s3:
            if not reference_column and not pk_columns:
                warn_msg: str = (f"Column {source_db}.{source_table}.{lob_column} "
                                 "is not eligible for LOB migration to S3 "
                                 "(not mapped in 'named-lobdata', and no PKs in table)")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                # skip table migration
                continue

            # define a forced file type
            if reference_column:
                pos: int = reference_column.find(".")
                if pos > 0:
                    # 'forced_filetype' includes the leading dot ('.')
                    forced_filetype = reference_column[pos:]
                    reference_column = reference_column[:pos]

            # obtain an S3 prefix for storing the lobdata
            if not session_specs[MigSpec.FLATTEN_STORAGE]:
                lob_prefix = build_lob_prefix(session_registry=session_registry,
                                              target_db=target_db,
                                              target_table=target_table,
                                              column_name=reference_column)

                # is a nonempty S3 prefix an issue ?
                if (not session_registry[MigConfig.STEPS][MigStep.SYNCHRONIZE_LOBDATA] and
                    session_specs[MigSpec.SKIP_NONEMPTY] and
                    s3_item_exists(errors=errors,
                                   prefix=lob_prefix)):
                    # yes, skip it
                    warn_msg: str = ("Skipped migrating LOBs in column "
                                     f"{source_db}.{source_table}.{lob_column}: "
                                     f"folder {target_s3}.{lob_prefix.as_posix()} is not empty")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)
                    # skip column migration
                    continue

            if not pk_columns:
                warn_msg: str = ("Expecting an index to exist on column "
                                 f"{source_db}.{source_table}.{reference_column}, as table has no PKs")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                pk_columns = [reference_column]

        # count migrateable tuples on source table for 'lob_column'
        if lob_tuples:
            # 'where_clause' has the list of 'reference_column' values indicating the LOBs to be migrated
            where_clause = lob_tuples.get(lob_column)
            table_count = len(where_clause)
        else:
            where_clause = f"{lob_column} IS NOT NULL"
            table_count = (db_count(errors=errors,
                                    table=source_table,
                                    where_clause=where_clause,
                                    engine=source_db) or 0) - offset_count

        # migrate the LOBs in 'lob_column'
        if not errors and table_count > 0:
            # build migration channel data
            channel_data: list[tuple[int, int]] = build_channel_data(max_channels=channel_count,
                                                                     channel_size=channel_size,
                                                                     table_count=table_count,
                                                                     offset_count=offset_count,
                                                                     limit_count=limit_count)
            if len(channel_data) == 1:
                # execute single task in current thread
                if target_s3:
                    # migration target is S3
                    _s3_migrate_lobs(mother_thread=mother_thread,
                                     session_id=session_id,
                                     source_db=source_db,
                                     source_table=source_table,
                                     target_s3=target_s3,
                                     target_table=target_table,
                                     lob_prefix=lob_prefix,
                                     lob_column=lob_column,
                                     pk_columns=pk_columns,
                                     where_clause=where_clause,
                                     limit_count=channel_data[0][0],
                                     offset_count=channel_data[0][1],
                                     forced_filetype=forced_filetype,
                                     reference_column=reference_column,
                                     migration_warnings=migration_warnings,
                                     logger=logger)
                else:
                    # migration target is database
                    _db_migrate_lobs(mother_thread=mother_thread,
                                     source_engine=source_db,
                                     source_table=source_table,
                                     lob_column=lob_column,
                                     pk_columns=pk_columns,
                                     target_engine=target_db,
                                     target_table=target_table,
                                     where_clause=where_clause,
                                     limit_count=channel_data[0][0],
                                     offset_count=channel_data[0][1],
                                     chunk_size=chunk_size,
                                     logger=logger)
            else:
                target: str = f"S3 storage '{target_s3}'" \
                    if target_s3 else f"{target_db}.{target_table}.{lob_column}"
                logger.debug(msg=f"Started migrating {sum(c[0] for c in channel_data)} LOBs "
                             f"from {source_db}.{source_table}.{lob_column} to {target}, "
                             f"using {len(channel_data)} channels")

                # execute tasks concurrently
                with ThreadPoolExecutor(max_workers=len(channel_data)) as executor:
                    task_futures: list[Future] = []
                    for channel_datum in channel_data:
                        if target_s3:
                            # migration target is S3
                            future: Future = executor.submit(_s3_migrate_lobs,
                                                             mother_thread=mother_thread,
                                                             session_id=session_id,
                                                             source_db=source_db,
                                                             source_table=source_table,
                                                             target_s3=target_s3,
                                                             target_table=target_table,
                                                             lob_prefix=lob_prefix,
                                                             lob_column=lob_column,
                                                             pk_columns=pk_columns,
                                                             where_clause=where_clause,
                                                             limit_count=channel_datum[0],
                                                             offset_count=channel_datum[1],
                                                             forced_filetype=forced_filetype,
                                                             reference_column=reference_column,
                                                             migration_warnings=migration_warnings,
                                                             logger=logger)
                        else:
                            # migration target is database
                            future: Future = executor.submit(_db_migrate_lobs,
                                                             mother_thread=mother_thread,
                                                             source_engine=source_db,
                                                             source_table=source_table,
                                                             lob_column=lob_column,
                                                             pk_columns=pk_columns,
                                                             target_engine=target_db,
                                                             target_table=target_table,
                                                             where_clause=where_clause,
                                                             limit_count=channel_datum[0],
                                                             offset_count=channel_datum[1],
                                                             chunk_size=chunk_size,
                                                             logger=logger)
                        task_futures.append(future)

                    # wait for all task futures to complete, then shutdown down the executor
                    futures.wait(fs=task_futures)
                    executor.shutdown(wait=False)


def _db_migrate_lobs(mother_thread: int,
                     source_engine: DbEngine,
                     source_table: str,
                     lob_column: str,
                     pk_columns: list[str],
                     target_engine: DbEngine,
                     target_table: str,
                     where_clause: str,
                     limit_count: int,
                     offset_count: int,
                     chunk_size: int,
                     logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    global lobdata_register
    with lobdata_lock:
        lobdata_register[mother_thread]["child-threads"].append(threading.get_ident())

    # initialize the counters
    lob_count: int = 0
    lob_bytes: int = 0

    # obtain a connection to the target database
    errors: list[str] = []
    target_conn: Any = db_connect(errors=errors,
                                  engine=target_engine,
                                  logger=logger)
    if target_conn:
        # disable triggers and rules to speed-up migration
        session_disable_restrictions(errors=errors,
                                     rdbms=target_engine,
                                     conn=target_conn,
                                     logger=logger)
        if not errors:
            totals: tuple[int, int] = db_migrate_lobs(errors=errors,
                                                      source_engine=source_engine,
                                                      source_table=source_table,
                                                      source_lob_column=lob_column,
                                                      source_pk_columns=pk_columns,
                                                      target_engine=target_engine,
                                                      target_table=target_table,
                                                      target_conn=target_conn,
                                                      target_committable=True,
                                                      where_clause=where_clause,
                                                      limit_count=limit_count,
                                                      offset_count=offset_count,
                                                      chunk_size=chunk_size,
                                                      logger=logger)
            if totals:
                lob_count = totals[0]
                lob_bytes = totals[1]

    with lobdata_lock:
        if errors:
            lobdata_register[mother_thread][source_table]["errors"].extend(errors)
        else:
            lobdata_register[mother_thread][source_table]["table-count"] += lob_count
            lobdata_register[mother_thread][source_table]["table-bytes"] += lob_bytes


def _s3_migrate_lobs(mother_thread: int,
                     session_id: str,
                     source_db: DbEngine,
                     source_table: str,
                     target_s3: S3Engine,
                     target_table: str,
                     lob_prefix: Path,
                     lob_column: str,
                     pk_columns: list[str],
                     where_clause: Any,
                     limit_count: int,
                     offset_count: int,
                     forced_filetype: str,
                     reference_column: str,
                     migration_warnings: list[str],
                     logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    global lobdata_register
    with lobdata_lock:
        lobdata_register[mother_thread]["child-threads"].append(threading.get_ident())

    # obtain an S3 client
    errors: list[str] = []
    s3_client = s3_get_client(errors=errors,
                              engine=target_s3,
                              logger=logger)
    if s3_client:
        # obtain a database connection
        db_conn: Any = db_connect(errors=errors,
                                  logger=logger)
        if db_conn:
            if isinstance(where_clause, list):
                # 'where_clause' is a list of 'reference_column' values indicating the LOBs to migrate
                temp_table: str = f"{db_get_session_table_prefix(engine=source_db)}T_{lob_column}"
                temp_column: str = f"id_{reference_column}"
                db_create_session_table(errors=errors,
                                        engine=source_db,
                                        connection=db_conn,
                                        table_name=temp_table,
                                        column_data=[f"{temp_column} VARCHAR2(64) PRIMARY KEY"],
                                        logger=logger)
                if not errors:
                    # the exact sublist of LOBs to be migrated by this thread is inserted
                    db_bulk_insert(errors=errors,
                                   target_table=temp_table,
                                   insert_attrs=[temp_column],
                                   insert_vals=[tuple(where_clause[offset_count:limit_count+offset_count])],
                                   engine=source_db,
                                   connection=db_conn)
                    if not errors:
                        # no offset/limit herefrom, as 'where_clause' alone precisely filters the relevant LOBs
                        offset_count = 0
                        limit_count = 0
                        where_clause = f"{reference_column} IN (SELECT {temp_column} FROM {temp_table})"

        if not errors:
            # 'target_table' is documentational, only
            totals: tuple[int, int] = s3_migrate_lobs(errors=errors,
                                                      session_id=session_id,
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
                                                      migration_warnings=migration_warnings,
                                                      logger=logger)
            with lobdata_lock:
                lobdata_register[mother_thread][source_table]["table-count"] += totals[0]
                lobdata_register[mother_thread][source_table]["table-bytes"] += totals[1]
                if errors:
                    lobdata_register[mother_thread][source_table]["errors"].extend(errors)
