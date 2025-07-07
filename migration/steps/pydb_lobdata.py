import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from enum import StrEnum
from logging import Logger
from pathlib import Path
from pypomes_core import (
    TIMEZONE_LOCAL, timestamp_duration, list_elem_starting_with
)
from pypomes_db import (
    DbEngine, db_connect,
    db_count, db_migrate_lobs, db_table_exists
)
from pypomes_s3 import S3Engine, s3_item_exists, s3_get_client
from typing import Any
from urlobject import URLObject

from app_constants import (
    MigConfig, MigMetric, MigSpec, MigSpot, DbConfig
)
from migration.pydb_common import build_channel_data
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob
from migration.steps.pydb_database import session_disable_restrictions
from migration.steps.pydb_s3 import s3_migrate_lobs

# _lobdata_threads: dict[int, dict[str, Any]] = {
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
_lobdata_threads: dict[int, dict[str, Any]] = {}
_lobdata_lock: threading.Lock = threading.Lock()


def migrate_lobs(errors: list[str],
                 session_id: str,
                 incremental_migrations: dict[str, tuple[int, int]],
                 migration_warnings: list[str],
                 migration_threads: list[int],
                 migrated_tables: dict[str, Any],
                 logger: Logger) -> tuple[int, int]:

    # initialize the return variables
    result_count: int = 0
    result_bytes: int = 0

    # add to the thread registration
    mother_thread: int = threading.get_ident()
    migration_threads.append(mother_thread)

    global _lobdata_threads
    with _lobdata_lock:
        _lobdata_threads[mother_thread] = {
            "child-threads": []
        }

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, Any] = session_registry[MigConfig.METRICS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]

    # retrieve the source and target DB and S3 engines
    source_db: DbEngine = session_spots[MigSpot.FROM_RDBMS]
    target_db: DbEngine = session_spots[MigSpot.TO_RDBMS]
    target_s3: S3Engine = session_spots[MigSpot.TO_S3]

    # retrieve the chunk size
    chunk_size: int = session_metrics[MigMetric.CHUNK_SIZE]

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
        with _lobdata_lock:
            _lobdata_threads[mother_thread][source_table] = {
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
        lob_columns: list[str] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            # migrating to S3 requires the lob column be mapped in 'named-lobdata'
            if is_lob(column_type) and \
                    (not target_s3 or
                     list_elem_starting_with(source=session_specs[MigSpec.NAMED_LOBDATA] or [],
                                             prefix=f"{table_name}.{column_name}=")):
                lob_columns.append(column_name)
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        if lob_columns:
            if not pk_columns:
                warn_msg: str = (f"Table {source_db}.{source_table} "
                                 f"is not eligible for LOB migration (no PKs)")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                # skip table migration
                continue

            # specific condition for migrating table LOBs to database
            if not target_s3 and not db_table_exists(errors=errors,
                                                     table_name=target_table,
                                                     engine=target_db,
                                                     logger=logger):
                # target table could not be found
                warn_msg: str = ("Unable to migrate LOBs, "
                                 f"table {target_db}.{target_table} was not found")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                # skip table migration
                continue

            # start migrating the source table LOBs
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            status: str = "ok"
            lob_count: int = 0
            lob_bytes: int = 0

            # process the existing LOB columns
            for lob_column in lob_columns:
                where_clause: str = f"{lob_column} IS NOT NULL"

                # count migrateable tuples on source table for 'lob_column'
                table_count: int = (db_count(errors=errors,
                                             table=source_table,
                                             where_clause=where_clause,
                                             engine=source_db) or 0) - offset_count
                if table_count > 0:
                    # migrate the column's LOBs
                    lob_prefix: Path | None = None
                    forced_filetype: str | None = None
                    ret_column: str | None = None

                    # specific condition for migrating column LOBs to S3
                    if target_s3:
                        # determine if lobdata in 'lob_column' is named in a reference column
                        for item in (session_specs[MigSpec.NAMED_LOBDATA] or []):
                            # format of item is '<table-name>.<column-name>=<named-column>[.<filetype>]'
                            if item.startswith(f"{table_name}.{lob_column}="):
                                ret_column = item[item.index("=")+1:]
                                pos: int = ret_column.find(".")
                                if pos > 0:
                                    forced_filetype = ret_column[pos:]
                                    ret_column = ret_column[:pos]
                                break

                        # obtain a S3 prefix for storing the lobdata
                        if not session_specs[MigSpec.FLATTEN_STORAGE]:
                            url: URLObject = URLObject(session_registry[target_db][DbConfig.HOST])
                            # 'url.hostname' returns 'None' for 'localhost'
                            host: str = f"{target_db}@{url.hostname or str(url)}"
                            ref_column: str = ret_column or lob_column
                            lob_prefix = Path(host,
                                              session_registry[target_db][DbConfig.NAME],
                                              target_schema,
                                              table_name,
                                              ref_column)

                            # is a nonempty S3 prefix an issue ?
                            if session_specs[MigSpec.SKIP_NONEMPTY] and \
                                    s3_item_exists(errors=errors,
                                                   prefix=lob_prefix):
                                # yes, skip it
                                warn_msg: str = f"Skipped nonempty {target_s3}.{lob_prefix.as_posix()}"
                                migration_warnings.append(warn_msg)
                                logger.warning(msg=warn_msg)
                                # skip column migration
                                continue

                    if not errors:
                        # build migration channel data
                        channel_data: list[tuple[int, int]] = \
                            build_channel_data(max_channels=session_metrics[MigMetric.LOBDATA_CHANNELS],
                                               channel_size=session_metrics[MigMetric.LOBDATA_CHANNEL_SIZE],
                                               table_count=table_count,
                                               offset_count=offset_count,
                                               limit_count=limit_count)
                        if len(channel_data) == 1:
                            # execute single task in current thread
                            if target_s3:
                                # migration target is S3
                                _s3_migrate_lobs(mother_thread=mother_thread,
                                                 session_id=session_id,
                                                 target_s3=target_s3,
                                                 target_table=target_table,
                                                 source_table=source_table,
                                                 lob_prefix=lob_prefix,
                                                 lob_column=lob_column,
                                                 pk_columns=pk_columns,
                                                 where_clause=where_clause,
                                                 limit_count=channel_data[0][0],
                                                 offset_count=channel_data[0][1],
                                                 forced_filetype=forced_filetype,
                                                 ret_column=ret_column,
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
                                                 offset_count=channel_data[0][1],
                                                 limit_count=channel_data[0][0],
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
                                                                         target_s3=target_s3,
                                                                         target_table=target_table,
                                                                         source_table=source_table,
                                                                         lob_prefix=lob_prefix,
                                                                         lob_column=lob_column,
                                                                         pk_columns=pk_columns,
                                                                         where_clause=where_clause,
                                                                         limit_count=channel_datum[0],
                                                                         offset_count=channel_datum[1],
                                                                         forced_filetype=forced_filetype,
                                                                         ret_column=ret_column,
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
                                                                         offset_count=channel_datum[1],
                                                                         limit_count=channel_datum[0],
                                                                         chunk_size=chunk_size,
                                                                         logger=logger)
                                    task_futures.append(future)

                                # wait for all task futures to complete, then shutdown down the executor
                                futures.wait(fs=task_futures)
                                executor.shutdown(wait=False)

                        with _lobdata_lock:
                            lob_count = _lobdata_threads[mother_thread][source_table]["table-count"]
                            lob_bytes = _lobdata_threads[mother_thread][source_table]["table-bytes"]
                            op_errors: list[str] = _lobdata_threads[mother_thread][source_table]["errors"]
                            if op_errors:
                                status = "error"
                                errors.extend(op_errors)

            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            table_data["lob-duration"] = duration
            table_data["lob-status"] = status
            table_data["lob-count"] = lob_count
            table_data["lob-bytes"] = lob_bytes
            target: str = f"S3 storage '{target_s3}'" if target_s3 else target_db
            logger.debug(msg=f"Migrated {lob_count} LOBs ({lob_bytes} bytes) in table "
                             f"{table_name}, from {source_db} to {target}, "
                             f"status {status}, duration {duration}")
            result_count += lob_count
            result_bytes += lob_bytes

    with _lobdata_lock:
        migration_threads.extend(_lobdata_threads[mother_thread]["child-threads"])
        _lobdata_threads.pop(mother_thread)

    return result_count, result_bytes


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

    # register the operation thread (might be same as mother thread)
    global _lobdata_threads
    with _lobdata_lock:
        _lobdata_threads[mother_thread]["child-threads"].append(threading.get_ident())

    # obtain a connection to the target database
    errors: list[str] = []
    lob_count: int = 0
    lob_bytes: int = 0
    target_conn: Any = db_connect(errors=errors,
                                  engine=target_engine)
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

    with _lobdata_lock:
        if errors:
            _lobdata_threads[mother_thread][source_table]["errors"].extend(errors)
        else:
            _lobdata_threads[mother_thread][source_table]["table-count"] += lob_count
            _lobdata_threads[mother_thread][source_table]["table-bytes"] += lob_bytes


def _s3_migrate_lobs(mother_thread: int,
                     session_id: str,
                     target_s3: S3Engine,
                     target_table: str,
                     source_table: str,
                     lob_prefix: Path,
                     lob_column: str,
                     pk_columns: list[str],
                     where_clause: str,
                     offset_count: int,
                     limit_count: int,
                     forced_filetype: str,
                     ret_column: str,
                     migration_warnings: list[str],
                     logger: Logger) -> None:

    global _lobdata_threads
    with _lobdata_lock:
        _lobdata_threads[mother_thread]["child-threads"].append(threading.get_ident())

    errors: list[str] = []
    s3_client = s3_get_client(errors=errors,
                              engine=target_s3,
                              logger=logger)
    if s3_client:
        # 'target_table' is documentational, only
        totals: tuple[int, int] = s3_migrate_lobs(errors=errors,
                                                  session_id=session_id,
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
                                                  ret_column=ret_column,
                                                  migration_warnings=migration_warnings,
                                                  logger=logger)
        with _lobdata_lock:
            _lobdata_threads[mother_thread][source_table]["table-count"] += totals[0]
            _lobdata_threads[mother_thread][source_table]["table-bytes"] += totals[1]
            if errors:
                _lobdata_threads[mother_thread][source_table]["errors"].extend(errors)
