import threading
from datetime import datetime
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from enum import StrEnum
from logging import Logger
from pathlib import Path
from pypomes_core import (
    TIMEZONE_LOCAL, timestamp_duration, list_correlate
)
from pypomes_db import DbEngine, db_connect, db_count, db_select
from pypomes_s3 import S3Engine, s3_get_client, s3_items_list
from typing import Any
from urlobject import URLObject

from app_constants import (
    MigConfig, MigMetric, MigSpec, MigSpot, DbConfig
)
from migration.pydb_common import build_channel_data
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob

# define the thread register
# _lobdata_threads: dict[int, dict[str, Any]] = {
#   <mother-thread> = {
#     "child-threads": [
#       <child-thread>,
#       ...
#     ],
#     "source-table-name": {
#       "table-count": <int>,
#       "table-deletes": <list[str]>,
#       "table-inserts": <list[str>,
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


def synchronize_lobs(errors: list[str],
                     session_id: str,
                     incremental_migrations: dict[str, tuple[int, int]],
                     migration_warnings: list[str],
                     migration_threads: list[int],
                     migrated_tables: dict[str, Any],
                     logger: Logger) -> tuple[int, int, int]:

    # initialize the return variables
    result_count: int = 0
    result_deletes: list[str] = []
    result_inserts: list[str] = []

    # add to the thread register
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

    # traverse list of migrated tables to copy the LOB data
    for table_name, table_data in migrated_tables.items():

        # error may come from previous iteration
        if errors or assert_session_abort(errors=errors,
                                          session_id=session_id,
                                          logger=logger):
            # abort the lobdata synchronization
            break

        source_schema: str = session_specs[MigSpec.FROM_SCHEMA]
        source_table: str = f"{source_schema}.{table_name}"
        target_schema: str = session_specs[MigSpec.TO_SCHEMA]
        target_table: str = f"{target_schema}.{table_name}"
        with _lobdata_lock:
            _lobdata_threads[mother_thread][source_table] = {
                "table-count": 0,
                "table-deletes": [],
                "table-inserts": [],
                "errors": []
            }

        # obtain limit and offset
        limit_count: int = 0
        offset_count: int = 0
        if table_name in incremental_migrations:
            limit_count, offset_count = incremental_migrations.get(table_name)

        # organize the information, using LOB types from the columns list
        lob_columns: list[tuple[str, str]] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            if is_lob(column_type):
                # synchronizing to S3 requires the lob column be mapped in 'named-lobdata'
                for item in (session_specs[MigSpec.NAMED_LOBDATA] or []):
                    # format of item is '<table-name>.<column-name>=<named-column>[.<filetype>]'
                    if item.startswith(f"{table_name}.{column_name}="):
                        lob_columns.append((column_name, item[item.index("=")+1:]))
                        break
        if lob_columns:
            # start synchronizing the LOBs in the S3 folder with its corresponding source table column
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            status: str = "ok"
            lob_count: int = 0
            lob_deletes: list[str] = []
            lob_inserts: list[str] = []

            # process the existing LOB columns
            for lob_column, return_column in lob_columns:
                where_clause: str = f"{lob_column} IS NOT NULL"
                pos: int = return_column.find(".")
                if pos > 0:
                    forced_filetype = return_column[pos:]
                    return_column = return_column[:pos]

                # count synchronizeable tuples on source table for 'lob_column'
                table_count: int = (db_count(errors=errors,
                                             table=source_table,
                                             where_clause=where_clause,
                                             engine=source_db) or 0) - offset_count
                if table_count > 0:
                    warn_msg: str = ("Expecting an index to exist on column "
                                     f"{source_db}.{source_table}.{return_column}")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)

                    # obtain a S3 prefix for storing the lobdata
                    url: URLObject = URLObject(session_registry[target_db][DbConfig.HOST])
                    # 'url.hostname' returns 'None' for 'localhost'
                    host: str = f"{target_db}@{url.hostname or str(url)}"
                    lob_prefix: Path = Path(host,
                                            session_registry[target_db][DbConfig.NAME],
                                            target_schema,
                                            table_name,
                                            return_column)

                    # build migration channel data
                    channel_data: list[tuple[int, int]] = \
                        build_channel_data(max_channels=session_metrics[MigMetric.LOBDATA_CHANNELS],
                                           channel_size=session_metrics[MigMetric.LOBDATA_CHANNEL_SIZE],
                                           table_count=table_count,
                                           offset_count=offset_count,
                                           limit_count=limit_count)
                    if len(channel_data) == 1:
                        # execute single task in current thread
                        _compute_lob_lists(mother_thread=mother_thread,
                                           source_db=source_db,
                                           source_table=source_table,
                                           source_column=return_column,
                                           s3_engine=target_s3,
                                           limit_count=channel_data[0][0],
                                           offset_count=channel_data[0][1],
                                           prefix=lob_prefix,
                                           logger=logger)
                    else:
                        target: str = f"S3 storage '{target_s3}'" \
                            if target_s3 else f"{target_db}.{target_table}.{lob_column}"
                        logger.debug(msg=f"Started synchronizing {sum(c[0] for c in channel_data)} LOBs "
                                         f"in {target} with {source_db}.{source_table}.{lob_column}, "
                                         f"using {len(channel_data)} channels")

                        # execute tasks concurrently
                        with ThreadPoolExecutor(max_workers=len(channel_data)) as executor:
                            task_futures: list[Future] = []
                            for channel_datum in channel_data:
                                future: Future = executor.submit(_compute_lob_lists,
                                                                 mother_thread=mother_thread,
                                                                 source_db=source_db,
                                                                 source_table=source_table,
                                                                 source_column=return_column,
                                                                 s3_engine=target_s3,
                                                                 limit_count=channel_datum[0],
                                                                 offset_count=channel_datum[1],
                                                                 prefix=lob_prefix,
                                                                 logger=logger)
                                task_futures.append(future)

                            # wait for all task futures to complete, then shutdown down the executor
                            futures.wait(fs=task_futures)
                            executor.shutdown(wait=False)

                        with _lobdata_lock:
                            lob_count = _lobdata_threads[mother_thread][source_table]["table-count"]
                            lob_deletes = _lobdata_threads[mother_thread][source_table]["table-deletes"]
                            lob_inserts = _lobdata_threads[mother_thread][source_table]["table-inserts"]
                            op_errors: list[str] = _lobdata_threads[mother_thread][source_table]["errors"]
                            if op_errors:
                                status = "error"
                                errors.extend(op_errors)

            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            secs: float = (finished - started).total_seconds()
            lob_inserts.sort()
            lob_deletes.sort()
            lob_inserts, lob_deletes = list_correlate(list_first=lob_inserts,
                                                      list_second=lob_deletes,
                                                      only_in_first=True,
                                                      only_in_second=True,
                                                      bin_search=True)
            table_data.update({
                "lob-status": status,
                "lob-count": lob_count,
                "lob-deletes": len(lob_deletes),
                "lob-inserts": len(lob_inserts),
                "lob-duration": duration,
                "lob-performance": f"{lob_count/secs:.2f} LOBs/s"
            })
            target: str = f"S3 storage '{target_s3}'" if target_s3 else target_db
            logger.debug(msg=f"Synchronized {lob_count} LOBs in {target} with "
                             f"{source_db}.{table_name}, status {status}, duration {duration}")
            result_count += lob_count
            result_deletes.extend(lob_deletes)
            result_inserts.extend(lob_inserts)

    with _lobdata_lock:
        migration_threads.extend(_lobdata_threads[mother_thread]["child-threads"])
        _lobdata_threads.pop(mother_thread)

    return result_count, len(result_deletes), len(result_inserts)


def _compute_lob_lists(mother_thread: int,
                       source_db: DbEngine,
                       source_table: str,
                       source_column: str,
                       s3_engine: S3Engine,
                       limit_count: int,
                       offset_count: int,
                       prefix: Path,
                       logger: Logger) -> None:

    # register the operation thread (might be same as mother thread)
    global _lobdata_threads
    with _lobdata_lock:
        _lobdata_threads[mother_thread]["child-threads"].append(threading.get_ident())

    # initialize the counter and the lists
    lob_count: int = 0
    lob_deletes: list[str] = []
    lob_inserts: list[str] = []

    # obtain a connection to the database
    errors: list[str] = []
    db_conn: Any = db_connect(errors=errors,
                              autocommit=True,
                              engine=source_db,
                              logger=logger)
    if not errors:
        # obtain an S3 client
        s3_client: Any = s3_get_client(errors=errors,
                                       engine=s3_engine,
                                       logger=logger)
        if not errors:
            # if there is an offset, include the previous tuple to function as 'start_after'
            from_first: bool = offset_count == 0
            db_items: list[tuple[str]] = db_select(errors=errors,
                                                   sel_stmt=f"SELECT {source_column} FROM {source_table}",
                                                   orderby_clause=source_column,
                                                   offset_count=offset_count if from_first else offset_count - 1,
                                                   limit_count=limit_count if from_first else limit_count + 1,
                                                   connection=db_conn,
                                                   logger=logger)
            if not errors:
                pos: int = 0
                start_after: str | None = None
                if not from_first:
                    start_after = (Path(prefix) / db_items[0][0]).as_posix()
                    pos = 1
                db_names: list[str] = [db_item[0] for db_item in db_items[pos:]]
                db_items.clear()

                s3_items: list[dict[str, Any]] = \
                    s3_items_list(errors=errors,
                                  max_count=limit_count,
                                  client=s3_client,
                                  start_after=start_after,
                                  prefix=prefix,
                                  logger=logger)
                if not errors:
                    s3_names: list[str] = []
                    for s3_item in s3_items:
                        name: str = s3_item.get("Key")
                        pos: int = name.rfind("/")
                        if pos > 0:
                            name = name[pos+1:]
                        s3_names.append(name)

                    correlations: tuple = list_correlate(list_first=db_names,
                                                         list_second=s3_names,
                                                         only_in_first=True,
                                                         only_in_second=True,
                                                         bin_search=True)
                    lob_count = len(db_names)
                    lob_inserts = correlations[0]
                    lob_deletes = correlations[1]

    with _lobdata_lock:
        if errors:
            _lobdata_threads[mother_thread][source_table]["errors"].extend(errors)
        else:
            _lobdata_threads[mother_thread][source_table]["table-count"] += lob_count
            _lobdata_threads[mother_thread][source_table]["table-deletes"].extend(lob_deletes)
            _lobdata_threads[mother_thread][source_table]["table-inserts"].extend(lob_inserts)
