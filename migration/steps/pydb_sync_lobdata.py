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
from pypomes_db import (
    DbEngine, db_connect, db_count, db_select
)
from pypomes_s3 import (
    S3Engine, s3_get_client, s3_items_list, s3_item_remove
)
from typing import Any

import migration.steps.pydb_lobdata as lob_ctrl
from app_constants import (
    MigConfig, MigMetric, MigSpec, MigSpot
)
from migration.pydb_common import build_channel_data, build_lob_prefix
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob
from migration.steps.pydb_lobdata import migrate_lob_columns

# structure of the thread register:
# lob_ctrl.lobdata_register: dict[int, dict[str, Any]] = {
#   <mother-thread> = {
#     "child-threads": [
#       <child-thread>,
#       ...
#     ],
#     "source-table-name": {
#       "table-count": <int>,
#       "table-deletes": <int>,
#       "table-inserts": <int>,
#       "errors": [
#         <error>,
#         ...
#       ],
#       "<lob-column-n>-deletes: list[str],
#       "<lob-column-n>-inserts: list[str],
#       ...
#     },
#   },
#   ...
# }


def synchronize_lobs(errors: list[str],
                     session_id: str,
                     incremental_migrations: dict[str, tuple[int, int]],
                     migration_warnings: list[str],
                     migration_threads: list[int],
                     migrated_tables: dict[str, Any],
                     logger: Logger) -> tuple[int, int, int]:

    # initialize the return variables
    result_count: int = 0
    result_deletes: int = 0
    result_inserts: int = 0

    # add to the thread register
    mother_thread: int = threading.get_ident()
    migration_threads.append(mother_thread)
    with lob_ctrl.lobdata_lock:
        lob_ctrl.lobdata_register[mother_thread] = {
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
        with lob_ctrl.lobdata_lock:
            lob_ctrl.lobdata_register[mother_thread][source_table] = {
                "table-count": 0,
                "errors": []
            }

        # obtain limit and offset
        limit_count: int = 0
        offset_count: int = 0
        # ----------- to be removed
        if table_name in incremental_migrations:
            limit_count, offset_count = incremental_migrations.get(table_name)

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
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
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        if lob_columns:
            # start synchronizing the LOBs in the S3 folder with its corresponding source table column
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            status: str = "ok"
            lob_count: int = 0
            delete_count: int = 0
            insert_count: int = 0
            table_deletes: dict[str, list[str]] = {}
            table_inserts: dict[str, list[str]] = {}

            # process the existing LOB columns
            for lob_column, reference_column in lob_columns:
                where_clause: str = f"{lob_column} IS NOT NULL"
                # 'reference_column' might contain a forced filetype specification
                pos: int = reference_column.find(".")
                if pos > 0:
                    reference_column = reference_column[:pos]

                # count synchronizeable tuples on source table for 'lob_column'
                table_count: int = (db_count(errors=errors,
                                             table=source_table,
                                             where_clause=where_clause,
                                             engine=source_db) or 0) - offset_count
                if table_count > 0:
                    warn_msg: str = ("Expecting an index to exist on column "
                                     f"{source_db}.{source_table}.{reference_column}")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)

                    # start synchronizing 'lob_column'
                    with lob_ctrl.lobdata_lock:
                        lob_ctrl.lobdata_register[mother_thread][source_table].update({
                            f"{lob_column}-deletes": [],
                            f"{lob_column}-inserts": []
                        })
                    # obtain an S3 prefix for storing the lobdata
                    lob_prefix = build_lob_prefix(session_registry=session_registry,
                                                  target_db=target_db,
                                                  target_table=target_table,
                                                  column_name=reference_column)
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
                                           source_column=reference_column,
                                           lob_column=lob_column,
                                           where_clause=where_clause,
                                           s3_engine=target_s3,
                                           limit_count=channel_data[0][0],
                                           offset_count=channel_data[0][1],
                                           lob_prefix=lob_prefix,
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
                                                                 source_column=reference_column,
                                                                 lob_column=lob_column,
                                                                 where_clause=where_clause,
                                                                 s3_engine=target_s3,
                                                                 lob_prefix=lob_prefix,
                                                                 limit_count=channel_datum[0],
                                                                 offset_count=channel_datum[1],
                                                                 logger=logger)
                                task_futures.append(future)

                            # wait for all task futures to complete, then shutdown down the executor
                            futures.wait(fs=task_futures)
                            executor.shutdown(wait=False)

                    col_deletes: list[str] = []
                    col_inserts: list[str] = []
                    with lob_ctrl.lobdata_lock:
                        table_data: dict[str, Any] = lob_ctrl.lobdata_register[mother_thread][source_table]
                        op_errors: list[str] = table_data.get("errors")
                        if op_errors:
                            status = "error"
                            errors.extend(op_errors)
                        else:
                            lob_count += table_data.get("table-count")
                            col_deletes = table_data.get(f"{lob_column}-deletes")
                            col_inserts = table_data.get(f"{lob_column}-inserts")

                    # 'col_inserts' and 'col_deletes' are sorted
                    col_inserts, col_deletes = list_correlate(list_first=col_inserts,
                                                              list_second=col_deletes,
                                                              only_in_first=True,
                                                              only_in_second=True,
                                                              bin_search=True)
                    delete_count += len(col_deletes)
                    insert_count += len(col_inserts)
                    table_deletes[lob_column] = col_deletes
                    table_inserts[lob_column] = col_inserts

            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            secs: float = (finished - started).total_seconds()
            table_data.update({
                "lob-status": status,
                "lob-count": lob_count,
                "lob-deletes": delete_count,
                "lob-inserts": insert_count,
                "lob-duration": duration,
                "lob-performance": f"{lob_count/secs:.2f} LOBs/s"
            })
            target: str = f"S3 storage '{target_s3}'" if target_s3 else target_db
            logger.debug(msg=f"Synchronized {lob_count} LOBs in {target} with "
                             f"{source_db}.{table_name}, status {status}, duration {duration}")
            result_count += lob_count
            result_deletes += delete_count
            result_inserts += insert_count

            # migrate the LOBs in 'table_inserts'
            migrate_lob_columns(errors=errors,
                                mother_thread=mother_thread,
                                session_id=session_id,
                                source_db=source_db,
                                target_db=target_db,
                                target_s3=target_s3,
                                source_table=source_table,
                                target_table=target_table,
                                pk_columns=pk_columns,
                                lob_columns=lob_columns,
                                lob_tuples=table_inserts,
                                offset_count=0,
                                limit_count=0,
                                migration_warnings=migration_warnings,
                                logger=logger)

            # remove the LOBs in 'table_deletes'
            if not errors:
                for lob_deletes in list(table_deletes.values()):
                    for lob_delete in lob_deletes:
                        s3_item_remove(errors=errors,
                                       identifier=lob_delete,
                                       engine=target_s3,
                                       logger=logger)
                        if errors:
                            break

    with lob_ctrl.lobdata_lock:
        migration_threads.extend(lob_ctrl.lobdata_register[mother_thread]["child-threads"])
        lob_ctrl.lobdata_register.pop(mother_thread)

    return result_count, result_deletes, result_inserts


def _compute_lob_lists(mother_thread: int,
                       source_db: DbEngine,
                       source_table: str,
                       source_column: str,
                       lob_column: str,
                       where_clause: str,
                       s3_engine: S3Engine,
                       lob_prefix: Path,
                       limit_count: int,
                       offset_count: int,
                       logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    with lob_ctrl.lobdata_lock:
        lob_ctrl.lobdata_register[mother_thread]["child-threads"].append(threading.get_ident())

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
            # if there is an offset, include the previous tuple to function as 'start_after' afterwards
            from_first: bool = offset_count == 0
            db_items: list[tuple[str]] = db_select(errors=errors,
                                                   sel_stmt=f"SELECT {source_column} FROM {source_table}",
                                                   where_clause=where_clause,
                                                   orderby_clause=source_column,
                                                   offset_count=offset_count if from_first else offset_count - 1,
                                                   limit_count=limit_count if from_first else limit_count + 1,
                                                   connection=db_conn,
                                                   logger=logger)
            if not errors:
                pos: int = 0
                start_after: str | None = None
                if not from_first:
                    start_after = (Path(lob_prefix) / db_items[0][0]).as_posix()
                    pos = 1
                db_names: list[str] = [db_item[0] for db_item in db_items[pos:]]
                db_items.clear()

                s3_items: list[dict[str, Any]] = \
                    s3_items_list(errors=errors,
                                  max_count=limit_count,
                                  client=s3_client,
                                  start_after=start_after,
                                  prefix=lob_prefix,
                                  logger=logger)
                if not errors:
                    s3_full: dict[str, str] = {}
                    s3_names: list[str] = []
                    for s3_item in s3_items:
                        full_name: str = s3_item.get("Key")
                        name = full_name
                        # extract the prefix
                        pos: int = name.rfind("/")
                        if pos > 0:
                            name = name[pos+1:]
                        # extract the extension
                        pos = name.rfind(".")
                        if pos > 0:
                            name = name[:pos]
                        s3_names.append(name)
                        s3_full[name] = full_name

                    # obtain lists of tuples to migrate to, and remove from, S3 storage
                    correlations: tuple = list_correlate(list_first=db_names,
                                                         list_second=s3_names,
                                                         only_in_first=True,
                                                         only_in_second=True,
                                                         bin_search=True)
                    lob_count = len(db_names)
                    lob_inserts = correlations[0]

                    # put the full names in the 'lob_deletes' list
                    lob_deletes = [s3_full.get(name) for name in correlations[1]]

    with lob_ctrl.lobdata_lock:
        if errors:
            lob_ctrl.lobdata_register[mother_thread][source_table]["errors"].extend(errors)
        else:
            lob_ctrl.lobdata_register[mother_thread][source_table]["table-count"] += lob_count
            lob_ctrl.lobdata_register[mother_thread][source_table][f"{lob_column}-deletes"].extend(lob_deletes)
            lob_ctrl.lobdata_register[mother_thread][source_table][f"{lob_column}-inserts"].extend(lob_inserts)
