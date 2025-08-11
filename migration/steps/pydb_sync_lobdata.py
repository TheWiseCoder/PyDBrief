import threading
from datetime import datetime
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from enum import StrEnum
from logging import Logger
from pathlib import Path
from pypomes_core import (
    TZ_LOCAL,
    timestamp_duration, list_correlate, list_prune_duplicates
)
from pypomes_db import (
    DbEngine, db_connect, db_count, db_select
)
from pypomes_s3 import (
    S3Engine, s3_get_client, s3_prefix_list, s3_items_remove
)
from typing import Any

import migration.steps.pydb_lobdata as lob_ctrl
from app_constants import (
    MigConfig, MigMetric, MigSpec, MigSpot
)
from migration.pydb_common import build_channel_data, build_lob_prefix
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob_column
from migration.steps.pydb_database import session_setup
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
#       "table-bytes": <int>,
#       "table-deletes": <int>,
#       "table-inserts": <int>,
#       "errors": [
#         <error>,
#         ...
#       ],
#       "<reference-column-n>-db-names: list[str],
#       "<reference-column-n>-s3-names: list[str],
#       "<reference-column-n>-s3-full: dict[str, str],
#       ...
#     },
#   },
#   ...
# }


def synchronize_lobs(errors: list[str],
                     session_id: str,
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
                "table-bytes": 0,
                "errors": []
            }

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
        lob_columns: list[tuple[str, str]] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            if is_lob_column(col_type=column_type):
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
            started: datetime = datetime.now(tz=TZ_LOCAL)
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
                table_count: int = db_count(errors=errors,
                                            table=source_table,
                                            where_clause=where_clause,
                                            engine=source_db) or 0
                if table_count > 0:
                    warn_msg: str = ("Expecting an index to exist on column "
                                     f"{source_db}.{source_table}.{reference_column}")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)

                    # start synchronizing 'lob_column'
                    with lob_ctrl.lobdata_lock:
                        lob_ctrl.lobdata_register[mother_thread][source_table].update({
                            f"{reference_column}-db-names": [],
                            f"{reference_column}-s3-names": [],
                            f"{reference_column}-s3-full": {}
                        })
                    # obtain an S3 prefix for storing the lobdata
                    lob_prefix = build_lob_prefix(session_registry=session_registry,
                                                  target_db=target_db,
                                                  target_table=target_table,
                                                  column_name=reference_column)
                    # build migration channel data ([(offset, limit),...])
                    channel_data: list[tuple[int, int]] = \
                        build_channel_data(max_channels=session_metrics[MigMetric.LOBDATA_CHANNELS],
                                           channel_size=session_metrics[MigMetric.LOBDATA_CHANNEL_SIZE],
                                           table_count=table_count,
                                           offset_count=0,
                                           limit_count=0)
                    # remove the limit on the last channel
                    channel_data[-1] = (channel_data[-1][0], 0)
                    if len(channel_data) == 1:
                        # execute single task in current thread
                        _compute_lob_lists(mother_thread=mother_thread,
                                           source_db=source_db,
                                           source_table=source_table,
                                           reference_column=reference_column,
                                           where_clause=where_clause,
                                           s3_engine=target_s3,
                                           offset_count=channel_data[0][0],
                                           limit_count=channel_data[0][1],
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
                                                                 reference_column=reference_column,
                                                                 where_clause=where_clause,
                                                                 s3_engine=target_s3,
                                                                 lob_prefix=lob_prefix,
                                                                 offset_count=channel_datum[0],
                                                                 limit_count=channel_datum[1],
                                                                 logger=logger)
                                task_futures.append(future)

                            # wait for all task futures to complete, then shutdown down the executor
                            futures.wait(fs=task_futures)
                            executor.shutdown(wait=False)

                    # coaslesce 'lob_column' data
                    col_db_names: list[str] = []
                    col_s3_names: list[str] = []
                    col_s3_full: dict[str, str] = {}
                    with lob_ctrl.lobdata_lock:
                        table_data: dict[str, Any] = lob_ctrl.lobdata_register[mother_thread][source_table]
                        op_errors: list[str] = table_data.get("errors")
                        if op_errors:
                            status = "error"
                            errors.extend(op_errors)
                        else:
                            lob_count = table_data.get("table-count")
                            col_db_names = table_data.get(f"{reference_column}-db-names")
                            col_s3_names = table_data.get(f"{reference_column}-s3-names")
                            col_s3_full = table_data.get(f"{reference_column}-s3-full")

                    col_db_names.sort()
                    list_prune_duplicates(target=col_db_names,
                                          is_sorted=True)
                    col_s3_names.sort()
                    list_prune_duplicates(target=col_s3_names,
                                          is_sorted=True)
                    correlations: tuple = list_correlate(list_first=col_db_names,
                                                         list_second=col_s3_names,
                                                         only_in_first=True,
                                                         only_in_second=True,
                                                         is_sorted=True)
                    col_deletes: list[str] = correlations[1]
                    col_inserts: list[str] = correlations[0]
                    delete_count += len(col_deletes)
                    insert_count += len(col_inserts)
                    table_inserts[reference_column] = col_inserts
                    table_deletes[reference_column] = [col_s3_full.get(i) for i in col_deletes]

            finished: datetime = datetime.now(tz=TZ_LOCAL)
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

            # clean up 'lob_columns' and 'table_inserts'
            for lob_column, reference_column in lob_columns.copy():
                # 'reference_column' might contain a forced filetype specification
                pos: int = reference_column.rfind(".")
                if pos > 0:
                    reference_column = reference_column[:pos]
                if len(table_inserts.get(reference_column)) == 0:
                    lob_columns.remove((lob_column, reference_column))
                    table_inserts.pop(reference_column)

            # migrate the LOBs in 'table_inserts'
            if lob_columns:
                migrate_lob_columns(errors=errors,
                                    mother_thread=mother_thread,
                                    session_id=session_id,
                                    source_db=source_db,
                                    target_db=target_db,
                                    target_s3=target_s3,
                                    source_schema=source_schema,
                                    source_table=source_table,
                                    target_table=target_table,
                                    pk_columns=pk_columns,
                                    lob_columns=lob_columns,
                                    lob_tuples=table_inserts,
                                    offset_count=0,
                                    limit_count=0,
                                    migration_warnings=migration_warnings,
                                    logger=logger)

            # process LOBs in 'table_deletes'
            if not errors:
                # aggregate in 'lob_deletes' all LOBs slotted for removal from S3 storage
                lob_deletes: list[str] = []
                for deletes in list(table_deletes.values()):
                    lob_deletes.extend(deletes)
                # remove LOBs
                if lob_deletes:
                    lob_deletes.sort()
                    list_prune_duplicates(target=lob_deletes,
                                          is_sorted=True)
                    s3_items_remove(errors=errors,
                                    identifiers=lob_deletes,
                                    logger=logger)
    with lob_ctrl.lobdata_lock:
        migration_threads.extend(lob_ctrl.lobdata_register[mother_thread]["child-threads"])
        lob_ctrl.lobdata_register.pop(mother_thread)

    return result_count, result_deletes, result_inserts


def _compute_lob_lists(mother_thread: int,
                       source_db: DbEngine,
                       source_table: str,
                       reference_column: str,
                       where_clause: str,
                       s3_engine: S3Engine,
                       lob_prefix: Path,
                       offset_count: int,
                       limit_count: int,
                       logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    with lob_ctrl.lobdata_lock:
        lob_ctrl.lobdata_register[mother_thread]["child-threads"].append(threading.get_ident())

    # initialize the counter and the lists
    lob_count: int = 0
    lobs_db_names: list[str] = []
    lobs_s3_names: list[str] = []
    lobs_s3_full: dict[str, str] = {}

    # obtain a connection to the database
    errors: list[str] = []
    db_conn: Any = db_connect(errors=errors,
                              engine=source_db,
                              logger=logger)
    if not errors:
        # prepare database session
        session_setup(errors=errors,
                      rdbms=source_db,
                      mode="source",
                      conn=db_conn,
                      logger=logger)
        if not errors:
            # obtain an S3 client
            s3_client: Any = s3_get_client(errors=errors,
                                           engine=s3_engine,
                                           logger=logger)
            if not errors:
                db_items: list[tuple[str]] = db_select(errors=errors,
                                                       sel_stmt=f"SELECT {reference_column} FROM {source_table}",
                                                       where_clause=where_clause,
                                                       orderby_clause=reference_column,
                                                       offset_count=offset_count,
                                                       limit_count=limit_count,
                                                       connection=db_conn,
                                                       committable=True,
                                                       logger=logger)
                if not errors:
                    lobs_db_names = [db_item[0] for db_item in db_items]
                    lob_count = len(lobs_db_names)
                    # HAZARD: 'start_after' is actually 'start_at'
                    start_after: str = (Path(lob_prefix) / lobs_db_names[0]).as_posix() if offset_count > 0 else None
                    db_items.clear()

                    s3_items: list[dict[str, Any]] = s3_prefix_list(errors=errors,
                                                                    prefix=lob_prefix,
                                                                    max_count=limit_count,
                                                                    client=s3_client,
                                                                    start_after=start_after,
                                                                    logger=logger)
                    if not errors:
                        for s3_item in s3_items:
                            full_name = s3_item.get("Key")
                            name: str = full_name
                            # extract the prefix
                            pos: int = name.rfind("/")
                            if pos > 0:
                                name = name[pos+1:]
                            # extract the extension
                            pos = name.rfind(".")
                            if pos > 0:
                                name = name[:pos]
                            lobs_s3_names.append(name)
                            lobs_s3_full[name] = full_name
                        s3_items.clear()
                        # no need to keep items existing in both lists
                        correlations: tuple = list_correlate(list_first=lobs_db_names,
                                                             list_second=lobs_s3_names,
                                                             only_in_first=True,
                                                             only_in_second=True,
                                                             is_sorted=True)
                        lobs_db_names = correlations[0]
                        lobs_s3_names = correlations[1]
                        # keep only needed items in 'lobs_s3_full'
                        if len(lobs_s3_names) < len(lobs_s3_full):
                            lobs_s3_full = {k: v for k, v in lobs_s3_full.items() if k in lobs_s3_names}

    with lob_ctrl.lobdata_lock:
        table_data: dict[str, Any] = lob_ctrl.lobdata_register[mother_thread][source_table]
        if errors:
            table_data["errors"].extend(errors)
        else:
            table_data["table-count"] += lob_count
            table_data[f"{reference_column}-db-names"].extend(lobs_db_names)
            table_data[f"{reference_column}-s3-names"].extend(lobs_s3_names)
            table_data[f"{reference_column}-s3-full"].update(lobs_s3_full)
