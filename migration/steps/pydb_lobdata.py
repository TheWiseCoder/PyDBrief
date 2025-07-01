import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from enum import StrEnum
from logging import Logger
from pathlib import Path
from pypomes_core import (
    TIMEZONE_LOCAL,
    timestamp_duration, validate_format_error, list_elem_starting_with
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
                 # migration_warnings: list[str],
                 migration_threads: list[int],
                 migrated_tables: dict[str, Any],
                 logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # zadd to the thread registration
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

    # retrieve the database and chunk size
    db_name: str = session_registry[target_db][DbConfig.NAME]
    chunk_size: int = session_metrics[MigMetric.CHUNK_SIZE]

    # traverse list of migrated tables to copy the LOB data
    for table_name, table_data in migrated_tables.items():

        # verify whether current migration is marked for abortion
        if assert_session_abort(errors=errors,
                                session_id=session_id,
                                logger=logger):
            break

        source_table: str = f"{session_specs[MigSpec.FROM_SCHEMA]}.{table_name}"
        target_table: str = f"{session_specs[MigSpec.TO_SCHEMA]}.{table_name}"
        with _lobdata_lock:
            _lobdata_threads[mother_thread][source_table] = {
                "table-count": 0,
                "errors": []
            }

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
        lob_columns: list[str] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            # migrating to S3 requires the lob column be mapped in 'named_lobdata'
            if is_lob(column_type) and \
                    (not target_s3 or
                     list_elem_starting_with(source=session_specs[MigSpec.NAMED_LOBDATA] or [],
                                             prefix=f"{table_name}.{column_name}=")):
                lob_columns.append(column_name)
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        if not pk_columns:
            err_msg: str = (f"Table {source_db}.{source_table} "
                            f"is not eligible for LOB migration (no PKs)")
            logger.error(msg=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))

        if not errors and lob_columns and db_table_exists(errors=errors,
                                                          table_name=target_table,
                                                          engine=target_db,
                                                          logger=logger):
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            status: str = "ok"
            count: int = 0
            limit_count: int = 0
            offset_count: int = 0
            if table_name in incremental_migrations:
                limit_count, offset_count = incremental_migrations.get(table_name)

            # count migrateable tuples on source table
            table_count: int = (db_count(errors=errors,
                                         table=source_table,
                                         engine=source_db) or 0) - offset_count
            if table_count > 0:

                # process the existing LOB columns
                for lob_column in lob_columns:
                    where_clause: str = f"{lob_column} IS NOT NULL"

                    # migrate the column's LOBs
                    lob_prefix: Path | None = None
                    forced_filetype: str | None = None
                    ret_column: str | None = None
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
                            lob_prefix = __build_prefix(rdbms=target_db,
                                                        host=url.hostname or str(url),
                                                        database=db_name,
                                                        schema=target_table[:target_table.index(".")],
                                                        table=target_table[target_table.index(".")+1:],
                                                        column=ret_column or lob_column)
                            # is a nonempty S3 prefix an issue ?
                            if session_specs[MigSpec.SKIP_NONEMPTY] and \
                                    s3_item_exists(errors=errors,
                                                   prefix=lob_prefix):
                                # yes, skip it
                                logger.debug(msg=f"Skipped nonempty "
                                                 f"{target_s3}.{lob_prefix.as_posix()}")
                                status = "skipped"

                    if not errors:

                        # build migration channel data
                        channel_data: list[tuple[int, int]] = \
                            build_channel_data(max_channels=session_metrics[MigMetric.PLAINDATA_CHANNELS],
                                               channel_size=session_metrics[MigMetric.PLAINDATA_CHANNEL_SIZE],
                                               table_count=table_count,
                                               offset_count=offset_count,
                                               limit_count=limit_count)
                        if len(channel_data) > 1:
                            target: str = f"S3 storage '{target_s3}'" \
                                if target_s3 else f"{target_db}.{target_table}"
                            logger.debug(msg=f"Started migrating {sum(c[0] for c in channel_data)} LOBs "
                                             f"from {source_db}.{source_table} to {target}, "
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

                            # wait for all task futures to complete
                            futures.wait(fs=task_futures)

                            with _lobdata_lock:
                                if _lobdata_threads[mother_thread][source_table]["errors"]:
                                    status = "error"
                                    errors.extend(_lobdata_threads[mother_thread][source_table]["errors"])
                                else:
                                    count = _lobdata_threads[mother_thread][source_table]["table-count"]

            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            table_data["lob-status"] = status
            table_data["lob-count"] = count
            table_data["lob-duration"] = duration
            logger.debug(msg=f"Migrated {count} lobdata in table {table_name}, "
                             f"from {source_db} to {target_db}, "
                             f"status {status}, duration {duration}")
            result += count

        elif not errors and lob_columns:
            # target table does not exist
            err_msg: str = ("Unable to migrate LOBs, "
                            f"table {target_db}.{target_table} was not found")
            logger.error(msg=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))
        # errors ?
        if errors:
            # yes, abort the lobdata migration
            break

    with _lobdata_lock:
        migration_threads.extend(_lobdata_threads[mother_thread]["child-threads"])
        _lobdata_threads.pop(mother_thread)

    return result


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

    global _lobdata_threads
    with _lobdata_lock:
        _lobdata_threads[mother_thread]["child-threads"].append(threading.get_ident())

    # obtain a connection to the target database
    errors: list[str] = []
    count: int = 0
    target_conn: Any = db_connect(errors=errors,
                                  engine=target_engine)
    if target_conn:
        # disable triggers and rules to speed-up migration
        session_disable_restrictions(errors=errors,
                                     rdbms=target_engine,
                                     conn=target_conn,
                                     logger=logger)
        if not errors:
            count = db_migrate_lobs(errors=errors,
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
    with _lobdata_lock:
        if errors:
            _lobdata_threads[mother_thread][source_table]["errors"].extend(errors)
        else:
            _lobdata_threads[mother_thread][source_table]["table-count"] += count


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
                     logger: Logger) -> None:

    global _lobdata_threads
    with _lobdata_lock:
        _lobdata_threads[mother_thread]["child-threads"].append(threading.get_ident())

    errors: list[str] = []
    s3_client = s3_get_client(errors=errors,
                              engine=target_s3,
                              logger=logger)
    if s3_client:
        count: int = s3_migrate_lobs(errors=errors,
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
                                     logger=logger)
        with _lobdata_lock:
            if errors:
                _lobdata_threads[mother_thread][source_table]["errors"].extend(errors)
            else:
                _lobdata_threads[mother_thread][source_table]["table-count"] += count


def __build_prefix(rdbms: DbEngine,
                   host: str,
                   schema: str,
                   database: str,
                   table: str,
                   column: str) -> Path:

    return Path(f"{rdbms}@{host}",
                schema,
                database,
                table,
                column)
