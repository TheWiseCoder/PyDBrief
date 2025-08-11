import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from enum import StrEnum
from logging import Logger
from pypomes_core import (
    TZ_LOCAL, timestamp_duration, validate_format_error
)
from pypomes_db import (
    DbEngine, db_is_reserved_word,
    db_connect, db_count, db_table_exists, db_migrate_data
)
from typing import Any

from app_constants import (
    MigConfig, MigSpot, MigSpec, MigMetric
)
from migration.pydb_common import build_channel_data
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob
from migration.steps.pydb_database import (
    session_setup, table_embedded_nulls
)

# _plaindata_threads: dict[int, dict[str, Any]] = {
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
_plaindata_threads: dict[int, dict[str, Any]] = {}
_plaindata_lock: threading.Lock = threading.Lock()


def migrate_plain(errors: list[str],
                  session_id: str,
                  incremental_migrations: dict[str, tuple[int, int]],
                  migration_warnings: list[str],
                  migration_threads: list[int],
                  migrated_tables: dict[str, Any],
                  logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # add to the thread registration
    mother_thread: int = threading.get_ident()
    migration_threads.append(mother_thread)

    global _plaindata_threads
    with _plaindata_lock:
        _plaindata_threads[mother_thread] = {
            "child-threads": []
        }

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, int] = session_registry[MigConfig.METRICS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]

    # retrieve the source and target RDBMS engines
    source_engine: DbEngine = session_spots[MigSpot.FROM_RDBMS]
    target_engine: DbEngine = session_spots[MigSpot.TO_RDBMS]

    # retrieve the input and output batch sizes
    batch_size_in: int = session_metrics[MigMetric.BATCH_SIZE_IN]
    batch_size_out: int = session_metrics[MigMetric.BATCH_SIZE_OUT]

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():

        # verify whether current migration is marked for abortion
        if assert_session_abort(errors=errors,
                                session_id=session_id,
                                logger=logger):
            break

        source_table: str = f"{session_specs[MigSpec.FROM_SCHEMA]}.{table_name}"
        target_table: str = f"{session_specs[MigSpec.TO_SCHEMA]}.{table_name}"
        with _plaindata_lock:
            _plaindata_threads[mother_thread][source_table] = {
                "table-count": 0,
                "errors": []
            }

        # verify whether the target table exists
        if db_table_exists(errors=errors,
                           table_name=target_table,
                           engine=target_engine,
                           logger=logger):
            # obtain limit and offset
            limit_count: int = 0
            offset_count: int = 0
            if table_name in incremental_migrations:
                limit_count, offset_count = incremental_migrations.get(table_name)

            # is a nonempty target table an issue ?
            if (session_specs[MigSpec.SKIP_NONEMPTY] and
                    not limit_count and (db_count(errors=errors,
                                                  table=target_table,
                                                  engine=target_engine) or 0) > 0):
                # yes, skip it
                logger.debug(msg=f"Skipped nonempty {target_engine}.{target_table}")
                table_data["plain-status"] = "skipped"

            elif not errors:
                # no, proceed
                count: int = 0
                status: str = "ok"
                started: datetime = datetime.now(tz=TZ_LOCAL)

                # count migrateable tuples on source table
                table_count: int = (db_count(errors=errors,
                                             table=source_table,
                                             engine=source_engine) or 0) - offset_count
                if table_count > 0:

                    identity_column: str | None = None
                    orderby_columns: list[str] = []
                    source_columns: list[str] = []
                    target_columns: list[str] = []

                    # setup source and target columns
                    for column_name, column_data in table_data["columns"].items():
                        column_type: str = column_data.get("source-type")
                        if not is_lob(col_type=column_type):
                            features: list[str] = column_data.get("features", [])
                            source_columns.append(column_name)
                            if db_is_reserved_word(word=column_name,
                                                   engine=target_engine):
                                target_columns.append(f'"{column_name}"')
                            else:
                                target_columns.append(column_name)
                            if "identity" in features:
                                identity_column = column_name
                            elif "primary-key" in features and (limit_count or batch_size_in):
                                orderby_columns.append(column_name)

                    if not orderby_columns:
                        warn_msg: str = ""
                        if session_metrics[MigMetric.PLAINDATA_CHANNELS] > 1:
                            warn_msg = "Multi-channel migration"
                        elif limit_count:
                            warn_msg = "Incremental migration"
                        elif offset_count:
                            warn_msg = "Reading offset"
                        elif batch_size_in:
                            warn_msg = "Batch reading"
                        if warn_msg:
                            warn_msg += f" specified for table having no PKs: {source_engine}.{source_table}"
                            migration_warnings.append(warn_msg)
                            logger.warning(msg=warn_msg)

                    # build migration channel data ([(offset, limit),...])
                    channel_data: list[tuple[int, int]] = \
                        build_channel_data(  # max_channels=session_metrics[MigMetric.PLAINDATA_CHANNELS],
                                           channel_size=session_metrics[MigMetric.PLAINDATA_CHANNEL_SIZE],
                                           table_count=table_count,
                                           offset_count=offset_count,
                                           limit_count=limit_count)
                    if len(channel_data) == 1:
                        # execute single task in current thread
                        _migrate_plain(mother_thread=mother_thread,
                                       source_engine=source_engine,
                                       source_table=source_table,
                                       source_columns=source_columns,
                                       target_engine=target_engine,
                                       target_table=target_table,
                                       target_columns=target_columns,
                                       orderby_clause=", ".join(orderby_columns),
                                       offset_count=channel_data[0][0],
                                       limit_count=channel_data[0][1],
                                       identity_column=identity_column,
                                       batch_size_in=batch_size_in,
                                       batch_size_out=batch_size_out,
                                       has_nulls=False,
                                       logger=logger)
                    else:
                        logger.debug(msg=f"Started migrating {sum(c[0] for c in channel_data)} tuples from "
                                         f"{source_engine}.{source_table} to {target_engine}.{target_table}, "
                                         f"using {len(channel_data)} channels")

                        # execute tasks concurrently
                        with ThreadPoolExecutor(max_workers=len(channel_data)) as executor:
                            task_futures: list[Future] = []
                            for channel_datum in channel_data:
                                future: Future = executor.submit(_migrate_plain,
                                                                 mother_thread=mother_thread,
                                                                 source_engine=source_engine,
                                                                 source_table=source_table,
                                                                 source_columns=source_columns,
                                                                 target_engine=target_engine,
                                                                 target_table=target_table,
                                                                 target_columns=target_columns,
                                                                 orderby_clause=", ".join(orderby_columns),
                                                                 offset_count=channel_datum[0],
                                                                 limit_count=channel_datum[1],
                                                                 identity_column=identity_column,
                                                                 batch_size_in=batch_size_in,
                                                                 batch_size_out=batch_size_out,
                                                                 has_nulls=False,
                                                                 logger=logger)
                                task_futures.append(future)

                            # wait for all task futures to complete, then shutdown down the executor
                            futures.wait(fs=task_futures)
                            executor.shutdown(wait=False)

                    with _plaindata_lock:
                        count = _plaindata_threads[mother_thread][source_table]["table-count"]
                        if _plaindata_threads[mother_thread][source_table]["errors"]:
                            status = "error"
                            errors.extend(_plaindata_threads[mother_thread][source_table]["errors"])
                    if status == "error":
                        table_embedded_nulls(errors=errors,
                                             rdbms=source_engine,
                                             table=source_table,
                                             logger=logger)

                finished: datetime = datetime.now(tz=TZ_LOCAL)
                duration: str = timestamp_duration(start=started,
                                                   finish=finished)
                secs: float = (finished - started).total_seconds()
                table_data.update({
                    "plain-duration": duration,
                    "plain-status": status,
                    "plain-count": count,
                    "plain-performance": f"{count/secs:.2f} tuples/s"
                })
                logger.debug(msg=f"Migrated {count} plaindata in table {table_name}, "
                                 f"from {source_engine} to {target_engine}, "
                                 f"status {status}, duration {duration}")
                result += count

        elif not errors:
            # target table does not exist
            err_msg: str = ("Unable to migrate plaindata, "
                            f"table {target_engine}.{target_table} was not found")
            logger.error(msg=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))
        if errors:
            # yes, abort the plaindata migration
            break

    with _plaindata_lock:
        migration_threads.extend(_plaindata_threads[mother_thread]["child-threads"])
        _plaindata_threads.pop(mother_thread)

    return result


def _migrate_plain(mother_thread: int,
                   source_engine: DbEngine,
                   source_table: str,
                   source_columns: list[str],
                   target_engine: DbEngine,
                   target_table: str,
                   target_columns: list[str],
                   orderby_clause: str,
                   offset_count: int,
                   limit_count: int,
                   identity_column: str,
                   batch_size_in: int,
                   batch_size_out: int,
                   has_nulls: bool,
                   logger: Logger) -> None:

    # register the operation thread (might be same as mother thread)
    global _plaindata_threads
    with _plaindata_lock:
        _plaindata_threads[mother_thread]["child-threads"].append(threading.get_ident())

    errors: list[str] = []
    count: int = 0

    # obtain a connection to the source database
    source_conn: Any = db_connect(errors=errors,
                                  engine=source_engine,
                                  logger=logger)
    if source_conn:
        # prepare database session
        session_setup(errors=errors,
                      rdbms=source_engine,
                      mode="source",
                      conn=source_conn,
                      logger=logger)
        if not errors:
            # obtain a connection to the target database
            target_conn: Any = db_connect(errors=errors,
                                          engine=target_engine,
                                          logger=logger)
            if target_conn:
                # prepare database session
                session_setup(errors=errors,
                              rdbms=target_engine,
                              mode="target",
                              conn=target_conn,
                              logger=logger)
                if not errors:
                    count = db_migrate_data(errors=errors,
                                            source_engine=source_engine,
                                            source_table=source_table,
                                            source_columns=source_columns,
                                            target_engine=target_engine,
                                            target_table=target_table,
                                            target_columns=target_columns,
                                            source_conn=source_conn,
                                            target_conn=target_conn,
                                            target_committable=True,
                                            orderby_clause=orderby_clause,
                                            offset_count=offset_count,
                                            limit_count=limit_count,
                                            identity_column=identity_column,
                                            batch_size_in=batch_size_in,
                                            batch_size_out=batch_size_out,
                                            has_nulls=has_nulls,
                                            logger=logger)
    with _plaindata_lock:
        if errors:
            _plaindata_threads[mother_thread][source_table]["errors"].extend(errors)
        else:
            _plaindata_threads[mother_thread][source_table]["table-count"] += count
