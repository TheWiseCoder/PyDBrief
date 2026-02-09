import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from enum import StrEnum
from logging import Logger
from pathlib import Path
from pypomes_core import TZ_LOCAL, timestamp_duration
from pypomes_db import (
    DbEngine, db_connect, db_count, db_close,
    db_migrate_lobs, db_table_exists, db_drop_table,
    db_bulk_insert, db_create_session_table, db_get_session_table_prefix
)
from pypomes_s3 import S3Engine, s3_get_client, s3_item_exists
from typing import Any

from app_constants import (
    MigConfig, MigMetric, MigSpec, MigSpot, MigStep, MigIncremental
)
from migration.pydb_common import build_channel_data, build_lob_prefix
from migration.pydb_sessions import assert_session_abort, get_session_registry
from migration.pydb_types import is_lob_column
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


def migrate_lob_tables(session_id: str,
                       incr_migrations: dict[str, dict[MigIncremental, int]],
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
        if errors or assert_session_abort(session_id=session_id,
                                          errors=errors,
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

        # obtain offset and limit
        offset_count: int = 0
        limit_count: int = 0
        if table_name in incr_migrations:
            limit_count = incr_migrations[table_name].get(MigIncremental.COUNT)
            offset_count = incr_migrations[table_name].get(MigIncremental.OFFSET)

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
        lob_columns: list[tuple[str, str]] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            # migrating to S3 requires the lob column be mapped in 'named-lobdata'
            if is_lob_column(col_type=column_type):
                reference_column: str | None = None
                # determine if lobdata in 'lob_column' has its filename defined in 'named-lobdata'
                for item in (session_specs[MigSpec.NAMED_LOBDATA] or []):
                    # format of item is '<table-name>.<column-name>=<named-column>[.<filetype>]'
                    if item.startswith(f"{table_name}.{column_name}="):
                        reference_column = item[item.index("=")+1:]
                        break
                if target_s3:
                    warn_msg: str | None = None
                    if reference_column == column_name:
                        warn_msg = "mapped to itself"
                        reference_column = None
                    elif not reference_column:
                        warn_msg = "not mapped"
                    if warn_msg:
                        warn_msg = (f"Column {source_db}.{source_table}.{column_name} "
                                    f"{warn_msg} in '{MigSpec.NAMED_LOBDATA}'")
                        migration_warnings.append(warn_msg)
                        logger.warning(msg=warn_msg)
                lob_columns.append((column_name, reference_column))
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        if lob_columns:
            # specific condition for migrating table LOBs to database
            if not target_s3 and not db_table_exists(table_name=target_table,
                                                     engine=target_db,
                                                     errors=errors):
                # target table could not be found (might be due to error)
                warn_msg: str = ("Unable to migrate LOBs, "
                                 f"table {target_db}.{target_table} was not found")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                # skip table migration
                continue

            # start migrating the source table LOBs
            started: datetime = datetime.now(tz=TZ_LOCAL)
            status: str = "ok"
            migrate_lob_columns(mother_thread=mother_thread,
                                session_id=session_id,
                                source_db=source_db,
                                target_db=target_db,
                                target_s3=target_s3,
                                source_schema=source_schema,
                                source_table=source_table,
                                target_table=target_table,
                                lob_columns=lob_columns,
                                pk_columns=pk_columns,
                                lob_tuples=None,
                                offset_count=offset_count,
                                limit_count=limit_count,
                                migration_warnings=migration_warnings,
                                errors=errors,
                                logger=logger)
            with lobdata_lock:
                lob_count: int = lobdata_register[mother_thread][source_table]["table-count"]
                lob_bytes: int = lobdata_register[mother_thread][source_table]["table-bytes"]
                op_errors: list[str] = lobdata_register[mother_thread][source_table]["errors"]
                if op_errors:
                    status = "error"
                    errors.extend(op_errors)

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
        migration_threads.extend(lobdata_register[mother_thread]["child-threads"])
        lobdata_register.pop(mother_thread)

    return result_count, result_bytes


def migrate_lob_columns(mother_thread: int,
                        session_id: str,
                        source_db: DbEngine,
                        target_db: DbEngine,
                        target_s3: S3Engine,
                        source_schema: str,
                        source_table: str,
                        target_table: str,
                        pk_columns: list[str],
                        lob_columns: list[tuple[str, str]],
                        lob_tuples: dict[str, list[str]] | None,
                        offset_count: int,
                        limit_count: int,
                        migration_warnings: list[str],
                        errors: list[str],
                        logger: Logger) -> None:

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_metrics: dict[MigMetric, Any] = session_registry[MigConfig.METRICS]

    # retrieve the channel and chunk specs
    channel_count: int = session_metrics[MigMetric.LOBDATA_CHANNELS]
    channel_size: int = session_metrics[MigMetric.LOBDATA_CHANNEL_SIZE]
    chunk_size: int = session_metrics[MigMetric.CHUNK_SIZE]

    # process the existing LOB columns
    for lob_column, reference_column in lob_columns:

        # verify whether current migration is marked for abortion
        if errors or assert_session_abort(session_id=session_id,
                                          errors=errors,
                                          logger=logger):
            # abort the lobdata migration
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
                                 f"(not mapped in '{MigSpec.NAMED_LOBDATA}', and no PKs in table)")
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                # skip table migration
                continue

            # define a forced file type
            if reference_column:
                pos: int = reference_column.rfind(".")
                if pos > 0:
                    # 'forced_filetype' includes the leading dot ('.')
                    forced_filetype = reference_column[pos:]
                    reference_column = reference_column[:pos]

            # obtain an S3 prefix for storing the lobdata
            if session_registry[MigConfig.STEPS][MigStep.SYNCHRONIZE_LOBDATA] or \
                    not session_specs[MigSpec.FLATTEN_STORAGE]:
                lob_prefix = build_lob_prefix(session_registry=session_registry,
                                              target_db=target_db,
                                              target_table=target_table,
                                              column_name=reference_column or lob_column)

                # is a nonempty S3 prefix an issue ?
                if (not session_registry[MigConfig.STEPS][MigStep.SYNCHRONIZE_LOBDATA] and
                    session_specs[MigSpec.SKIP_NONEMPTY] and
                    s3_item_exists(identifier=lob_prefix.as_posix(),
                                   errors=errors)):
                    # yes, skip it
                    warn_msg: str = ("Skipped migrating LOBs in column "
                                     f"{source_db}.{source_table}.{lob_column}: "
                                     f"folder {target_s3}.{lob_prefix.as_posix()} is not empty")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)
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
            if max_workers == 1:
                # execute single task in current thread
                if target_s3:
                    # migration target is S3
                    _s3_migrate_lobs(mother_thread=mother_thread,
                                     session_id=session_id,
                                     source_db=source_db,
                                     source_schema=source_schema,
                                     source_table=source_table,
                                     target_s3=target_s3,
                                     target_table=target_table,
                                     lob_prefix=lob_prefix,
                                     lob_column=lob_column,
                                     pk_columns=pk_columns or [reference_column],
                                     where_clause=where_clause,
                                     offset_count=channel_data[0][0],
                                     limit_count=tot_count,
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
                                     pk_columns=pk_columns or [reference_column],
                                     target_engine=target_db,
                                     target_table=target_table,
                                     where_clause=where_clause,
                                     offset_count=channel_data[0][0],
                                     limit_count=tot_count,
                                     chunk_size=chunk_size,
                                     logger=logger)
            else:
                target: str = f"S3 storage '{target_s3}'" \
                    if target_s3 else f"{target_db}.{target_table}.{lob_column}"
                logger.debug(msg=f"Started migrating {tot_count} LOBs from "
                                 f"{source_db}.{source_table}.{lob_column} to {target}, "
                                 f"using {max_workers} channels")

                # execute tasks concurrently
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    task_futures: list[Future] = []
                    for channel_datum in channel_data:
                        if target_s3:
                            # migration target is S3
                            future: Future = executor.submit(_s3_migrate_lobs,
                                                             mother_thread=mother_thread,
                                                             session_id=session_id,
                                                             source_db=source_db,
                                                             source_schema=source_schema,
                                                             source_table=source_table,
                                                             target_s3=target_s3,
                                                             target_table=target_table,
                                                             lob_prefix=lob_prefix,
                                                             lob_column=lob_column,
                                                             pk_columns=pk_columns or [reference_column],
                                                             where_clause=where_clause,
                                                             offset_count=channel_datum[0],
                                                             limit_count=channel_datum[1],
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
                                                             pk_columns=pk_columns or [reference_column],
                                                             target_engine=target_db,
                                                             target_table=target_table,
                                                             where_clause=where_clause,
                                                             offset_count=channel_datum[0],
                                                             limit_count=channel_datum[1],
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
                     offset_count: int,
                     limit_count: int,
                     chunk_size: int,
                     logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    with lobdata_lock:
        lobdata_register[mother_thread]["child-threads"].append(threading.get_ident())

    lob_count: int = 0
    lob_bytes: int = 0
    errors: list[str] = []

    totals: tuple[int, int] = db_migrate_lobs(source_engine=source_engine,
                                              source_table=source_table,
                                              source_lob_column=lob_column,
                                              source_pk_columns=pk_columns,
                                              target_engine=target_engine,
                                              target_table=target_table,
                                              where_clause=where_clause,
                                              offset_count=offset_count,
                                              limit_count=limit_count,
                                              chunk_size=chunk_size,
                                              errors=errors,
                                              logger=logger)
    if not errors:
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
                     source_schema: str,
                     source_table: str,
                     target_s3: S3Engine,
                     target_table: str,
                     lob_prefix: Path,
                     lob_column: str,
                     pk_columns: list[str],
                     where_clause: str | list[str],
                     offset_count: int,
                     limit_count: int,
                     forced_filetype: str,
                     reference_column: str,
                     migration_warnings: list[str],
                     logger: Logger) -> None:

    # register the operation thread (might be same as the mother thread)
    with lobdata_lock:
        lobdata_register[mother_thread]["child-threads"].append(threading.get_ident())

    # obtain an S3 client
    errors: list[str] = []
    s3_client = s3_get_client(engine=target_s3,
                              errors=errors)
    if s3_client:
        db_conn: Any = None
        temp_table: str | None = None
        if isinstance(where_clause, list):
            # obtain a database connection
            db_conn = db_connect(engine=source_db,
                                 errors=errors)
            if db_conn:
                # 'where_clause' is a list of 'reference_column' values indicating the LOBs to migrate
                temp_table = f"{source_schema}." + \
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
            totals: tuple[int, int] = s3_migrate_lobs(session_id=session_id,
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
                                                      errors=errors,
                                                      logger=logger)
            with lobdata_lock:
                if errors:
                    lobdata_register[mother_thread][source_table]["errors"].extend(errors)
                else:
                    lobdata_register[mother_thread][source_table]["table-count"] += totals[0]
                    lobdata_register[mother_thread][source_table]["table-bytes"] += totals[1]
        if db_conn:
            db_drop_table(table_name=temp_table,
                          connection=db_conn,
                          committable=True)
            db_close(connection=db_conn)
