import threading
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from logging import Logger
from pypomes_core import (
    TZ_LOCAL, timestamp_duration, validate_format_error
)
from pypomes_db import (
    db_is_reserved_word, db_count, db_table_exists, db_migrate_data
)
from typing import Any

from app_constants import PYDB_DB_ENGINE, MigStep
from entities.migration import Migration, SPAN_CHANNEL_COUNT, SPAN_CHANNEL_SIZE
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_span import MigrationSpan
from entities.migration_table import MigrationTable, SPAN_BATCH_SIZE_IN, SPAN_BATCH_SIZE_OUT
from entities.migration_work import MigrationWork
from entities.session import Session, sessions_aborting
from migration.pydb_common import (
    build_channel_data, execute_sql, get_migration_span, get_migration_work
)
from migration.pydb_database import table_embedded_nulls
from migration.pydb_types import is_lob_column

# structure of the thread registry:
# plaindata_registry: dict[int, dict[str, Any]] = {
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
plaindata_registry: dict[int, dict[str, Any]] = {}
plaindata_lock: threading.Lock = threading.Lock()


def migrate_plaindata(session: Session,
                      mig_step: MigStep,
                      migration: Migration,
                      migration_threads: list[int],
                      migrated_tables: dict[str, Any],
                      migration_warnings: list[str],
                      errors: list[str],
                      logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # add to the thread registration
    mother_thread: int = threading.get_ident()
    migration_threads.append(mother_thread)

    with plaindata_lock:
        plaindata_registry[mother_thread] = {
            "child-threads": []
        }

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():

        # abort the plaindata migration on error from previous cycle
        if errors:
            break

        # verify whether current migration is marked for abortion
        if session.cd_session in sessions_aborting:
            sessions_aborting.remove(session.cd_session)
            break

        target_db: str = session.get_target_db().cd_engine
        target_table: str = f"{session.nm_target_schema}.{table_name}"
        with plaindata_lock:
            plaindata_registry[mother_thread][table_name] = {
                "table-count": 0,
                "errors": []
            }

        # obtain the MigrationWork instance
        migration_work: MigrationWork = get_migration_work(migration=migration,
                                                           step=mig_step,
                                                           table=table_name,
                                                           errors=errors)
        if not errors and db_table_exists(table_name=target_table,
                                          engine=target_db,
                                          errors=errors):
            # obtain migration table data
            migration_table: MigrationTable = MigrationTable.for_table(
                table=table_name,
                migration_tables=migration.get_migration_tables() or []
            ) or MigrationTable()
            if migration_table.ds_pre_sql:
                execute_sql(migration=migration,
                            mig_step=mig_step,
                            db_engine=session.get_source_db().cd_engine,
                            sql_text=migration_table.ds_pre_sql)

            batch_size_in: int = migration_table.nr_batch_size_in or SPAN_BATCH_SIZE_IN[2]
            batch_size_out: int = migration_table.nr_batch_size_out or SPAN_BATCH_SIZE_OUT[2]
            limit_count: int = migration_table.nr_incremental_count or 0
            offset_count: int = migration_table.nr_incremental_offset or 0

            if (migration.is_skip_nonempty and
                    not limit_count and (db_count(table=target_table,
                                                  engine=target_db,
                                                  errors=errors) or 0) > 0):
                # yes, skip it
                logger.debug(msg=f"Skipped nonempty {target_db}.{target_table}")
                table_data["plain-status"] = "skipped"

            elif not errors:
                result += __migrate_plaindata(session=session,
                                              mig_step=mig_step,
                                              migration=migration,
                                              migration_work=migration_work,
                                              mother_thread=mother_thread,
                                              table_data=table_data,
                                              offset_count=offset_count,
                                              limit_count=limit_count,
                                              batch_size_in=batch_size_in,
                                              batch_size_out=batch_size_out,
                                              is_remove_ctrlchars=migration_table.is_remove_ctrlchars or False,
                                              migration_warnings=migration_warnings,
                                              logger=logger,
                                              errors=errors)
        elif not errors:
            # target table does not exist
            err_msg: str = ("Unable to migrate plaindata, "
                            f"table {target_db}.{target_table} was not found")
            logger.error(msg=err_msg)
            MigrationIssue.new_issue(id_migration=migration.id,
                                     cd_step=mig_step,
                                     cd_type=IssueType.ERROR,
                                     ds_issue=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))

    with plaindata_lock:
        migration_threads.extend(plaindata_registry[mother_thread]["child-threads"])
        plaindata_registry.pop(mother_thread)

    return result


def __migrate_plaindata(session: Session,
                        mig_step: MigStep,
                        migration: Migration,
                        migration_work: MigrationWork,
                        mother_thread: int,
                        table_data: dict[str, Any],
                        offset_count: int,
                        limit_count: int,
                        batch_size_in: int,
                        batch_size_out: int,
                        is_remove_ctrlchars: bool,
                        migration_warnings: list[str],
                        logger: Logger,
                        errors: list[str]) -> int:
    result: int = 0
    status: str = "ok"
    started: datetime = datetime.now(tz=TZ_LOCAL)
    source_db: str = session.get_source_db().cd_engine
    target_db: str = session.get_target_db().cd_engine
    source_table: str = f"{session.nm_source_schema}.{migration_work.nm_table}"
    target_table: str = f"{session.nm_target_schema}.{migration_work.nm_table}"

    # count migrateable tuples on source table
    table_count: int = (db_count(table=source_table,
                                 engine=source_db,
                                 errors=errors) or 0) - offset_count
    if table_count > 0:
        channel_count: int = migration.nr_channel_count or SPAN_CHANNEL_COUNT[2]
        channel_size: int = migration.nr_channel_size or SPAN_CHANNEL_SIZE[2]
        identity_column: str | None = None
        orderby_columns: list[str] = []
        source_columns: list[str] = []
        target_columns: list[str] = []

        # setup source and target columns
        for column_name, column_data in table_data["columns"].items():
            column_type: str = column_data.get("source-type")
            if not is_lob_column(col_type=column_type):
                features: list[str] = column_data.get("features", [])
                source_columns.append(column_name)
                if db_is_reserved_word(word=column_name,
                                       engine=target_db):
                    target_columns.append(f'"{column_name}"')
                else:
                    target_columns.append(column_name)
                if "identity" in features:
                    identity_column = column_name
                elif "primary-key" in features and (limit_count or batch_size_in):
                    orderby_columns.append(column_name)

        if not orderby_columns:
            warn_msg: str = ""
            if channel_count > 1:
                warn_msg = "Multi-channel migration"
            elif limit_count:
                warn_msg = "Incremental migration"
            elif offset_count:
                warn_msg = "Reading offset"
            elif batch_size_in:
                warn_msg = "Batch reading"
            if warn_msg:
                warn_msg += f" specified for table having no PKs: {source_db}.{source_table}"
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_step=mig_step,
                                         cd_type=IssueType.WARNING,
                                         ds_issue=warn_msg)

        # build migration channel data ([(offset, limit),...])
        channel_data: list[tuple[int, int]] = \
            build_channel_data(channel_size=channel_size,
                               table_count=table_count,
                               offset_count=offset_count,
                               limit_count=limit_count)
        max_workers: int = min(channel_count, len(channel_data))
        tot_count: int = sum(i[1] for i in channel_data)
        logger.debug(msg=f"Started migrating {tot_count} tuples from "
                         f"{source_db}.{source_table} to {target_db}.{target_table}, "
                         f"in {len(channel_data)} steps, using {max_workers} channels")
        if max_workers == 1:
            for channel_datum in channel_data:
                # execute single task in current thread
                _migrate_plain(session=session,
                               mig_step=mig_step,
                               migration_work=migration_work,
                               mother_thread=mother_thread,
                               source_columns=source_columns,
                               target_columns=target_columns,
                               orderby_clause=", ".join(orderby_columns),
                               offset_count=channel_datum[0],
                               limit_count=channel_datum[1],
                               identity_column=identity_column,
                               batch_size_in=batch_size_in,
                               batch_size_out=batch_size_out,
                               has_ctrlchars=is_remove_ctrlchars)
        else:
            # execute tasks concurrently
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                task_futures: list[Future] = []
                for channel_datum in channel_data:
                    future: Future = executor.submit(_migrate_plain,
                                                     session=session,
                                                     mig_step=mig_step,
                                                     migration_work=migration_work,
                                                     mother_thread=mother_thread,
                                                     source_columns=source_columns,
                                                     target_columns=target_columns,
                                                     orderby_clause=", ".join(orderby_columns),
                                                     offset_count=channel_datum[0],
                                                     limit_count=channel_datum[1],
                                                     identity_column=identity_column,
                                                     batch_size_in=batch_size_in,
                                                     batch_size_out=batch_size_out,
                                                     has_ctrlchars=is_remove_ctrlchars)
                    task_futures.append(future)

                # wait for all task futures to complete, then shutdown down the executor
                futures.wait(fs=task_futures)
                executor.shutdown(wait=False)

        with plaindata_lock:
            result = plaindata_registry[mother_thread][migration_work.nm_table]["table-count"]
            curr_errors: list[str] = plaindata_registry[mother_thread][migration_work.nm_table]["errors"]
            if curr_errors:
                status = "error"
                errors.extend(curr_errors)
        if status == "error":
            table_embedded_nulls(db_engine=source_db,
                                 table=source_table,
                                 errors=errors,
                                 logger=logger)

    finished: datetime = datetime.now(tz=TZ_LOCAL)
    duration: str = timestamp_duration(start=started,
                                       finish=finished)
    secs: float = (finished - started).total_seconds()
    table_data.update({
        "plain-duration": duration,
        "plain-status": status,
        "plain-count": result,
        "plain-performance": f"{result/secs:.2f} tuples/s"
    })
    logger.debug(msg=f"Migrated {result} plaindata from table {source_db}.{source_table} to "
                     f"f{target_db}.{target_table}, status {status}, duration {duration}")
    return result


def _migrate_plain(session: Session,
                   mig_step: MigStep,
                   migration_work: MigrationWork,
                   mother_thread: int,
                   source_columns: list[str],
                   target_columns: list[str],
                   orderby_clause: str,
                   offset_count: int,
                   limit_count: int,
                   identity_column: str,
                   batch_size_in: int,
                   batch_size_out: int,
                   has_ctrlchars: bool) -> None:

    errors: list[str] = []

    # register the operation thread (might be same as mother thread)
    with plaindata_lock:
        plaindata_registry[mother_thread]["child-threads"].append(threading.get_ident())

    # determine whether the migration needs to be carried out
    count: int = 0
    migration_span: MigrationSpan = get_migration_span(migration_work=migration_work,
                                                       first_row=offset_count,
                                                       errors=errors)
    if not errors and not migration_span.is_done:

        source_db: str = session.get_source_db().cd_engine
        target_db: str = session.get_target_db().cd_engine
        source_table: str = f"{session.nm_source_schema}.{migration_work.nm_table}"
        target_table: str = f"{session.nm_target_schema}.{migration_work.nm_table}"

        count = db_migrate_data(source_engine=source_db,
                                source_table=source_table,
                                source_columns=source_columns,
                                target_engine=target_db,
                                target_table=target_table,
                                target_columns=target_columns,
                                orderby_clause=orderby_clause,
                                offset_count=offset_count,
                                limit_count=limit_count,
                                identity_column=identity_column,
                                batch_size_in=batch_size_in,
                                batch_size_out=batch_size_out,
                                has_ctrlchars=has_ctrlchars,
                                errors=errors)
        # acknowledge the migration
        migration_span.nr_row_count = count
        migration_span.is_done = True
        migration_span.update(db_engine=PYDB_DB_ENGINE,
                              errors=errors)
        migration_work.nr_row_count += count
        migration_work.update(db_engine=PYDB_DB_ENGINE,
                              errors=errors)
    with plaindata_lock:
        if errors:
            MigrationIssue.new_issues(id_migration=migration_work.id_migration,
                                      cd_step=mig_step,
                                      cd_type=IssueType.ERROR,
                                      ds_issues=errors)
            plaindata_registry[mother_thread][migration_work.nm_table]["errors"].extend(errors)
        else:
            plaindata_registry[mother_thread][migration_work.nm_table]["table-count"] += count
