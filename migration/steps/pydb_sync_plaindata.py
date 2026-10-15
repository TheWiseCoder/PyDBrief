import threading
from logging import Logger
from typing import Any
from pypomes_db import db_connect, db_commit, db_rollback, db_close, db_sync_data

from entities.migration import Migration, minded_migrations
from app_constants import PYDB_DB_ENGINE, MigState, MigStep
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_span import MigrationSpan
from entities.migration_table import SPAN_BATCH_SIZE_IN, MigrationTable
from entities.migration_work import MigrationWork
from entities.session import Session
from migration.pydb_common import get_migration_span, get_migration_work
from migration.pydb_database import table_embedded_nulls
from migration.pydb_types import is_lob_column


def synchronize_plaindata(session: Session,
                          mig_step: MigStep,
                          migration: Migration,
                          migration_threads: list[int],
                          migrated_tables: dict[str, Any],
                          # migration_warnings: list[str],
                          errors: list[str],
                          logger: Logger) -> tuple[int, int, int]:

    # initialize the return variables
    result_deletes: int = 0
    result_inserts: int = 0
    result_updates: int = 0

    # add to the thread registration
    migration_threads.append(threading.get_ident())

    # retrieve the source and target RDBMS engines
    source_db: str = session.get_source_db().cd_engine
    target_db: str = session.get_target_db().cd_engine

    # traverse list of migrated tables to synchronize their plain data
    for table_name, table_data in migrated_tables.items():

        # verify whether current migration is marked for abortion
        mig_key: str = f"{mig_step}-{migration.id}"
        if minded_migrations.get(mig_key) == MigState.ABORTING:
            break

        # obtain the MigrationWork instance
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
            # obtain input batch size, limit and offset
            batch_size_in: int = migration_table.nr_batch_size_in or SPAN_BATCH_SIZE_IN[2]
            limit_count: int = migration_table.nr_incremental_count or 0
            offset_count: int = migration_table.nr_incremental_offset or 0

            # determine whether the synchronization needs to be carried out
            migration_span: MigrationSpan = get_migration_span(migration_work=migration_work,
                                                               first_row=offset_count,
                                                               errors=errors)
            if not errors and not migration_span.is_done:
                source_table: str = f"{session.nm_source_schema}.{table_name}"
                target_table: str = f"{session.nm_target_schema}.{table_name}"
                has_ctrlchars: bool = migration_table.is_remove_ctrlchars

                # identify identity column and build the lists of PK and sync columns
                pk_columns: list[str] = []
                sync_columns: list[str] = []
                identity_column: str | None = None
                for column_name, column_data in table_data["columns"].items():
                    # exclude LOB (large binary objects) types
                    column_type: str = column_data.get("source-type")
                    if not is_lob_column(col_type=column_type):
                        features: list[str] = column_data.get("features", [])
                        if "primary-key" in features:
                            pk_columns.append(column_name)
                        else:
                            sync_columns.append(column_name)
                        if "identity" in features:
                            identity_column = column_name

                counts: tuple[int, int, int] = (0,  0, 0)
                # obtain target DB connection
                target_conn: Any = db_connect(engine=target_db,
                                              errors=errors)
                if target_conn:
                    counts = db_sync_data(source_engine=source_db,
                                          source_table=source_table,
                                          target_engine=target_db,
                                          target_table=target_table,
                                          pk_columns=pk_columns,
                                          sync_columns=sync_columns,
                                          identity_column=identity_column,
                                          ignore_updates=mig_step == MigStep.CORRELATE_PLAINDATA,
                                          offset_count=offset_count,
                                          limit_count=limit_count,
                                          batch_size=batch_size_in,
                                          has_nulls=has_ctrlchars,
                                          target_conn=target_conn,
                                          errors=errors) or (0, 0, 0)
                    # acknowledge the migration
                    if errors:
                        table_embedded_nulls(db_engine=target_db,
                                             table=target_table,
                                             errors=errors,
                                             logger=logger)
                        MigrationIssue.new_issues(id_migration=migration.id,
                                                  cd_step=mig_step,
                                                  cd_type=IssueType.ERROR,
                                                  ds_issues=errors)
                    else:
                        # obtain state DB connection
                        pydb_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                                    errors=errors)
                        if pydb_conn:
                            migration_span.nr_row_count = counts[0] + counts[1] + counts[2]
                            migration_span.is_done = True
                            migration_span.update(db_engine=PYDB_DB_ENGINE,
                                                  db_conn=pydb_conn,
                                                  errors=errors)
                            if not errors:
                                migration_work.nr_row_count = counts[0] + counts[1] + counts[2]
                                migration_work.update(db_engine=PYDB_DB_ENGINE,
                                                      db_conn=pydb_conn,
                                                      errors=errors)
                            if errors:
                                db_rollback(connection=pydb_conn,
                                            engine=PYDB_DB_ENGINE)
                            else:
                                db_commit(connection=pydb_conn,
                                          engine=PYDB_DB_ENGINE,
                                          errors=errors)
                            db_close(connection=pydb_conn,
                                     engine=PYDB_DB_ENGINE)
                    if errors:
                        db_rollback(connection=target_conn,
                                    engine=target_db)
                    else:
                        db_commit(connection=target_conn,
                                  engine=target_db,
                                  errors=errors)
                    db_close(connection=target_conn,
                             engine=target_db)

                deletes: int = counts[0]
                inserts: int = counts[1]
                updates: int = counts[2]
                if errors:
                    status: str = "partial"
                else:
                    status: str = "full"

                op: str = "correlate" if mig_step == MigStep.CORRELATE_PLAINDATA else "sync"
                table_data[f"{op}-status"] = status
                table_data[f"{op}-deletes"] = deletes
                table_data[f"{op}-inserts"] = inserts
                if mig_step == MigStep.SYNCHRONIZE_PLAINDATA:
                    table_data["sync-updates"] = updates
                op = "Correlated" if mig_step == MigStep.CORRELATE_PLAINDATA else "Synchronized"
                logger.debug(msg=(f"{op} {source_db}.{target_table} "
                                  f"as per {source_db}.{target_table}, status {status}"))
                result_deletes += deletes
                result_inserts += inserts
                result_updates += updates

    return result_deletes, result_inserts, result_updates
