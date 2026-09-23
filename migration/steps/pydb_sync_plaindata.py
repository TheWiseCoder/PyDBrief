import threading
from logging import Logger
from typing import Any
from pypomes_db import db_connect, db_commit, db_sync_data

from entities.migration import Migration, MigStep
from entities.migration_table import MigrationTable
from entities.session import Session, sessions_aborting
from migration.pydb_database import table_embedded_nulls
from migration.pydb_types import is_lob_column


def synchronize_plaindata(migration: Migration,
                          session: Session,
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
    correlate_only: bool = migration.cd_step == MigStep.CORRELATE_PLAINDATA

    # traverse list of migrated tables to synchronize their plain data
    for table_name, table_data in migrated_tables.items():

        # verify whether current migration is marked for abortion
        if session.cd_session in sessions_aborting:
            sessions_aborting.remove(session.cd_session)
            break

        # obtain the corresponding MigrationTable instance
        migration_table: MigrationTable = \
            next((t for t in (migration.get_migration_tables() or []) if t.nm_table == table_name), None)

        # obtain input batch size, limit and offset
        batch_size_in: int = migration_table.nr_batch_size_in
        limit_count: int = (migration_table.nr_incremental_count if migration_table else 0) or 0
        offset_count: int = (migration_table.nr_incremental_offset if migration_table else 0) or 0

        source_table: str = f"{session.nm_source_schema}.{table_name}"
        target_table: str = f"{session.nm_target_schema}.{table_name}"
        has_ctrlchars: bool = migration_table.is_remove_ctrlchars

        # identify identity column and build the lists of PK and sync columns
        op_errors: list[str] = []
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

        # obtain target DB connection
        db_conn: Any = db_connect(engine=target_db,
                                  errors=op_errors)
        counts: tuple[int, int, int] = (0,  0, 0)
        if not op_errors:
            counts = db_sync_data(source_engine=source_db,
                                  source_table=source_table,
                                  target_engine=target_db,
                                  target_table=target_table,
                                  pk_columns=pk_columns,
                                  sync_columns=sync_columns,
                                  identity_column=identity_column,
                                  ignore_updates=correlate_only,
                                  offset_count=offset_count,
                                  limit_count=limit_count,
                                  batch_size=batch_size_in,
                                  has_nulls=has_ctrlchars,
                                  target_conn=db_conn,
                                  errors=op_errors) or (0, 0, 0)
            if op_errors:
                table_embedded_nulls(db_engine=target_db,
                                     table=target_table,
                                     errors=op_errors,
                                     logger=logger)
                errors.extend(op_errors)

            # unconditionally commit the transaction
            db_commit(connection=db_conn,
                      engine=target_db,
                      errors=op_errors)

        deletes: int = counts[0]
        inserts: int = counts[1]
        updates: int = counts[2]
        if op_errors:
            status: str = "partial"
        else:
            status: str = "full"

        op: str = "correlate" if correlate_only else "sync"
        table_data[f"{op}-status"] = status
        table_data[f"{op}-deletes"] = deletes
        table_data[f"{op}-inserts"] = inserts
        if not correlate_only:
            table_data["sync-updates"] = updates
        op = "Correlated" if correlate_only else "Synchronized"
        logger.debug(msg=(f"{op} {source_db}.{target_table} "
                          f"as per {source_db}.{target_table}, status {status}"))
        result_deletes += deletes
        result_inserts += inserts
        result_updates += updates

    return result_deletes, result_inserts, result_updates
