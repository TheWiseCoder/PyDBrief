import threading
from enum import StrEnum
from logging import Logger
from typing import Any
from pypomes_db import DbEngine, db_sync_data

from app_constants import (
    MigConfig, MigMetric, MigSpot, MigSpec
)
from migration import pydb_types
from migration.pydb_database import table_embedded_nulls
from migration.pydb_sessions import assert_session_abort, get_session_registry


def synchronize_plain(session_id: str,
                      correlate_only: bool,
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

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, Any] = session_registry[MigConfig.METRICS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]

    # retrieve the source and target RDBMS engines
    source_db: DbEngine = session_spots[MigSpot.FROM_RDBMS]
    target_db: DbEngine = session_spots[MigSpot.TO_RDBMS]

    # retrieve the input batch size
    batch_size_in: int = session_metrics[MigMetric.BATCH_SIZE_IN]

    # traverse list of migrated tables to synchronize their plain data
    for table_name, table_data in migrated_tables.items():

        # verify whether current migration is marked for abortion
        if assert_session_abort(session_id=session_id,
                                errors=errors,
                                logger=logger):
            break

        source_table: str = f"{session_specs[MigSpec.FROM_SCHEMA]}.{table_name}"
        target_table: str = f"{session_specs[MigSpec.TO_SCHEMA]}.{table_name}"
        has_nulls: bool = table_name in (session_specs[MigSpec.REMOVE_CTRLCHARS] or [])

        # identify identity column and build the lists of PK and sync columns
        op_errors: list[str] = []
        pk_columns: list[str] = []
        sync_columns: list[str] = []
        identity_column: str | None = None
        for column_name, column_data in table_data["columns"].items():
            # exclude LOB (large binary objects) types
            column_type: str = column_data.get("source-type")
            if not pydb_types.is_lob_column(col_type=column_type):
                features: list[str] = column_data.get("features", [])
                if "primary-key" in features:
                    pk_columns.append(column_name)
                else:
                    sync_columns.append(column_name)
                if "identity" in features:
                    identity_column = column_name

        counts: tuple[int, int, int] = db_sync_data(source_engine=source_db,
                                                    source_table=source_table,
                                                    target_engine=target_db,
                                                    target_table=target_table,
                                                    pk_columns=pk_columns,
                                                    sync_columns=sync_columns,
                                                    ignore_updates=correlate_only,
                                                    identity_column=identity_column,
                                                    batch_size=batch_size_in,
                                                    has_nulls=has_nulls,
                                                    errors=op_errors) or (0, 0, 0)
        deletes: int = counts[0]
        inserts: int = counts[1]
        updates: int = counts[2]
        if op_errors:
            table_embedded_nulls(rdbms=target_db,
                                 table=target_table,
                                 errors=op_errors,
                                 logger=logger)
            errors.extend(op_errors)
            status: str = "none"
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
