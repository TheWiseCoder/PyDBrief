import threading
from enum import StrEnum
from logging import Logger
from typing import Any
from pypomes_db import (
    DbEngine, db_connect, db_sync_data
)

from app_constants import (
    MigConfig, MigMetric, MigSpot, MigSpec
)
from migration import pydb_types
from migration.pydb_database import table_embedded_nulls, session_setup
from migration.pydb_sessions import assert_session_abort, get_session_registry


def synchronize_plain(errors: list[str],
                      migration_threads: list[int],
                      migrated_tables: dict[str, Any],
                      session_id: str,
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
        if assert_session_abort(errors=errors,
                                session_id=session_id,
                                logger=logger):
            break

        source_table: str = f"{session_specs[MigSpec.FROM_SCHEMA]}.{table_name}"
        target_table: str = f"{session_specs[MigSpec.TO_SCHEMA]}.{table_name}"
        has_nulls: bool = table_name in (session_specs[MigSpec.REMOVE_NULLS] or [])

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

        # obtain a connection to the source DB
        source_conn: Any = db_connect(errors=op_errors,
                                      engine=source_db,
                                      logger=logger)
        if source_conn:
            # prepare database session
            session_setup(errors=op_errors,
                          rdbms=source_db,
                          mode="source",
                          conn=source_conn,
                          logger=logger)
            if not op_errors:
                # obtain a connection to the target DB
                target_conn: Any = db_connect(errors=op_errors,
                                              engine=target_db,
                                              logger=logger)
                if target_conn:
                    # prepare database session
                    session_setup(errors=op_errors,
                                  rdbms=target_db,
                                  mode="target",
                                  conn=target_conn,
                                  logger=logger)
                    if not op_errors:
                        counts: tuple[int, int, int] = db_sync_data(errors=op_errors,
                                                                    source_engine=source_db,
                                                                    source_table=source_table,
                                                                    target_engine=target_db,
                                                                    target_table=target_table,
                                                                    pk_columns=pk_columns,
                                                                    sync_columns=sync_columns,
                                                                    source_conn=source_conn,
                                                                    target_conn=target_conn,
                                                                    source_committable=True,
                                                                    target_committable=True,
                                                                    identity_column=identity_column,
                                                                    batch_size=batch_size_in,
                                                                    has_nulls=has_nulls,
                                                                    logger=logger) or (0, 0, 0)
                        deletes: int = counts[0]
                        inserts: int = counts[1]
                        updates: int = counts[2]
                        if op_errors:
                            table_embedded_nulls(errors=op_errors,
                                                 rdbms=target_db,
                                                 table=target_table,
                                                 logger=logger)
                            errors.extend(op_errors)
                            status: str = "none"
                        else:
                            status: str = "full"

                        table_data["sync-status"] = status
                        table_data["sync-deletes"] = deletes
                        table_data["sync-inserts"] = inserts
                        table_data["sync-updates"] = updates
                        logger.debug(msg=(f"Synchronized {source_db}.{target_table} "
                                          f"as per {source_db}.{target_table}, status {status}"))
                        result_deletes += deletes
                        result_inserts += inserts
                        result_updates += updates

    return result_deletes, result_inserts, result_updates
