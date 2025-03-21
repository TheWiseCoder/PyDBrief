from logging import Logger
from typing import Any
from pypomes_db import db_sync_data, DbEngine

from migration import pydb_types
from migration.pydb_common import MIGRATION_METRICS, Metrics
from migration.steps import pydb_database


def synchronize_plain(errors: list[str],
                      source_rdbms: DbEngine,
                      target_rdbms: DbEngine,
                      source_schema: str,
                      target_schema: str,
                      remove_nulls: list[str],
                      source_conn: Any,
                      target_conn: Any,
                      migrated_tables: dict,
                      logger: Logger) -> tuple[int, int, int]:

    # initialize the return variables
    result_deletes: int = 0
    result_inserts: int = 0
    result_updates: int = 0

    # traverse list of migrated tables to synchronize their plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"

        # identify identity column and build the lists of PK and sync columns
        op_errors: list[str] = []
        pk_columns: list[str] = []
        sync_columns: list[str] = []
        identity_column: str | None = None
        for column_name, column_data in table_data["columns"].items():
            # exclude LOB (large binary objects) types
            column_type: str = column_data.get("source-type")
            if not pydb_types.is_lob(col_type=column_type):
                features: list[str] = column_data.get("features", [])
                if "primary-key" in features:
                    pk_columns.append(column_name)
                else:
                    sync_columns.append(column_name)
                if "identity" in features:
                    identity_column = column_name

        deletes, inserts, updates = db_sync_data(errors=op_errors,
                                                 source_engine=source_rdbms,
                                                 source_table=source_table,
                                                 target_engine=target_rdbms,
                                                 target_table=target_table,
                                                 pk_columns=pk_columns,
                                                 sync_columns=sync_columns,
                                                 source_conn=source_conn,
                                                 target_conn=target_conn,
                                                 source_committable=True,
                                                 target_committable=True,
                                                 identity_column=identity_column,
                                                 batch_size=MIGRATION_METRICS.get(Metrics.BATCH_SIZE_IN),
                                                 has_nulls=table_name in remove_nulls,
                                                 logger=logger) or (0, 0, 0)
        if op_errors:
            pydb_database.check_embedded_nulls(errors=op_errors,
                                               rdbms=target_rdbms,
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
        logger.debug(msg=(f"Synchronized {source_rdbms}.{source_table} "
                          f"as per {target_rdbms}.{target_table}, status {status}"))
        result_deletes += deletes
        result_inserts += inserts
        result_updates += updates

    return result_deletes, result_inserts, result_updates
