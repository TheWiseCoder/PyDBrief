from logging import Logger
from typing import Any
from pypomes_core import validate_format_error
from pypomes_db import db_sync_data

from migration import pydb_types, pydb_common


def synchronize_plain(errors: list[str],
                      source_rdbms: str,
                      target_rdbms: str,
                      source_schema: str,
                      target_schema: str,
                      remove_nulls: list[str],
                      source_conn: Any,
                      target_conn: Any,
                      migrated_tables: dict,
                      logger: Logger | None) -> tuple[int, int, int]:

    # initialize the return variables
    result_deletes: int = 0
    result_inserts: int = 0
    result_updates: int = 0

    # traverse list of migrated tables to synchronize the plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"

        # identify identity column and build the lists of PKs and sync columns
        op_errors: list[str] = []
        identity_column: str | None = None
        pk_columns: list[str] = []
        sync_columns: list[str] = []
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
                    if identity_column:
                        # 102: Unexpected error: {}
                        op_errors.append(validate_format_error(
                            102, f"Table '{target_table}' has more than one identity column"))
                    else:
                        identity_column = column_name
        if not pk_columns:
            # 102: Unexpected error: {}
            op_errors.append(validate_format_error(
                102, f"Table '{target_table}' has no primary keys"))

        deletes, inserts, updates = (0, 0, 0) if op_errors else \
            db_sync_data(errors=op_errors,
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
                         batch_size=pydb_common.MIGRATION_BATCH_SIZE,
                         has_nulls=table_name in remove_nulls,
                         logger=logger) or (0, 0, 0)
        if op_errors:
            # did a 'ValueError' exception on NULLs in strings occur ?
            # ("A string literal cannot contain NUL (0x00) characters.")
            if target_rdbms == "postgres" and \
               "contain NUL" in " ".join(op_errors):
                # yes, provide instructions on how to handle the problem
                msg: str = (f"Table '{source_table}' has NULLs embedded in string data, "
                            f"which is not accepted by '{target_rdbms}'. Please add this "
                            f"table to the 'remove-nulls' migration parameter, and try again.")
                # 101: {}
                errors.append(validate_format_error(101, msg))
            else:
                # no, report the errors
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
