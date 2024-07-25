from logging import Logger
from typing import Any
from pypomes_core import validate_format_error
from pypomes_db import db_migrate_data

from migration import pydb_types, pydb_common


def migrate_plain(errors: list[str],
                  source_rdbms: str,
                  target_rdbms: str,
                  source_schema: str,
                  target_schema: str,
                  source_conn: Any,
                  target_conn: Any,
                  migrated_tables: dict,
                  logger: Logger | None) -> int:

    # initialize the return variable
    result: int = 0

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"

        # exclude LOB (large binary objects) types from the column names list
        identity_column: str | None = None
        column_names: list[str] = []
        for column_name, column_data in table_data["columns"].items():
            column_type: str = column_data.get("source-type")
            if not pydb_types.is_lob(col_type=column_type):
                column_names.append(column_name)
                if "identity" in column_data.get("features", []):
                    if identity_column:
                        # 102: Unexpected error: {}
                        errors.append(validate_format_error(
                            102, f"Table {target_table} has more than one identity column"))
                    else:
                        identity_column = column_name

        op_errors: list[str] = []
        count: int = db_migrate_data(errors=op_errors,
                                     source_engine=source_rdbms,
                                     source_table=source_table,
                                     source_columns=column_names,
                                     target_engine=target_rdbms,
                                     target_table=target_table,
                                     source_conn=source_conn,
                                     target_conn=target_conn,
                                     source_committable=True,
                                     target_committable=True,
                                     identity_column=identity_column,
                                     batch_size=pydb_common.MIGRATION_BATCH_SIZE,
                                     logger=logger) or 0
        if op_errors:
            errors.extend(op_errors)
            status: str = "partial" if count else "none"
        else:
            status: str = "full"
        table_data["plain-status"] = status
        table_data["plain-count"] = count
        logger.debug(msg=(f"Migrated tuples from {source_rdbms}.{source_table} "
                          f"to {target_rdbms}.{target_table}, status {status}"))
        result += count

    return result
