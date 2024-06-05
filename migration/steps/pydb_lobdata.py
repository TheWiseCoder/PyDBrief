from logging import Logger
from pypomes_db import db_migrate_lobs
from typing import Any

from migration import pydb_types, pydb_common


def migrate_lobs(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 source_schema: str,
                 target_schema: str,
                 source_conn: Any,
                 target_conn: Any,
                 migrated_tables: dict,
                 logger: Logger | None) -> int:

    # initialize the return variavble
    result: int = 0

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"

        # organize the information, using LOB (large binary objects) types from the column names list
        table_pks: list[str] = []
        table_lobs: list[str] = []
        for column_name, column_data in table_data["columns"].items():
            column_type: str = column_data["source-type"]
            if pydb_types.is_lob(column_type):
                table_lobs.append(column_name)
            features: list[str] = column_data["features"]
            if "primary key" in features:
                table_pks.append(column_name)

        # can only migrate LOBs if table has primary keys
        op_errors: list[str] = []
        count: int = 0
        if table_pks:
            # process the existing LOB columns
            for table_lob in table_lobs:
                count += db_migrate_lobs(errors=op_errors,
                                         source_engine=source_rdbms,
                                         source_table=source_table,
                                         source_lob_column=table_lob,
                                         source_pk_columns=table_pks,
                                         target_engine=target_rdbms,
                                         target_table=target_table,
                                         source_conn=source_conn,
                                         target_conn=target_conn,
                                         chunk_size=pydb_common.MIGRATION_CHUNK_SIZE,
                                         logger=logger) or 0

        if op_errors:
            errors.extend(op_errors)
            status: str = "partial" if count else "none"
        else:
            status: str = "full"
        table_data["lob-status"] = status
        table_data["lob-count"] = count
        logger.debug(msg=f"Migrated LOBs from {source_rdbms}.{source_table} "
                         f"to {target_rdbms}.{target_table}, status {status}")
        result += count

    return result
