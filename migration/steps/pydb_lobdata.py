from logging import Logger, ERROR
from pypomes_core import validate_format_error
from pypomes_db import db_migrate_lobs
from typing import Any

from migration.pydb_types import is_lob
from migration.pydb_common import MIGRATION_CHUNK_SIZE, log
from migration.steps.pydb_s3 import s3_migrate_lobs


def migrate_lobs(errors: list[str],
                 source_rdbms: str,
                 target_rdbms: str,
                 source_schema: str,
                 target_schema: str,
                 target_s3: str,
                 add_extensions: bool,
                 source_conn: Any,
                 target_conn: Any,
                 migrated_tables: dict[str, Any],
                 logger: Logger | None) -> int:

    # initialize the return variavble
    result: int = 0

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"

        # organize the information, using LOB types from the columns list
        pk_columns: list[str] = []
        lob_columns: list[str] = []
        table_columns = table_data.get("columns", {})
        for column_name, column_data in table_columns.items():
            column_type: str = column_data.get("source-type")
            if is_lob(column_type):
                lob_columns.append(column_name)
            features: list[str] = column_data.get("features", [])
            if "primary-key" in features:
                pk_columns.append(column_name)

        # can only migrate LOBs if table has primary key
        op_errors: list[str] = []
        count: int = 0
        if lob_columns and pk_columns:
            # process the existing LOB columns
            for lob_column in lob_columns:
                if target_s3:
                    count += s3_migrate_lobs(errors=op_errors,
                                             target_s3=target_s3,
                                             target_rdbms=target_rdbms,
                                             target_table=target_table,
                                             source_rdbms=source_rdbms,
                                             source_table=source_table,
                                             lob_column=lob_column,
                                             pk_columns=pk_columns,
                                             add_extensions=add_extensions,
                                             source_conn=source_conn,
                                             logger=logger) or 0
                else:
                    count += db_migrate_lobs(errors=op_errors,
                                             source_engine=source_rdbms,
                                             source_table=source_table,
                                             source_lob_column=lob_column,
                                             source_pk_columns=pk_columns,
                                             target_engine=target_rdbms,
                                             target_table=target_table,
                                             source_conn=source_conn,
                                             target_conn=target_conn,
                                             source_committable=True,
                                             target_committable=True,
                                             chunk_size=MIGRATION_CHUNK_SIZE,
                                             logger=logger) or 0
            if op_errors:
                errors.extend(op_errors)
                status: str = "none"
            else:
                status: str = "full"
                result += count
            table_data["lob-status"] = status
            table_data["lob-count"] = count
            logger.debug(msg=(f"Migrated LOBs from {source_rdbms}.{source_table} "
                              f"to {target_rdbms}.{target_table}, status {status}"))
        elif lob_columns:
            log(logger=logger,
                level=ERROR,
                msg=(f"Table {source_rdbms}.{target_table}, "
                     f"no primary key column found"))
            # 101: {}
            err_msg: str = ("Unable to migrate LOBs. "
                            f"Table {source_rdbms}.{source_table} has no primary keys")
            op_errors.append(validate_format_error(101,
                                                   err_msg))
    return result
