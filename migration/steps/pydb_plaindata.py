from logging import Logger
from typing import Any
from pypomes_core import validate_format_error
from pypomes_db import db_count, db_migrate_data

from migration import pydb_types, pydb_common
from migration.steps import pydb_database


def migrate_plain(errors: list[str],
                  source_rdbms: str,
                  target_rdbms: str,
                  source_schema: str,
                  target_schema: str,
                  skip_nonempty: bool,
                  remove_nulls: list[str],
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

        # is a nonempty target table an issue ?
        if skip_nonempty and (db_count(errors=errors,
                                       table=target_table,
                                       engine=target_rdbms,
                                       connection=target_conn) or 0) > 0:
            # yes, skip it
            logger.debug(msg=f"Skipped nonempty '{target_rdbms}.{target_table}'")
            table_data["plain-status"] = "skipped"
        else:
            # no, proceed
            no_pk: bool = True
            op_errors: list[str] = []
            identity_column: str | None = None
            column_names: list[str] = []
            # exclude LOB (large binary objects) types
            for column_name, column_data in table_data["columns"].items():
                column_type: str = column_data.get("source-type")
                if not pydb_types.is_lob(col_type=column_type):
                    features: list[str] = column_data.get("features", [])
                    column_names.append(column_name)
                    if "primary-key" in features:
                        no_pk = False
                    if "identity" in features:
                        if identity_column:
                            # 102: Unexpected error: {}
                            op_errors.append(validate_format_error(
                                102, f"Table {target_rdbms}.{target_table} has more than one identity column"))
                        else:
                            identity_column = column_name
            if no_pk:
                # 102: Unexpected error: {}
                op_errors.append(validate_format_error(
                    102, f"Table {target_rdbms}.{target_table} has no primary keys"))

            count: int = 0 if op_errors else \
                db_migrate_data(errors=op_errors,
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
                                has_nulls=table_name in remove_nulls,
                                logger=logger) or 0
            if op_errors:
                pydb_database.check_embedded_nulls(errors=op_errors,
                                                   rdbms=target_rdbms,
                                                   table=target_table)
                errors.extend(op_errors)
                status: str = "none"
            else:
                status: str = "full"

            table_data["plain-status"] = status
            table_data["plain-count"] = count
            logger.debug(msg=(f"Migrated tuples from {source_rdbms}.{source_table} "
                              f"to {target_rdbms}.{target_table}, status {status}"))
            result += count

    return result
