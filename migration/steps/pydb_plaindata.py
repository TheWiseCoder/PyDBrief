from logging import Logger
from pypomes_core import validate_format_error
from pypomes_db import db_count, db_table_exists, db_migrate_data, DbEngine
from typing import Any

from migration import pydb_types, pydb_common
from migration.steps import pydb_database


def migrate_plain(errors: list[str],
                  source_rdbms: DbEngine,
                  target_rdbms: DbEngine,
                  source_schema: str,
                  target_schema: str,
                  skip_nonempty: bool,
                  incremental_migration: dict[str, int],
                  remove_nulls: list[str],
                  source_conn: Any,
                  target_conn: Any,
                  migrated_tables: dict,
                  logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"
        op_errors: list[str] = []

        # does the target table exist ?
        if db_table_exists(errors=op_errors,
                           table_name=target_table,
                           engine=target_rdbms,
                           connection=target_conn,
                           logger=logger):
            # yes, is a nonempty target table an issue ?
            if skip_nonempty and (db_count(errors=op_errors,
                                           table=target_table,
                                           engine=target_rdbms,
                                           connection=target_conn) or 0) > 0:
                # yes, skip it
                logger.debug(msg=f"Skipped nonempty {target_rdbms}.{target_table}")
                table_data["plain-status"] = "skipped"
            elif not op_errors:
                # no, proceed
                limit_count: int = incremental_migration.get(table_name)
                offset_count: int = -1 if limit_count else None
                identity_column: str | None = None
                orderby_columns: list[str] = []
                column_names: list[str] = []
                # exclude LOB types
                for column_name, column_data in table_data["columns"].items():
                    column_type: str = column_data.get("source-type")
                    if not pydb_types.is_lob(col_type=column_type):
                        features: list[str] = column_data.get("features", [])
                        column_names.append(column_name)
                        if "identity" in features:
                            identity_column = column_name
                        elif "primary-key" in features and \
                                (limit_count > 0 or pydb_common.MIGRATION_BATCH_SIZE_IN > 0):
                            orderby_columns.append(column_name)
                if limit_count > 0 and not orderby_columns:
                    err_msg: str = (f"Table {source_rdbms}.{source_table} "
                                    f"is not eligible for incremental migration (no PKs)")
                    logger.error(msg=err_msg)
                    # 101: {}
                    op_errors.append(validate_format_error(101,
                                                           err_msg))

                count: int = (db_migrate_data(errors=op_errors,
                                              source_engine=source_rdbms,
                                              source_table=source_table,
                                              source_columns=column_names,
                                              target_engine=target_rdbms,
                                              target_table=target_table,
                                              source_conn=source_conn,
                                              target_conn=target_conn,
                                              source_committable=True,
                                              target_committable=True,
                                              orderby_clause=", ".join(orderby_columns),
                                              offset_count=offset_count,
                                              limit_count=limit_count,
                                              identity_column=identity_column,
                                              batch_size_in=pydb_common.MIGRATION_BATCH_SIZE_IN,
                                              batch_size_out=pydb_common.MIGRATION_BATCH_SIZE_OUT,
                                              has_nulls=table_name in remove_nulls,
                                              logger=logger) or 0) if not op_errors else 0
                if op_errors:
                    pydb_database.check_embedded_nulls(errors=op_errors,
                                                       rdbms=target_rdbms,
                                                       table=target_table,
                                                       logger=logger)
                    status: str = "error"
                else:
                    status: str = "ok"

                table_data["plain-status"] = status
                table_data["plain-count"] = count
                logger.debug(msg=(f"Attempted plaindata migration from {source_rdbms}.{source_table} "
                                  f"to {target_rdbms}.{target_table}, status {status}"))
                result += count

        elif not op_errors:
            # target table does not exist
            err_msg: str = ("Unable to migrate plaindata, "
                            f"Table {target_rdbms}.{target_table} was not found")
            logger.error(msg=err_msg)
            # 101: {}
            op_errors.append(validate_format_error(101,
                                                   err_msg))
        errors.extend(op_errors)

    return result
