from logging import Logger
from pathlib import Path
from pypomes_core import validate_format_error
from pypomes_db import (
    DbEngine, DbParam,
    db_get_param, db_migrate_lobs, db_table_exists
)
from pypomes_s3 import S3Engine, s3_item_exists
from typing import Any
from urlobject import URLObject

from migration.pydb_types import is_lob
from migration.pydb_common import MIGRATION_METRICS, MetricsConfig
from migration.steps.pydb_s3 import s3_migrate_lobs


def migrate_lobs(errors: list[str],
                 source_rdbms: DbEngine,
                 target_rdbms: DbEngine,
                 source_schema: str,
                 target_schema: str,
                 target_s3: S3Engine,
                 accept_empty: bool,
                 skip_nonempty: bool,
                 incremental_migration: dict[str, int],
                 reflect_filetype: bool,
                 flatten_storage: bool,
                 named_lobdata: list[str],
                 source_conn: Any,
                 target_conn: Any,
                 # migration_warnings: list[str],
                 migrated_tables: dict[str, Any],
                 logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():
        target_table: str = f"{target_schema}.{table_name}"
        source_table: str = f"{source_schema}.{table_name}"

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
        if not pk_columns:
            err_msg: str = (f"Table {source_rdbms}.{source_table} "
                            f"is not eligible for LOB migration (no PKs)")
            logger.error(msg=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))

        count: int = 0
        if not errors and lob_columns and db_table_exists(errors=errors,
                                                          table_name=target_table,
                                                          engine=target_rdbms,
                                                          connection=target_conn,
                                                          logger=logger):
            status: str | None = None
            limit_count: int = incremental_migration.get(table_name)
            # process the existing LOB columns
            for lob_column in lob_columns:
                where_clause: str = f"{lob_column} IS NOT NULL" if accept_empty else None
                if target_s3:
                    forced_filetype: str | None = None
                    named_column: str | None = None
                    # determine if lobdata in 'lob_column' is named
                    for item in named_lobdata or []:
                        # format of item is '<table-name>.<column-name>=<named-column>[.<filetype>]'
                        if item.startswith(f"{table_name}.{lob_column}="):
                            named_column = item[item.index("=")+1:]
                            pos: int = named_column.find(".")
                            if pos > 0:
                                forced_filetype = named_column[pos:]
                                named_column = named_column[:pos]
                            break

                    # obtain a S3 prefix for storing the lobdata
                    lob_prefix: Path | None = None
                    if not flatten_storage:
                        url: URLObject = URLObject(db_get_param(key=DbParam.HOST,
                                                                engine=target_rdbms))
                        # 'url.hostname' returns 'None' for 'localhost'
                        lob_prefix = __build_prefix(rdbms=target_rdbms,
                                                    host=url.hostname or str(url),
                                                    schema=target_table[:target_table.index(".")],
                                                    table=target_table[target_table.index(".")+1:],
                                                    column=named_column or lob_column)
                        # is a nonempty S3 prefix an issue ?
                        if skip_nonempty and s3_item_exists(errors=errors,
                                                            prefix=lob_prefix):
                            # yes, skip it
                            logger.debug(msg=f"Skipped nonempty {target_s3}.{lob_prefix.as_posix()}")
                            status = "skipped"
                    # errors ?
                    if not errors:
                        # no, migrate the column's LOBs
                        count += s3_migrate_lobs(errors=errors,
                                                 target_s3=target_s3,
                                                 target_rdbms=target_rdbms,
                                                 target_table=target_table,
                                                 source_rdbms=source_rdbms,
                                                 source_table=source_table,
                                                 lob_prefix=lob_prefix,
                                                 lob_column=lob_column,
                                                 pk_columns=pk_columns,
                                                 where_clause=where_clause,
                                                 accept_empty=accept_empty,
                                                 limit_count=limit_count,
                                                 reflect_filetype=reflect_filetype,
                                                 forced_filetype=forced_filetype,
                                                 named_column=named_column,
                                                 source_conn=source_conn,
                                                 logger=logger) or 0
                else:
                    count += db_migrate_lobs(errors=errors,
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
                                             where_clause=where_clause,
                                             offset_count=-1 if limit_count else None,
                                             limit_count=limit_count,
                                             accept_empty=accept_empty,
                                             chunk_size=MIGRATION_METRICS.get(MetricsConfig.CHUNK_SIZE),
                                             logger=logger) or 0
            if errors:
                status = "none"
            else:
                # do not change 'status' if it has already been set
                status = status or "full"
                result += count
            table_data["lob-status"] = status
            table_data["lob-count"] = count
            logger.debug(msg=f"Migrated LOBs from {source_rdbms}.{source_table} "
                             f"to {target_rdbms}.{target_table}, status {status}")
        elif not errors and lob_columns:
            # target table does not exist
            err_msg: str = ("Unable to migrate LOBs. "
                            f"Table {target_rdbms}.{target_table} was not found")
            logger.error(msg=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))
        # errors ?
        if errors:
            # yes, abort the lobdata migration
            break

    return result


def __build_prefix(rdbms: str,
                   host: str,
                   schema: str,
                   table: str,
                   column: str) -> Path:

    return Path(f"{rdbms}@{host}",
                schema,
                table,
                column)
