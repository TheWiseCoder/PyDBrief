from datetime import datetime, UTC
from logging import Logger
from pypomes_core import timestamp_duration, validate_format_error
from pypomes_db import (
    DbEngine, db_is_reserved_word,
    db_count, db_table_exists, db_migrate_data
)
from typing import Any

from migration import pydb_types
from app_constants import MetricsConfig
from migration.pydb_common import assert_abort_state, get_metrics_params

from migration.steps.pydb_database import check_embedded_nulls


def migrate_plain(errors: list[str],
                  source_rdbms: DbEngine,
                  target_rdbms: DbEngine,
                  source_schema: str,
                  target_schema: str,
                  skip_nonempty: bool,
                  incremental_migrations: dict[str, tuple[int, int]],
                  remove_nulls: list[str],
                  source_conn: Any,
                  target_conn: Any,
                  migration_warnings: list[str],
                  migrated_tables: dict[str, Any],
                  session_id: str,
                  logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # retrieve the input and output batch sizes
    batch_size_in: int = get_metrics_params(session_id=session_id).get(MetricsConfig.BATCH_SIZE_IN)
    batch_size_out: int = get_metrics_params(session_id=session_id).get(MetricsConfig.BATCH_SIZE_OUT)

    # traverse list of migrated tables to copy the plain data
    for table_name, table_data in migrated_tables.items():
        source_table: str = f"{source_schema}.{table_name}"
        target_table: str = f"{target_schema}.{table_name}"

        # verify whether current migration is marked for abortion
        if assert_abort_state(errors=errors,
                              session_id=session_id,
                              logger=logger):
            break

        # verify whether the target table exists
        if db_table_exists(errors=errors,
                           table_name=target_table,
                           engine=target_rdbms,
                           connection=target_conn,
                           logger=logger):
            limit_count: int | None = None
            offset_count: int | None = None
            if table_name in incremental_migrations:
                limit_count, offset_count = incremental_migrations.get(table_name)

            # is a nonempty target table an issue ?
            if skip_nonempty and not limit_count and \
                    (db_count(errors=errors,
                              table=target_table,
                              engine=target_rdbms,
                              connection=target_conn) or 0) > 0 and not errors:
                # yes, skip it
                logger.debug(msg=f"Skipped nonempty {target_rdbms}.{target_table}")
                table_data["plain-status"] = "skipped"

            elif not errors:
                # no, proceed
                identity_column: str | None = None
                orderby_columns: list[str] = []
                source_columns: list[str] = []
                target_columns: list[str] = []

                # setup source and target columns
                for column_name, column_data in table_data["columns"].items():
                    column_type: str = column_data.get("source-type")
                    if not pydb_types.is_lob(col_type=column_type):
                        features: list[str] = column_data.get("features", [])
                        source_columns.append(column_name)
                        if db_is_reserved_word(word=column_name,
                                               engine=target_rdbms):
                            target_columns.append(f'"{column_name}"')
                        else:
                            target_columns.append(column_name)
                        if "identity" in features:
                            identity_column = column_name
                        elif "primary-key" in features and (limit_count or batch_size_in):
                            orderby_columns.append(column_name)

                if not orderby_columns:
                    suffix: str = f"for table {source_rdbms}.{source_table} having no PKs"
                    if limit_count:
                        warn: str = f"Incremental migration specified {suffix}"
                        migration_warnings.append(warn)
                        logger.warning(msg=warn)
                    if offset_count:
                        warn: str = f"Reading offset specified {suffix}"
                        migration_warnings.append(warn)
                        logger.warning(msg=warn)
                    if batch_size_in:
                        warn: str = f"Batch reading specified {suffix}"
                        migration_warnings.append(warn)
                        logger.warning(msg=warn)

                started: datetime = datetime.now(tz=UTC)
                count: int = db_migrate_data(errors=errors,
                                             source_engine=source_rdbms,
                                             source_table=source_table,
                                             source_columns=source_columns,
                                             target_engine=target_rdbms,
                                             target_table=target_table,
                                             target_columns=target_columns,
                                             source_conn=source_conn,
                                             target_conn=target_conn,
                                             source_committable=True,
                                             target_committable=True,
                                             orderby_clause=", ".join(orderby_columns),
                                             offset_count=offset_count,
                                             limit_count=limit_count,
                                             identity_column=identity_column,
                                             batch_size_in=batch_size_in,
                                             batch_size_out=batch_size_out,
                                             has_nulls=table_name in remove_nulls,
                                             logger=logger) or 0
                if errors:
                    check_embedded_nulls(errors=errors,
                                         rdbms=target_rdbms,
                                         table=target_table,
                                         logger=logger)
                    status: str = "error"
                else:
                    status: str = "ok"

                finished: datetime = datetime.now(tz=UTC)
                duration: str = timestamp_duration(start=started,
                                                   finish=finished)
                table_data["plain-status"] = status
                table_data["plain-count"] = count
                table_data["plain-duration"] = duration
                logger.debug(msg=f"Migrated {count} plaindata from {source_rdbms}.{source_table} "
                                 f"to {target_rdbms}.{target_table}, status {status}, duration {duration}")
                result += count

        elif not errors:
            # target table does not exist
            err_msg: str = ("Unable to migrate plaindata, "
                            f"Table {target_rdbms}.{target_table} was not found")
            logger.error(msg=err_msg)
            # 101: {}
            errors.append(validate_format_error(101,
                                                err_msg))
        if errors:
            # yes, abort the plaindata migration
            break

    return result
