import warnings
from datetime import datetime
from logging import INFO, Logger
from pypomes_core import DATETIME_FORMAT_INV
from pypomes_db import db_connect
from sqlalchemy.sql.elements import Type
from typing import Any

from migration import pydb_common
from migration.steps.pydb_database import (
    disable_session_restrictions, restore_session_restrictions
)
from migration.steps.pydb_lobdata import migrate_lobs
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain

# treat warnings as errors
warnings.filterwarnings("error")


# this is the entry point for the migration process
def migrate(errors: list[str],
            source_rdbms: str,
            target_rdbms: str,
            source_schema: str,
            target_schema: str,
            step_metadata: bool,
            step_plaindata: bool,
            step_lobdata: bool,
            include_tables: list[str],
            exclude_tables: list[str],
            external_columns: dict[str, Type],
            logger: Logger | None) -> dict:

    # log the start of the migration
    steps: str = ""
    if step_metadata:
        steps += ", metadata"
    if step_plaindata:
        steps += ", plaindata"
    if step_lobdata:
        steps += ", lobdata"
    msg: str = (f"Migration started, "
                f"from {source_rdbms}.{source_schema} "
                f"to {target_rdbms}.{target_schema}, "
                f"steps {steps[2:]}")
    if include_tables:
        msg += f", include tables {','.join(include_tables)}"
    if exclude_tables:
        msg += f", exclude tables {','.join(exclude_tables)}"
    pydb_common.log(logger=logger,
                    level=INFO,
                    msg=msg)

    started: datetime = datetime.now()
    pydb_common.log(logger=logger,
                    level=INFO,
                    msg="Started discovering the metadata")
    migrated_tables: dict = migrate_metadata(errors=errors,
                                             source_rdbms=source_rdbms,
                                             target_rdbms=target_rdbms,
                                             source_schema=source_schema,
                                             target_schema=target_schema,
                                             step_metadata=step_metadata,
                                             include_tables=include_tables,
                                             exclude_tables=exclude_tables,
                                             external_columns=external_columns,
                                             logger=logger)
    pydb_common.log(logger=logger,
                    level=INFO,
                    msg="Finished discovering the metadata")

    # initialize the counters
    plain_count: int = 0
    lob_count: int = 0

    # proceed, if migration of plain data and/or LOB data has been indicated
    if migrated_tables and \
       (step_plaindata or step_lobdata):

        # obtain source and target connections
        op_errors: list[str] = []
        source_conn: Any = db_connect(errors=op_errors,
                                      engine=source_rdbms,
                                      logger=logger)
        target_conn: Any = db_connect(errors=op_errors,
                                      engine=target_rdbms,
                                      logger=logger)

        if source_conn and target_conn:
            # disable target RDBMS restrictions to speed-up bulk copying
            disable_session_restrictions(errors=op_errors,
                                         rdbms=target_rdbms,
                                         conn=target_conn,
                                         logger=logger)
            # proceed, if restrictions were disabled
            if not op_errors:
                # migrate the plain data, if applicable
                if step_plaindata:
                    pydb_common.log(logger=logger,
                                    level=INFO,
                                    msg="Started migrating the plain data")
                    plain_count = migrate_plain(errors=op_errors,
                                                source_rdbms=source_rdbms,
                                                target_rdbms=target_rdbms,
                                                source_schema=source_schema,
                                                target_schema=target_schema,
                                                source_conn=source_conn,
                                                target_conn=target_conn,
                                                migrated_tables=migrated_tables,
                                                logger=logger)
                    errors.extend(op_errors)
                    pydb_common.log(logger=logger,
                                    level=INFO,
                                    msg="Finished migrating the plain data")

                # migrate the LOB data, if applicable
                if step_lobdata:
                    pydb_common.log(logger=logger,
                                    level=INFO,
                                    msg="Started migrating the LOBs")
                    op_errors = []
                    lob_count = migrate_lobs(errors=op_errors,
                                             source_rdbms=source_rdbms,
                                             target_rdbms=target_rdbms,
                                             source_schema=source_schema,
                                             target_schema=target_schema,
                                             source_conn=source_conn,
                                             target_conn=target_conn,
                                             migrated_tables=migrated_tables,
                                             logger=logger)
                    errors.extend(op_errors)
                    pydb_common.log(logger=logger,
                                    level=INFO,
                                    msg="Finished migrating the LOBs")

                # restore target RDBMS restrictions delaying bulk copying
                op_errors = []
                restore_session_restrictions(errors=op_errors,
                                             rdbms=target_rdbms,
                                             conn=target_conn,
                                             logger=logger)

            # register possible errors and close source and target connections
            errors.extend(op_errors)
            source_conn.close()
            target_conn.close()

    finished: datetime = datetime.now()
    steps: list[str] = []
    if step_metadata:
        steps.append("migrate-metadata")
    if step_plaindata:
        steps.append("migrate-plaindata")
    if step_lobdata:
        steps.append("migrate-lobdata")

    result: dict = {
        "started": started.strftime(format=DATETIME_FORMAT_INV),
        "finished": finished.strftime(format=DATETIME_FORMAT_INV),
        "steps": steps,
        "source": {
            "rdbms": source_rdbms,
            "schema": source_schema
        },
        "target": {
            "rdbms": target_rdbms,
            "schema": target_schema
        },
        "migrated-tables": migrated_tables,
        "total-plains": plain_count,
        "total-lobs": lob_count
    }
    if include_tables:
        result["include-tables"]: include_tables
    if exclude_tables:
        result["exclude-tables"]: exclude_tables
    if external_columns:
        result["external-columns"] = {col_name: str(col_type)[str(col_type).rfind(".")+1:-2]
                                      for (col_name, col_type) in external_columns.items()}

    return result
