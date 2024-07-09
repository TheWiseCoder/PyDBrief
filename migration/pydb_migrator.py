import warnings
from datetime import datetime
from logging import INFO, Logger
from pypomes_core import DATETIME_FORMAT_INV
from pypomes_db import db_connect
from sqlalchemy.sql.elements import Type
from typing import Any

from migration.pydb_common import get_connection_params, log
from migration.steps.pydb_database import (
    disable_session_restrictions, restore_session_restrictions
)
from migration.steps.pydb_lobdata import migrate_lobs
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain

# treat warnings as errors
warnings.filterwarnings("error")


# structure of the migration data returned:
# {
#   "started": <yyyy-mm-dd>,
#   "finished": <yyyy-mm-dd>,
#   "version": <i.j.k>,
#   "source": {
#     "rdbms": <rdbms>,
#     "schema": <schema>
#   },
#   "target": {
#     "rdbms": <rdbms>,
#     "schema": <schema>
#   },
#   "steps": [
#     "migrate-metadata",
#     "migrate-plaindata",
#     "migrate-lobdata"
#   ],
#   "process-indexes": true,
#   "include-tables": <list[str]>,
#   "exclude-tables": <list[str]>,
#   "include-views": <list[str]>,
#   "exclude-columns": <list[str]>,
#   "exclude-constraints": <list[str]>,
#   "external-columns": {
#     "<schema>.<table>.<column>": "<type>"
#   }
#   "total-plains": nnn,
#   "total-lobs": nnn,
#   "migrated-tables": <migrated-tables-structure>
# }
def migrate(errors: list[str],
            source_rdbms: str,
            target_rdbms: str,
            source_schema: str,
            target_schema: str,
            step_metadata: bool,
            step_plaindata: bool,
            step_lobdata: bool,
            process_indexes: bool,
            relax_reflection: bool,
            include_tables: list[str],
            exclude_tables: list[str],
            include_views: list[str],
            exclude_columns: list[str],
            exclude_constraints: list[str],
            external_columns: dict[str, Type],
            version: str,
            logger: Logger | None) -> dict:

    # log the start of the migration
    msg: str = (f"Migration started, "
                f"from {source_rdbms}.{source_schema} "
                f"to {target_rdbms}.{target_schema} ")
    steps: str = ""
    if step_metadata:
        steps += ", metadata"
    if step_plaindata:
        steps += ", plaindata"
    if step_lobdata:
        steps += ", lobdata"
    msg += f"steps {steps[2:]}"
    if process_indexes:
        msg += ", process indexes"
    if include_tables:
        msg += f", include tables {','.join(include_tables)}"
    if exclude_tables:
        msg += f", exclude tables {','.join(exclude_tables)}"
    if include_views:
        msg += f", include views {','.join(include_views)}"
    if exclude_constraints:
        msg += f", exclude constraints {','.join(exclude_constraints)}"
    log(logger=logger,
        level=INFO,
        msg=msg)

    # errors while obtaining connection parameters will be listed on output, only
    from_rdbms: dict[str, Any] = get_connection_params(errors=errors,
                                                       rdbms=source_rdbms)
    from_rdbms["schema"] = source_schema
    from_rdbms.pop("pwd")
    to_rdbms: dict[str, Any] = get_connection_params(errors=errors,
                                                     rdbms=target_rdbms)
    to_rdbms["schema"] = target_schema
    to_rdbms.pop("pwd")
    started: datetime = datetime.now()

    # initialize the return variable
    result: dict = {
        "started": started.strftime(format=DATETIME_FORMAT_INV),
        "steps": steps,
        "source": from_rdbms,
        "target": to_rdbms,
        "version": version
    }
    if step_metadata:
        result["process-indexes"] = process_indexes
    if include_tables:
        result["include-tables"] = include_tables
    if exclude_tables:
        result["exclude-tables"] = exclude_tables
    if include_views:
        result["include-views"] = include_tables
    if exclude_columns:
        result["exclude-columns"] = exclude_columns
    if exclude_constraints:
        result["exclude-constraints"] = exclude_constraints
    if external_columns:
        result["external-columns"] = {col_name: str(col_type())
                                      for (col_name, col_type) in external_columns.items()}
    log(logger=logger,
        level=INFO,
        msg="Started discovering the metadata")
    migrated_tables: dict = migrate_metadata(errors=errors,
                                             source_rdbms=source_rdbms,
                                             target_rdbms=target_rdbms,
                                             source_schema=source_schema,
                                             target_schema=target_schema,
                                             step_metadata=step_metadata,
                                             process_indexes=process_indexes,
                                             relax_reflection=relax_reflection,
                                             include_tables=include_tables,
                                             exclude_tables=exclude_tables,
                                             include_views=include_views,
                                             exclude_columns=exclude_columns,
                                             exclude_constraints=exclude_constraints,
                                             external_columns=external_columns,
                                             logger=logger)
    log(logger=logger,
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
                    log(logger=logger,
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
                    log(logger=logger,
                        level=INFO,
                        msg="Finished migrating the plain data")

                # migrate the LOB data, if applicable
                if step_lobdata:
                    log(logger=logger,
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
                    log(logger=logger,
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

    result["finished"] = datetime.now()
    result["migrated-tables"] = migrated_tables
    result["total-plains"] = plain_count
    result["total-lobs"] = lob_count

    return result
