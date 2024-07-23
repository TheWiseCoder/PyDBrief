import warnings
from datetime import datetime
from logging import INFO, Logger
from pypomes_core import DATETIME_FORMAT_INV
from pypomes_db import db_connect
from sqlalchemy.sql.elements import Type
from typing import Any

from migration.pydb_common import get_rdbms_params, get_s3_params, log
from migration.pydb_types import type_to_name
from migration.steps.pydb_database import (
    session_disable_restrictions, session_restore_restrictions
)
from migration.steps.pydb_lobdata import migrate_lobs
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain

# treat warnings as errors
warnings.filterwarnings("error")


# structure of the migration data returned:
# {
#   "started": <yyyy-mm-ddThh:mm:ss>,
#   "finished": <yyyy-mm-ddThh:mm:ss>,
#   "version": <i.j.k>,
#   "source": {
#     "rdbms": <rdbms>,
#     "schema": <schema>,
#     "name": <db-name>,
#     "host": <db-host>,
#     "port": nnnn,
#     "user": "db-user"
#   },
#   "target": {
#     "rdbms": <rdbms>,
#     "schema": <schema>,
#     "name": <db-name>,
#     "host": <db-host>,
#     "port": nnnn,
#     "user": "db-user"
#   },
#   "steps": [
#     "migrate-metadata",
#     "migrate-plaindata",
#     "migrate-lobdata"
#   ],
#   "process-indexes": true,
#   "process-views": <bool>,
#   "include-relations": <list[str]>,
#   "exclude-relations": <list[str]>,
#   "exclude-constraints": <list[str]>,
#   "exclude-columns": <list[str]>,
#   "override-columns": [
#     "<schema>.<table>.<column>=<type>"
#   ],
#   "total-plains": nnn,
#   "total-lobs": nnn,
#   "total-tables": nnn,
#   "migrated-tables": <migrated-tables-structure>
# }
def migrate(errors: list[str],
            source_rdbms: str,
            target_rdbms: str,
            source_schema: str,
            target_schema: str,
            target_s3: str,
            step_metadata: bool,
            step_plaindata: bool,
            step_lobdata: bool,
            process_indexes: bool,
            process_views: bool,
            relax_reflection: bool,
            include_relations: list[str],
            exclude_relations: list[str],
            exclude_columns: list[str],
            exclude_constraints: list[str],
            override_columns: dict[str, Type],
            version: str,
            logger: Logger | None) -> dict:

    # set external columns to displayable list
    override_cols: list[str] = []
    for col_name, col_type in override_columns.items():
        override_cols.append(col_name + "=" +
                             type_to_name(rdbms=target_rdbms,
                                          col_type=col_type))
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
    if process_views:
        msg += ", process views"
    if include_relations:
        msg += f", include relations {','.join(include_relations)}"
    if exclude_relations:
        msg += f", exclude relations {','.join(exclude_relations)}"
    if exclude_constraints:
        msg += f", exclude constraints {','.join(exclude_constraints)}"
    if exclude_columns:
        msg += f", exclude columns {','.join(exclude_columns)}"
    if override_cols:
        msg += f", override columns {','.join(override_cols)}"
    log(logger=logger,
        level=INFO,
        msg=msg)

    # errors while obtaining connection parameters will be listed on output, only
    from_rdbms: dict[str, Any] = get_rdbms_params(errors=errors,
                                                  rdbms=source_rdbms)
    from_rdbms["schema"] = source_schema
    from_rdbms.pop("pwd")
    to_rdbms: dict[str, Any] = get_rdbms_params(errors=errors,
                                                rdbms=target_rdbms)
    to_rdbms["schema"] = target_schema
    to_rdbms.pop("pwd")
    started: datetime = datetime.now()

    # initialize the return variable
    result: dict = {
        "started": started.strftime(format=DATETIME_FORMAT_INV),
        "steps": steps[2:],
        "source-rdbms": from_rdbms,
        "target-rdbms": to_rdbms,
        "version": version
    }
    if target_s3:
        to_s3: dict[str, Any] = get_s3_params(errors=errors,
                                              s3_engine=target_s3)
        to_s3.pop("secret-key")
        result["target-s3"] = to_s3
    if process_indexes:
        result["process-indexes"] = process_views
    if process_views:
        result["process-views"] = process_views
    if include_relations:
        result["include-relations"] = include_relations
    if exclude_relations:
        result["exclude-relations"] = exclude_relations
    if exclude_constraints:
        result["exclude-constraints"] = exclude_constraints
    if exclude_columns:
        result["exclude-columns"] = exclude_columns
    if override_cols:
        result["override-columns"] = override_cols
    log(logger=logger,
        level=INFO,
        msg="Started discovering the metadata")
    migrated_tables: dict[str, Any] = \
        migrate_metadata(errors=errors,
                         source_rdbms=source_rdbms,
                         target_rdbms=target_rdbms,
                         source_schema=source_schema,
                         target_schema=target_schema,
                         target_s3=target_s3,
                         step_metadata=step_metadata,
                         process_indexes=process_indexes,
                         process_views=process_views,
                         relax_reflection=relax_reflection,
                         include_relations=include_relations,
                         exclude_relations=exclude_relations,
                         exclude_columns=exclude_columns,
                         exclude_constraints=exclude_constraints,
                         override_columns=override_columns,
                         logger=logger) or {}
    log(logger=logger,
        level=INFO,
        msg="Finished discovering the metadata")

    # proceed, if migration of plain data and/or LOB data has been indicated
    if migrated_tables and \
       (step_plaindata or step_lobdata):

        # initialize the counters
        plain_count: int = 0
        lob_count: int = 0

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
            session_disable_restrictions(errors=op_errors,
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
                                             target_s3=target_s3,
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
                session_restore_restrictions(errors=op_errors,
                                             rdbms=target_rdbms,
                                             conn=target_conn,
                                             logger=logger)

            # register possible errors and close source and target connections
            errors.extend(op_errors)
            source_conn.close()
            target_conn.close()
        
        result["total-plains"] = plain_count
        result["total-lobs"] = lob_count

    result["finished"] = datetime.now().strftime(format=DATETIME_FORMAT_INV)
    result["migrated-tables"] = migrated_tables
    result["total-tables"] = len(migrated_tables)

    return result
