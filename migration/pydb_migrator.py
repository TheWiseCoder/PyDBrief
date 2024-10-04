import json
import sys
import warnings
from datetime import datetime
from io import BytesIO
from logging import Logger, FileHandler
from pathlib import Path
from pypomes_core import (
    DATETIME_FORMAT_INV,
    env_is_docker, dict_jsonify, str_sanitize, exc_format, validate_format_error
)
from pypomes_db import db_connect
from pypomes_logging import logging_get_entries
from sqlalchemy.sql.elements import Type
from typing import Any

from migration.pydb_common import (
    REGISTRY_DOCKER, REGISTRY_HOST,
    get_rdbms_params, get_s3_params
)
from migration.pydb_types import type_to_name
from migration.steps.pydb_database import (
    session_disable_restrictions, session_restore_restrictions
)
from migration.steps.pydb_lobdata import migrate_lobs
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain
from migration.steps.pydb_sync import synchronize_plain

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
#     "migrate-lobdata",
#     "synchronize-plaindata"
#   ],
#   "process-indexes": <bool>>,
#   "process-views": <bool>,
#   "relax-reflection": <bool>
#   "accept-empty": <bool>
#   "skip-nonempty": <bool>,
#   "include-relations": <list[str]>,
#   "exclude-relations": <list[str]>,
#   "exclude-constraints": <list[str]>,
#   "exclude-columns": <list[str]>,
#   "override-columns": [
#     "<table-name>.<column-name>=<type>"
#   ],
#   "remove-nulls": [
#     "<table-name>"
#   ]
#   "named-lobdata": [
#     "<table-name>.<lob-column>=<names-column>[.<extension>]"
#   ],
#   "migration-id": <migration-id>,
#   "total-plains": nnn,
#   "total-lobs": nnn,
#   "total-tables": nnn,
#   "sync-deletes": nnn,
#   "sync-inserts": nnn,
#   "sync-updates": nnn,
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
            step_synchronize: bool,
            process_indexes: bool,
            process_views: bool,
            relax_reflection: bool,
            accept_empty: bool,
            skip_nonempty: bool,
            reflect_filetype: bool,
            remove_nulls: list[str],
            include_relations: list[str],
            exclude_relations: list[str],
            exclude_columns: list[str],
            exclude_constraints: list[str],
            named_lobdata: list[str],
            override_columns: dict[str, Type],
            migration_badge: str,
            version: str,
            logger: Logger | None) -> dict[str, Any]:

    # set external columns to displayable list
    override_cols: list[str] = []
    for col_name, col_type in override_columns.items():
        override_cols.append(col_name + "=" +
                             type_to_name(rdbms=target_rdbms,
                                          col_type=col_type))
    # log the start of the migration
    msg: str = (f"Migration started, "
                f"from {source_rdbms}.{source_schema} "
                f"to {target_rdbms}.{target_schema}: ")
    steps: list[str] = []
    if step_metadata:
        steps.append("migrate-metadata")
    if step_plaindata:
        steps.append("migrate-plaindata")
    if step_lobdata:
        steps.append("migrate-lobdata")
    if step_synchronize:
        steps.append("synchronize-plaindata")
    msg += f"steps {','.join(steps)}"
    if process_indexes:
        msg += "; process indexes"
    if process_views:
        msg += "; process views"
    if relax_reflection:
        msg += "; relax reflection"
    if accept_empty:
        msg += "; accept empty"
    if skip_nonempty:
        msg += "; skip nonempty"
    if reflect_filetype:
        msg += "; reflect filetype"
    if migration_badge:
        msg += f"; migration badge '{migration_badge}'"
    if remove_nulls:
        msg += f"; remove nulls {','.join(remove_nulls)}"
    if include_relations:
        msg += f"; include relations {','.join(include_relations)}"
    if exclude_relations:
        msg += f"; exclude relations {','.join(exclude_relations)}"
    if exclude_constraints:
        msg += f"; exclude constraints {','.join(exclude_constraints)}"
    if exclude_columns:
        msg += f"; exclude columns {','.join(exclude_columns)}"
    if override_cols:
        msg += f"; override columns {','.join(override_cols)}"
    if named_lobdata:
        msg += f"; exclude columns {','.join(named_lobdata)}"
    logger.info(msg=msg)

    # errors while obtaining connection parameters will be listed on output, only
    from_rdbms: dict[str, Any] = get_rdbms_params(errors=errors,
                                                  rdbms=source_rdbms)
    from_rdbms["schema"] = source_schema
    from_rdbms.pop("pwd")
    dict_jsonify(source=from_rdbms)
    to_rdbms: dict[str, Any] = get_rdbms_params(errors=errors,
                                                rdbms=target_rdbms)
    to_rdbms["schema"] = target_schema
    to_rdbms.pop("pwd")
    dict_jsonify(source=to_rdbms)
    started: datetime = datetime.now()

    # initialize the return variable
    result: dict = {
        "started": started.strftime(format=DATETIME_FORMAT_INV),
        "steps": steps,
        "source-rdbms": from_rdbms,
        "target-rdbms": to_rdbms,
        "version": version
    }
    if migration_badge:
        result["migration-badge"] = migration_badge
    if target_s3:
        to_s3: dict[str, Any] = get_s3_params(errors=errors,
                                              s3_engine=target_s3)
        to_s3.pop("secret-key")
        result["target-s3"] = to_s3
    if process_indexes:
        result["process-indexes"] = process_indexes
    if process_views:
        result["process-views"] = process_views
    if process_views:
        result["relax-reflection"] = relax_reflection
    if accept_empty:
        result["accept-empty"] = accept_empty
    if skip_nonempty:
        result["skip-nonempty"] = skip_nonempty
    if reflect_filetype:
        result["reflect-filetype"] = reflect_filetype
    if remove_nulls:
        result["remove-nulls"] = remove_nulls
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
    if named_lobdata:
        result["named-lobdata"] = named_lobdata

    logger.info(msg="Started discovering the metadata")

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
    logger.info(msg="Finished discovering the metadata")

    # proceed, if migration of plain data and/or LOB data has been indicated
    if migrated_tables and \
       (step_plaindata or step_lobdata or step_synchronize):

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
                # migrate the plain data
                if step_plaindata:
                    logger.info("Started migrating the plain data")
                    plain_count = migrate_plain(errors=op_errors,
                                                source_rdbms=source_rdbms,
                                                target_rdbms=target_rdbms,
                                                source_schema=source_schema,
                                                target_schema=target_schema,
                                                skip_nonempty=skip_nonempty,
                                                remove_nulls=remove_nulls,
                                                source_conn=source_conn,
                                                target_conn=target_conn,
                                                migrated_tables=migrated_tables,
                                                logger=logger)
                    errors.extend(op_errors)
                    logger.info(msg="Finished migrating the plain data")

                # migrate the LOB data
                if step_lobdata:
                    logger.info("Started migrating the LOBs")
                    op_errors = []
                    lob_count = migrate_lobs(errors=op_errors,
                                             source_rdbms=source_rdbms,
                                             target_rdbms=target_rdbms,
                                             source_schema=source_schema,
                                             target_schema=target_schema,
                                             target_s3=target_s3,
                                             skip_nonempty=skip_nonempty,
                                             accept_empty=accept_empty,
                                             reflect_filetype=reflect_filetype,
                                             named_lobdata=named_lobdata,
                                             source_conn=source_conn,
                                             target_conn=target_conn,
                                             migrated_tables=migrated_tables,
                                             logger=logger)
                    errors.extend(op_errors)
                    logger.info(msg="Finished migrating the LOBs")

                # synchronize the plain data
                if step_synchronize:
                    logger.info(msg="Started synchronizing the plain data")
                    op_errors = []
                    sync_deletes, sync_inserts, sync_updates = \
                        synchronize_plain(errors=op_errors,
                                          source_rdbms=source_rdbms,
                                          target_rdbms=target_rdbms,
                                          source_schema=source_schema,
                                          target_schema=target_schema,
                                          remove_nulls=remove_nulls,
                                          source_conn=source_conn,
                                          target_conn=target_conn,
                                          migrated_tables=migrated_tables,
                                          logger=logger)
                    errors.extend(op_errors)
                    logger.info(msg="Finished synchronizing the plain data")
                    result["sync-deletes"] = sync_deletes
                    result["sync-inserts"] = sync_inserts
                    result["sync-updates"] = sync_updates

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

    if logger and logger.root:
        for handler in logger.root.handlers:
            if isinstance(handler, FileHandler):
                result["log-file"] = Path(handler.baseFilename).as_posix()
                break
    finished: datetime = datetime.now()
    result["finished"] = finished.strftime(format=DATETIME_FORMAT_INV)
    result["migrated-tables"] = migrated_tables
    result["total-tables"] = len(migrated_tables)

    if migration_badge:
        try:
            log_migration(errors=errors,
                          badge=migration_badge,
                          log_json=result,
                          log_from=started)
        except Exception as e:
            exc_err: str = str_sanitize(exc_format(exc=e,
                                                   exc_info=sys.exc_info()))
            # 101: {}
            errors.append(validate_format_error(101,
                                                exc_err))
    return result


def log_migration(errors: list[str],
                  badge: str,
                  log_json: dict[str, Any],
                  log_from: datetime) -> None:

    # define the base path
    base_path: str = REGISTRY_DOCKER if REGISTRY_DOCKER and env_is_docker() else REGISTRY_HOST

    # write the log file (create intermediate missing folders)
    log_entries: BytesIO = logging_get_entries(errors=errors,
                                               log_from=log_from)
    if log_entries:
        log_entries.seek(0)
        log_file: Path = Path(base_path, f"{badge}.log")
        log_file.parent.mkdir(parents=True,
                              exist_ok=True)
        with log_file.open("wb") as f:
            f.write(log_entries.getvalue())

    # write the JSON file
    if errors:
        log_json = dict(log_json)
        log_json["errors"]: errors
    json_data = json.dumps(obj=log_json,
                           ensure_ascii=False,
                           indent=4)
    json_file: Path = Path(base_path, f"{badge}.json")
    with json_file.open("w") as f:
        f.write(json_data)
