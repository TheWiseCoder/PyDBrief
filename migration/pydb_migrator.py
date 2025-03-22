import json
import sys
import threading
import warnings
from datetime import datetime
from io import BytesIO
from logging import Logger
from pathlib import Path
from pypomes_core import (
    DATETIME_FORMAT_INV,
    dict_jsonify, pypomes_versions, env_is_docker,
    str_sanitize, exc_format, validate_format_error
)
from pypomes_db import (
    DbEngine, DbParam, db_connect
)
from pypomes_logging import logging_get_entries, logging_get_params
from pypomes_s3 import S3Engine, S3Param
from sqlalchemy.sql.elements import Type
from typing import Any

from app_constants import MigrationConfig
from migration.pydb_common import (
    get_rdbms_params, get_s3_params, get_migration_metrics
)
from app_constants import REGISTRY_DOCKER, REGISTRY_HOST
from migration.steps.pydb_database import (
    session_disable_restrictions, session_restore_restrictions
)
from migration.steps.pydb_lobdata import migrate_lobs
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain
from migration.steps.pydb_sync import synchronize_plain


# structure of the migration data returned:
# {
#   "started": <yyyy-mm-ddThh:mm:ss>,
#   "finished": <yyyy-mm-ddThh:mm:ss>,
#   "version": <i.j.k>,
#   "source-rdbms": {
#     "rdbms": <rdbms>,
#     "schema": <schema>,
#     "name": <db-name>,
#     "host": <db-host>,
#     "port": nnnn,
#     "user": "db-user"
#   },
#   "target-rdbms": {
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
#   "migration-badge": <migration-badge>,
#   "total-plains": nnn,
#   "total-lobs": nnn,
#   "total-tables": nnn,
#   "sync-deletes": nnn,
#   "sync-inserts": nnn,
#   "sync-updates": nnn,
#   "warnings": [
#     ...
#   ]
#   "migrated-tables": <migrated-tables-structure>
# }
def migrate(errors: list[str],
            source_rdbms: DbEngine,
            target_rdbms: DbEngine,
            source_schema: str,
            target_schema: str,
            target_s3: S3Engine,
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
            flatten_storage: bool,
            incremental_migration: dict[str, int],
            remove_nulls: list[str],
            include_relations: list[str],
            exclude_relations: list[str],
            exclude_columns: list[str],
            exclude_constraints: list[str],
            named_lobdata: list[str],
            override_columns: dict[str, Type],
            migration_badge: str,
            app_name: str,
            app_version: str,
            logger: Logger) -> dict[str, Any]:

    started: str = datetime.now().strftime(format=DATETIME_FORMAT_INV)
    steps: list = []
    if step_metadata:
        steps.append(MigrationConfig.MIGRATE_METADATA.value)
    if step_plaindata:
        steps.append(MigrationConfig.MIGRATE_PLAINDATA.value)
    if step_lobdata:
        steps.append(MigrationConfig.MIGRATE_LOBDATA.value)
    if step_synchronize:
        steps.append(MigrationConfig.SYNCHRONIZE_PLAINDATA.value)

    from_rdbms: dict[str, Any] = get_rdbms_params(errors=errors,
                                                  db_engine=source_rdbms)
    from_rdbms["schema"] = source_schema
    # avoid displaying the password
    from_rdbms.pop(str(DbParam.PWD))

    to_rdbms: dict[str, Any] = get_rdbms_params(errors=errors,
                                                db_engine=target_rdbms)
    to_rdbms["schema"] = target_schema
    # avoid displaying the password
    to_rdbms.pop(str(DbParam.PWD))

    # initialize the return variable
    result: dict = {
        "colophon": {
            app_name: app_version,
            "Foundations": pypomes_versions()
        },
        "steps": steps,
        "migration-metrics": get_migration_metrics(),
        "source-rdbms": from_rdbms,
        "target-rdbms": to_rdbms
    }
    if target_s3:
        to_s3: dict[str, Any] = get_s3_params(errors=errors,
                                              s3_engine=target_s3)
        # avoid displaying the secret key
        to_s3.pop(str(S3Param.SECRET_KEY))
        result["target-s3"] = to_s3
    if migration_badge:
        result[MigrationConfig.MIGRATION_BADGE.value] = migration_badge
    if process_indexes:
        result[MigrationConfig.PROCESS_INDEXES.value] = process_indexes
    if process_views:
        result[MigrationConfig.PROCESS_VIEWS.value] = process_views
    if include_relations:
        result[MigrationConfig.INCLUDE_RELATIONS.value] = include_relations
    if exclude_relations:
        result[MigrationConfig.EXCLUDE_RELATIONS.value] = exclude_relations
    if exclude_constraints:
        result[MigrationConfig.EXCLUDE_CONSTRAINTS.value] = exclude_constraints
    if exclude_columns:
        result[MigrationConfig.EXCLUDE_COLUMNS.value] = exclude_columns
    if override_columns:
        result[MigrationConfig.OVERRIDE_COLUMNS.value] = override_columns
    if relax_reflection:
        result[MigrationConfig.RELAX_REFLECTION.value] = relax_reflection
    if accept_empty:
        result[MigrationConfig.ACCEPT_EMPTY.value] = accept_empty
    if skip_nonempty:
        result[MigrationConfig.SKIP_NONEMPTY.value] = skip_nonempty
    if reflect_filetype:
        result[MigrationConfig.REFLECT_FILETYPE.value] = reflect_filetype
    if flatten_storage:
        result[MigrationConfig.FLATTEN_STORAGE.value] = flatten_storage
    if remove_nulls:
        result[MigrationConfig.REMOVE_NULLS.value] = remove_nulls
    if incremental_migration:
        result[MigrationConfig.INCREMENTAL_MIGRATION.value] = incremental_migration
    if named_lobdata:
        result[MigrationConfig.NAMED_LOBDATA.value] = named_lobdata

    # handle warnings as errors
    warnings.filterwarnings(action="error")

    logger.info(result)
    logger.info(msg="Started discovering the metadata")
    migration_warnings: list[str] = []

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
                         # migration_warnings=migration_warnings,
                         logger=logger) or {}
    logger.info(msg="Finished discovering the metadata")

    # proceed, if migration of plain data and/or LOB data has been indicated
    if not errors and migrated_tables and \
       (step_plaindata or step_lobdata or step_synchronize):

        # initialize the counters
        plain_count: int = 0
        lob_count: int = 0

        # obtain source and target connections
        source_conn: Any = db_connect(errors=errors,
                                      engine=source_rdbms,
                                      logger=logger)
        target_conn: Any = db_connect(errors=errors,
                                      engine=target_rdbms,
                                      logger=logger)
        if not errors:
            # disable target RDBMS restrictions to speed-up bulk operations
            session_disable_restrictions(errors=errors,
                                         rdbms=target_rdbms,
                                         conn=target_conn,
                                         logger=logger)

            # proceed, if restrictions were disabled
            if not errors:
                # migrate the plain data
                if step_plaindata:
                    logger.info("Started migrating the plain data")
                    plain_count = migrate_plain(errors=errors,
                                                source_rdbms=source_rdbms,
                                                target_rdbms=target_rdbms,
                                                source_schema=source_schema,
                                                target_schema=target_schema,
                                                skip_nonempty=skip_nonempty,
                                                incremental_migration=incremental_migration,
                                                remove_nulls=remove_nulls,
                                                source_conn=source_conn,
                                                target_conn=target_conn,
                                                migration_warnings=migration_warnings,
                                                migrated_tables=migrated_tables,
                                                logger=logger)
                    logger.info(msg="Finished migrating the plain data")

                # migrate the LOB data
                if not errors and step_lobdata:
                    # ignore warnings from 'boto3' and 'minio' packages
                    # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
                    if target_s3:
                        warnings.filterwarnings(action="ignore")

                    logger.info("Started migrating the LOBs")
                    lob_count = migrate_lobs(errors=errors,
                                             source_rdbms=source_rdbms,
                                             target_rdbms=target_rdbms,
                                             source_schema=source_schema,
                                             target_schema=target_schema,
                                             target_s3=target_s3,
                                             skip_nonempty=skip_nonempty,
                                             incremental_migration=incremental_migration,
                                             accept_empty=accept_empty,
                                             reflect_filetype=reflect_filetype,
                                             flatten_storage=flatten_storage,
                                             named_lobdata=named_lobdata,
                                             source_conn=source_conn,
                                             target_conn=target_conn,
                                             # migration_warnings=migration_warnings,
                                             migrated_tables=migrated_tables,
                                             logger=logger)
                    logger.info(msg="Finished migrating the LOBs")

                # synchronize the plain data
                if not errors and step_synchronize:
                    logger.info(msg="Started synchronizing the plain data")
                    sync_deletes, sync_inserts, sync_updates = \
                        synchronize_plain(errors=errors,
                                          source_rdbms=source_rdbms,
                                          target_rdbms=target_rdbms,
                                          source_schema=source_schema,
                                          target_schema=target_schema,
                                          remove_nulls=remove_nulls,
                                          source_conn=source_conn,
                                          target_conn=target_conn,
                                          # migration_warnings=migration_warnings,
                                          migrated_tables=migrated_tables,
                                          logger=logger)
                    logger.info(msg="Finished synchronizing the plain data")
                    result["total-sync-deletes"] = sync_deletes
                    result["total-sync-inserts"] = sync_inserts
                    result["total-sync-updates"] = sync_updates

                # restore target RDBMS restrictions delaying bulk operations
                if not errors:
                    session_restore_restrictions(errors=errors,
                                                 rdbms=target_rdbms,
                                                 conn=target_conn,
                                                 logger=logger)
            # close source and target connections
            if not errors:
                source_conn.close()
                target_conn.close()

        result["total-plains"] = plain_count
        result["total-lobs"] = lob_count

    result["started"] = started
    result["finished"] = datetime.now().strftime(format=DATETIME_FORMAT_INV)
    if migration_warnings:
        result["warnings"] = migration_warnings
    result["migrated-tables"] = migrated_tables
    result["total-tables"] = len(migrated_tables)
    result["logging"] = dict_jsonify(source=logging_get_params())

    if migration_badge:
        try:
            log_migration(errors=errors,
                          badge=migration_badge,
                          log_json=result)
        except Exception as e:
            exc_err: str = str_sanitize(target_str=exc_format(exc=e,
                                                              exc_info=sys.exc_info()))
            logger.error(msg=exc_err)
            # 101: {}
            errors.append(validate_format_error(101,
                                                exc_err))
    return result


def log_migration(errors: list[str],
                  badge: str,
                  log_json: dict[str, Any]) -> None:

    # define the base path
    base_path: str = REGISTRY_DOCKER if REGISTRY_DOCKER and env_is_docker() else REGISTRY_HOST

    log_file: Path = Path(base_path,
                          f"{badge}.log")
    # create intermediate missing folders
    log_file.parent.mkdir(parents=True,
                          exist_ok=True)
    # write the log file
    log_thread: str = str(threading.get_ident())
    log_entries: BytesIO = logging_get_entries(errors=errors,
                                               log_thread=log_thread)
    if log_entries:
        log_entries.seek(0)
        with log_file.open("wb") as f:
            f.write(log_entries.getvalue())

    # write the JSON file
    if errors:
        log_json = dict(log_json)
        log_json["errors"] = errors
    json_data = json.dumps(obj=dict_jsonify(source=log_json),
                           ensure_ascii=False,
                           indent=4)
    json_file: Path = Path(base_path,
                           f"{badge}.json")
    with json_file.open("w") as f:
        f.write(json_data)
