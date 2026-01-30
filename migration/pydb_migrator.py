import json
import os
import sys
import threading
import warnings
from datetime import datetime
from enum import StrEnum
from io import BytesIO
from logging import Logger
from pathlib import Path

from pypomes_core import (
    TZ_LOCAL, DatetimeFormat,
    dict_jsonify, timestamp_duration, pypomes_versions,
    env_is_docker, str_sanitize, exc_format, validate_format_error
)
from pypomes_db import DbEngine, db_count
from pypomes_logging import logging_get_entries, logging_get_params
from typing import Any

from app_constants import (
    REGISTRY_DOCKER, REGISTRY_HOST,
    DbConfig, SessionState,
    MigConfig, MigMetric, MigSpot, MigStep, MigSpec, MigIncremental
)
from app_ident import get_env_keys
from migration.pydb_common import get_rdbms_specs, get_s3_specs
from migration.pydb_sessions import get_session_registry
from migration.pydb_types import type_to_name
from migration.steps.pydb_lobdata import migrate_lob_tables
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain
from migration.steps.pydb_sync_lobdata import synchronize_lobs
from migration.steps.pydb_sync_plaindata import synchronize_plain


def migrate(session_id: str,
            app_name: str,
            app_version: str,
            base_url: str,
            requester: str,
            errors: list[str],
            logger: Logger) -> dict[str, Any]:

    migration_started: datetime = datetime.now(tz=TZ_LOCAL)

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, int] = session_registry[MigConfig.METRICS]
    session_steps: dict[MigStep, bool] = session_registry[MigConfig.STEPS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]

    from_rdbms: dict[DbConfig | str, Any] = get_rdbms_specs(session_id=session_id,
                                                            db_engine=session_spots[MigSpot.FROM_RDBMS],
                                                            errors=errors)
    to_rdbms: dict[DbConfig | str, Any] = get_rdbms_specs(session_id=session_id,
                                                          db_engine=session_spots[MigSpot.TO_RDBMS],
                                                          errors=errors)
    # initialize the return variable
    env_keys: list[str] = get_env_keys()
    result: dict[StrEnum | str, Any] = {
        "colophon": {
            app_name: {
                "version": app_version,
                "base-url": base_url,
                "requester": requester
            },
            "foundations": pypomes_versions(),
            "environment": {key: value for key, value in os.environ.items()
                            if key in env_keys and not ("_PWD" in key or "_SECRET" in key)}
        },
        MigSpec.SESSION_ID: session_id,
        MigConfig.METRICS: session_metrics,
        MigConfig.STEPS: session_steps,
        MigConfig.SPECS: {k: v for k, v in session_specs.items() if k != MigSpec.OVERRIDE_COLUMNS and v},
        "source-rdbms": from_rdbms,
        "target-rdbms": to_rdbms,
        "logging": logging_get_params()
    }
    override_columns: dict[MigSpec, Any] = session_specs.get(MigSpec.OVERRIDE_COLUMNS)
    if override_columns:
        result[MigConfig.SPECS][MigSpec.OVERRIDE_COLUMNS] = []
        for key, value in override_columns.items():
            name: str = type_to_name(col_type=value,
                                     rdbms=session_spots[MigSpot.TO_RDBMS])
            result[MigConfig.SPECS][MigSpec.OVERRIDE_COLUMNS].append(f"{key}={name}")
    if session_spots[MigSpot.TO_S3]:
        result["target-s3"] = get_s3_specs(session_id=session_id,
                                           s3_engine=session_spots[MigSpot.TO_S3],
                                           errors=errors)
    # handle warnings as errors
    warnings.filterwarnings(action="error")

    # initialize the local warnings list
    migration_warnings: list[str] = []

    # log the migration start
    logger.info(msg=json.dumps(obj=dict_jsonify(source=result),
                               ensure_ascii=False))
    logger.info(msg="Started discovering the metadata")

    # set state for session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_registry[MigSpec.STATE] = SessionState.MIGRATING

    migrated_tables: dict[str, Any] = migrate_metadata(session_id=session_id,
                                                       migration_warnings=migration_warnings,
                                                       errors=errors,
                                                       logger=logger) or {}
    logger.info(msg="Finished discovering the metadata")

    # initialize the thread registration
    migration_threads: list[int] = [threading.get_ident()]

    # proceed, if migration of plain data and/or LOB data has been indicated
    if not errors and migrated_tables and \
        (session_steps[MigStep.MIGRATE_PLAINDATA] or
         session_steps[MigStep.MIGRATE_LOBDATA] or
         session_steps[MigStep.SYNCHRONIZE_PLAINDATA] or
         session_steps[MigStep.SYNCHRONIZE_LOBDATA]):

        # establish incremental migration sizes and offsets
        incr_migrations: dict[str, dict[MigIncremental, int]] = session_specs[MigSpec.INCR_MIGRATIONS]
        if not errors and incr_migrations:
            incr_migrations = __establish_increments(migrating_tables=list(migrated_tables.keys()),
                                                     incr_migrations=session_specs[MigSpec.INCR_MIGRATIONS],
                                                     target_rdbms=(None if session_spots[MigSpot.TO_S3]
                                                                   else session_spots[MigSpot.TO_RDBMS]),
                                                     target_schema=session_specs[MigSpec.TO_SCHEMA],
                                                     inc_size=session_metrics.get(MigMetric.INCREMENTAL_SIZE),
                                                     errors=errors)
        # migrate the plain data
        if not errors and session_steps[MigStep.MIGRATE_PLAINDATA]:
            logger.info("Started migrating the plain data")
            started: datetime = datetime.now(tz=TZ_LOCAL)
            count: int = migrate_plain(session_id=session_id,
                                       incr_migrations=incr_migrations,
                                       migration_threads=migration_threads,
                                       migrated_tables=migrated_tables,
                                       migration_warnings=migration_warnings,
                                       errors=errors,
                                       logger=logger)
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            result["total-plain-count"] = count
            result["total-plain-duration"] = duration
            if count > 0:
                secs: float = (finished - started).total_seconds()
                result["total-plain-performance"] = f"{count/secs:.2f} tuples/s"
            logger.info(msg="Finished migrating the plain data")

        # migrate the LOB data
        if not errors and session_steps[MigStep.MIGRATE_LOBDATA]:
            logger.info("Started migrating the LOBs")

            # ignore warnings from 'boto3' and 'minio' packages
            # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
            if session_spots[MigSpot.TO_S3]:
                warnings.filterwarnings(action="ignore")

            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int] = migrate_lob_tables(session_id=session_id,
                                                         incr_migrations=incr_migrations,
                                                         migration_threads=migration_threads,
                                                         migrated_tables=migrated_tables,
                                                         migration_warnings=migration_warnings,
                                                         errors=errors,
                                                         logger=logger)
            lob_count: int = counts[0]
            lob_bytes: int = counts[1]
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            mins: float = (finished - started).total_seconds() / 60
            performance: str = (f"{lob_count/mins:.2f} LOBs/min, "
                                f"{lob_bytes/(mins * 1024 ** 2):.2f} MBytes/min")
            result.update({
                "total-lob-count": lob_count,
                "total-lob-bytes": lob_bytes,
                "total-lob-duration": duration,
                "total-lob-performance": performance
            })
            logger.debug(msg=f"Finished migrating {lob_count} LOBs, "
                             f"{lob_bytes} bytes, in {duration} ({performance})")

        # synchronize the plain data
        if not errors and session_steps[MigStep.SYNCHRONIZE_PLAINDATA]:
            logger.info(msg="Started synchronizing the plain data")
            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int, int] = synchronize_plain(session_id=session_id,
                                                             migration_threads=migration_threads,
                                                             migrated_tables=migrated_tables,
                                                             # migration_warnings=migration_warnings,
                                                             errors=errors,
                                                             logger=logger)
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            result.update({
                "total-sync-deletes": counts[0],
                "total-sync-inserts": counts[1],
                "total-sync-updates": counts[2],
                "total-sync-duration": duration
            })
            logger.info(msg="Finished synchronizing the plain data")

        # synchronize the LOBs
        if not errors and session_steps[MigStep.SYNCHRONIZE_LOBDATA]:
            logger.info(msg="Started synchronizing the LOBs")

            # ignore warnings from 'boto3' and 'minio' packages
            # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
            warnings.filterwarnings(action="ignore")

            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int, int] = synchronize_lobs(session_id=session_id,
                                                            migration_threads=migration_threads,
                                                            migrated_tables=migrated_tables,
                                                            migration_warnings=migration_warnings,
                                                            errors=errors,
                                                            logger=logger)
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            result.update({
                "total-sync-count": counts[0],
                "total-sync-deletes": counts[1],
                "total-sync-inserts": counts[2],
                "total-sync-duration": duration
            })
            logger.info(msg="Finished synchronizing the LOBs")

    # update the session state
    curr_state: SessionState = session_registry.get(MigSpec.STATE)
    new_state: SessionState = SessionState.FINISHED \
        if curr_state == SessionState.MIGRATING else SessionState.ABORTED
    session_registry[MigSpec.STATE] = new_state

    migration_finished: datetime = datetime.now(tz=TZ_LOCAL)
    result.update({
        "total-tables": len(migrated_tables),
        "migrated-tables": migrated_tables,
        "started": migration_started.strftime(format=DatetimeFormat.INV),
        "finished": migration_finished.strftime(format=DatetimeFormat.INV),
        "duration": timestamp_duration(start=migration_started,
                                       finish=migration_finished)
    })
    if migration_warnings:
        result["warnings"] = migration_warnings

    if session_specs[MigSpec.MIGRATION_BADGE]:
        try:
            __log_migration(badge=session_specs[MigSpec.MIGRATION_BADGE],
                            threads=migration_threads,
                            errors=errors,
                            log_json=result)
        except Exception as e:
            exc_err: str = str_sanitize(exc_format(exc=e,
                                                   exc_info=sys.exc_info()))
            logger.error(msg=exc_err)
            # 101: {}
            errors.append(validate_format_error(101,
                                                exc_err))
    return result


def __establish_increments(migrating_tables: list[str],
                           incr_migrations: dict[str, dict[MigIncremental, int]],
                           target_rdbms: DbEngine | None,
                           target_schema: str,
                           inc_size: int,
                           errors: list[str]) -> dict[str, dict[MigIncremental, int]]:

    result = incr_migrations.copy()

    for key, value in incr_migrations.items():
        if key in migrating_tables:
            size: int = value.get(MigIncremental.COUNT)
            if not size:
                size = inc_size
            elif size == -1:
                size = 0
            offset: int = value.get(MigIncremental.OFFSET)
            if not offset:
                offset = 0
            elif offset == -1:
                if target_rdbms:
                    offset = db_count(table=f"{target_schema}.{key}",
                                      engine=target_rdbms,
                                      committable=True,
                                      errors=errors)
                    if errors:
                        break
                else:
                    offset = 0
            result[key][MigIncremental.COUNT] = size
            result[key][MigIncremental.OFFSET] = offset
        else:
            result.pop(key)

    return result


def __log_migration(badge: str,
                    threads: list[int],
                    log_json: dict[str, Any],
                    errors: list[str]) -> None:

    # define the base path
    base_path: str = REGISTRY_DOCKER if REGISTRY_DOCKER and env_is_docker() else REGISTRY_HOST

    log_file: Path = Path(base_path,
                          f"{badge}.log")
    # create intermediate missing folders
    log_file.parent.mkdir(parents=True,
                          exist_ok=True)
    # write the log file
    log_entries: BytesIO = logging_get_entries(log_threads=list(map(str, set(threads))),
                                               errors=errors)
    if log_entries:
        log_entries.seek(0)
        with log_file.open("wb") as f:
            f.write(log_entries.getvalue())

    # write the JSON file
    if errors:
        log_json = log_json.copy()
        log_json["errors"] = errors
    json_data = json.dumps(obj=log_json,
                           ensure_ascii=False,
                           indent=4)
    json_file: Path = Path(base_path,
                           f"{badge}.json")
    with json_file.open("w") as f:
        f.write(json_data)
