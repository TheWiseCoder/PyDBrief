import json
import sys
import threading
import warnings
from datetime import datetime
from enum import StrEnum
from io import BytesIO
from logging import Logger
from pathlib import Path
from pypomes_core import (
    TIMEZONE_LOCAL, DatetimeFormat,
    dict_jsonify, timestamp_duration, pypomes_versions,
    env_is_docker, str_sanitize, exc_format, validate_format_error
)
from pypomes_db import DbEngine, db_count
from pypomes_logging import logging_get_entries, logging_get_params
from typing import Any

from app_constants import (
    REGISTRY_DOCKER, REGISTRY_HOST,
    DbConfig, MigrationState,
    MigConfig, MigMetric, MigSpot, MigStep, MigSpec
)
from migration.pydb_common import get_rdbms_specs, get_s3_specs
from migration.pydb_sessions import get_session_registry
from migration.steps.pydb_lobdata import migrate_lob_tables
from migration.steps.pydb_metadata import migrate_metadata
from migration.steps.pydb_plaindata import migrate_plain
from migration.steps.pydb_sync_lobdata import synchronize_lobs
from migration.steps.pydb_sync_plaindata import synchronize_plain


def migrate(errors: list[str],
            session_id: str,
            app_name: str,
            app_version: str,
            logger: Logger) -> dict[str, Any]:

    migration_started: datetime = datetime.now(tz=TIMEZONE_LOCAL)

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, int] = session_registry[MigConfig.METRICS]
    session_steps: dict[MigStep, bool] = session_registry[MigConfig.STEPS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]

    from_rdbms: dict[DbConfig | str, Any] = get_rdbms_specs(errors=errors,
                                                            session_id=session_id,
                                                            db_engine=session_spots[MigSpot.FROM_RDBMS])
    to_rdbms: dict[DbConfig | str, Any] = get_rdbms_specs(errors=errors,
                                                          session_id=session_id,
                                                          db_engine=session_spots[MigSpot.TO_RDBMS])
    # initialize the return variable
    result: dict[StrEnum | str, Any] = {
        "colophon": {
            app_name: app_version,
            "foundations": pypomes_versions()
        },
        MigSpec.SESSION_ID: session_id,
        MigConfig.METRICS: session_metrics,
        MigConfig.STEPS: session_steps,
        MigConfig.SPECS: session_specs,
        "source-rdbms": from_rdbms,
        "target-rdbms": to_rdbms,
        "logging": logging_get_params()
    }
    if session_spots[MigSpot.TO_S3]:
        result["target-s3"] = get_s3_specs(errors=errors,
                                           session_id=session_id,
                                           s3_engine=session_spots[MigSpot.TO_S3])
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
    session_registry[MigSpec.STATE] = MigrationState.MIGRATING

    migrated_tables: dict[str, Any] = migrate_metadata(errors=errors,
                                                       session_id=session_id,
                                                       # migration_warnings=migration_warnings,
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
        incremental_migrations: dict[str, tuple[int, int]] = {}
        if not errors and session_specs[MigSpec.INCREMENTAL_MIGRATIONS]:
            incremental_migrations = \
                __establish_increments(errors=errors,
                                       migrating_tables=list(migrated_tables.keys()),
                                       incremental_migrations=session_specs[MigSpec.INCREMENTAL_MIGRATIONS],
                                       target_rdbms=session_spots[MigSpot.TO_RDBMS],
                                       target_schema=session_specs[MigSpec.TO_SCHEMA],
                                       incremental_size=session_metrics.get(MigMetric.INCREMENTAL_SIZE),
                                       logger=logger)
        # migrate the plain data
        if not errors and session_steps[MigStep.MIGRATE_PLAINDATA]:
            logger.info("Started migrating the plain data")
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            count: int = migrate_plain(errors=errors,
                                       session_id=session_id,
                                       incremental_migrations=incremental_migrations,
                                       migration_warnings=migration_warnings,
                                       migration_threads=migration_threads,
                                       migrated_tables=migrated_tables,
                                       logger=logger)
            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
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

            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            counts: tuple[int, int] = migrate_lob_tables(errors=errors,
                                                         session_id=session_id,
                                                         incremental_migrations=incremental_migrations,
                                                         migration_warnings=migration_warnings,
                                                         migration_threads=migration_threads,
                                                         migrated_tables=migrated_tables,
                                                         logger=logger)
            lob_count: int = counts[0]
            lob_bytes: int = counts[1]
            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            secs: float = (finished - started).total_seconds()
            result.update({
                "total-lob-count": lob_count,
                "total-lob-bytes": lob_bytes,
                "total-lob-duration": duration,
                "total-lob-performance": f"{lob_count/secs:.2f} LOBs/s, {lob_bytes/secs:.2f} bytes/s"
            })
            logger.info(msg="Finished migrating the LOBs")

        # synchronize the plain data
        if not errors and session_steps[MigStep.SYNCHRONIZE_PLAINDATA]:
            logger.info(msg="Started synchronizing the plain data")
            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            counts: tuple[int, int, int] = synchronize_plain(errors=errors,
                                                             session_id=session_id,
                                                             # migration_warnings=migration_warnings,
                                                             migration_threads=migration_threads,
                                                             migrated_tables=migrated_tables,
                                                             logger=logger)
            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
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

            started: datetime = datetime.now(tz=TIMEZONE_LOCAL)
            counts: tuple[int, int, int] = synchronize_lobs(errors=errors,
                                                            session_id=session_id,
                                                            migration_warnings=migration_warnings,
                                                            migration_threads=migration_threads,
                                                            migrated_tables=migrated_tables,
                                                            logger=logger)
            finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
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
    curr_state: MigrationState = session_registry.get(MigSpec.STATE)
    new_state: MigrationState = MigrationState.FINISHED \
        if curr_state == MigrationState.MIGRATING else MigrationState.ABORTED
    session_registry[MigSpec.STATE] = new_state

    migration_finished: datetime = datetime.now(tz=TIMEZONE_LOCAL)
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
            __log_migration(errors=errors,
                            badge=session_specs[MigSpec.MIGRATION_BADGE],
                            threads=migration_threads,
                            log_json=result)
        except Exception as e:
            exc_err: str = str_sanitize(source=exc_format(exc=e,
                                                          exc_info=sys.exc_info()))
            logger.error(msg=exc_err)
            # 101: {}
            errors.append(validate_format_error(101,
                                                exc_err))
    return result


def __establish_increments(errors: list[str],
                           migrating_tables: list[str],
                           incremental_migrations: dict[str, tuple[int, int]],
                           target_rdbms: DbEngine,
                           target_schema: str,
                           incremental_size: int,
                           logger: Logger) -> dict[str, tuple[int, int]]:

    # a shallow copy suffices, as tuples are immutable objects
    result = incremental_migrations.copy()

    for key, value in incremental_migrations.items():
        if key in migrating_tables:
            size: int = value[0]
            if not size:
                size = incremental_size
            elif size == -1:
                size = 0
            offset: int = value[1]
            if not offset:
                offset = 0
            elif offset == -1:
                offset = db_count(errors=errors,
                                  table=f"{target_schema}.{key}",
                                  engine=target_rdbms,
                                  committable=True,
                                  logger=logger)
                if errors:
                    break
            result[key] = (size, offset)
        else:
            result.pop(key)

    return result


def __log_migration(errors: list[str],
                    badge: str,
                    threads: list[int],
                    log_json: dict[str, Any]) -> None:

    # define the base path
    base_path: str = REGISTRY_DOCKER if REGISTRY_DOCKER and env_is_docker() else REGISTRY_HOST

    log_file: Path = Path(base_path,
                          f"{badge}.log")
    # create intermediate missing folders
    log_file.parent.mkdir(parents=True,
                          exist_ok=True)
    # write the log file
    log_entries: BytesIO = logging_get_entries(errors=errors,
                                               log_threads=list(map(str, set(threads))))
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
