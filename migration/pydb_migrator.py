import json
import os
import sys
import threading
import warnings
from datetime import datetime
from io import BytesIO
from logging import Logger
from pypomes_core import (
    TZ_LOCAL, DatetimeFormat,
    timestamp_duration, env_is_docker, pypomes_versions,
    dict_jsonify, str_sanitize, validate_format_error, exc_format
)
from pypomes_logging import logging_get_entries, logging_get_params
from pathlib import Path
from typing import Any

from app_constants import REGISTRY_DOCKER, REGISTRY_HOST, PYDB_DB_ENGINE, InputParam
from app_ident import get_env_keys
from entities.migration import Migration, MigStep
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_table import MigrationTable
from entities.migration_work import MigrationWork
from entities.session import Session, SessionState
from migration.steps.pydb_correlate_lobdata import correlate_lobdata
from migration.steps.pydb_migrate_lobdata import migrate_lobdata
from migration.steps.pydb_migrate_metadata import migrate_metadata
from migration.steps.pydb_migrate_plaindata import migrate_plaindata
from migration.steps.pydb_sync_plaindata import synchronize_plaindata


def migrate(migration: Migration,
            session: Session,
            app_name: str,
            app_version: str,
            base_url: str,
            requester: str,
            logger: Logger) -> None:

    # time the migration start
    migration_started: datetime = datetime.now(tz=TZ_LOCAL)

    # initialize the errors list
    errors: list[str] = []

    # initialize the operation report
    env_keys: list[str] = get_env_keys()
    op_report: dict[str, Any] = {
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
        InputParam.SESSION: session.cd_session,
        InputParam.SOURCE_DB: session.get_source_db().get_inputs(),
        InputParam.TARGET_DB: session.get_target_db().get_inputs(),
        InputParam.SPECS: migration.get_inputs(),
        "logging": logging_get_params(),
    }
    if session.get_target_s3():
        op_report[InputParam.TARGET_S3] = session.get_target_s3().get_inputs()

    migration_tables: list[MigrationTable] = migration.get_migration_tables()
    if migration_tables:
        table_specs: list[dict[str, Any]] = []
        for migration_table in migration_tables:
            table_specs.append(migration_table.get_inputs())
        op_report["table-specs"] = table_specs

    # handle warnings as errors
    warnings.filterwarnings(action="error")

    # initialize the local warnings list
    migration_warnings: list[str] = []

    # log the migration start
    logger.info(msg=json.dumps(obj=dict_jsonify(source=op_report),
                               ensure_ascii=False))
    logger.info(msg="Started discovering the metadata")
    migrated_tables: dict[str, Any] = migrate_metadata(migration=migration,
                                                       session=session,
                                                       migration_warnings=migration_warnings,
                                                       errors=errors,
                                                       logger=logger) or {}
    logger.info(msg="Finished discovering the metadata")

    # initialize the thread registration
    migration_threads: list[int] = [threading.get_ident()]

    # proceed, if migration/synchronization/correlation has been indicated
    if not errors and migrated_tables and migration.cd_step != MigStep.MIGRATE_METADATA:

        # migrate the plain data
        if migration.cd_step == MigStep.MIGRATE_PLAINDATA:
            logger.info("Started migrating the plain data")
            started: datetime = datetime.now(tz=TZ_LOCAL)
            count: int = migrate_plaindata(migration=migration,
                                           session=session,
                                           migration_threads=migration_threads,
                                           migrated_tables=migrated_tables,
                                           migration_warnings=migration_warnings,
                                           errors=errors,
                                           logger=logger)
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            op_report["total-plain-count"] = count
            op_report["total-plain-duration"] = duration
            if count > 0:
                secs: float = (finished - started).total_seconds()
                op_report["total-plain-performance"] = f"{count/secs:.2f} tuples/s"
            logger.info(msg="Finished migrating the plain data")

        # migrate the LOB data
        if not errors and migration.cd_step == MigStep.MIGRATE_LOBDATA:
            logger.info("Started migrating the LOBs")

            # ignore warnings from 'boto3' and 'minio' packages
            # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
            if session.id_target_s3:
                warnings.filterwarnings(action="ignore")

            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int] = migrate_lobdata(migration=migration,
                                                      session=session,
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
            op_report.update({
                "total-lob-count": lob_count,
                "total-lob-bytes": lob_bytes,
                "total-lob-duration": duration,
                "total-lob-performance": performance
            })
            logger.debug(msg=f"Finished migrating {lob_count} LOBs, "
                             f"{lob_bytes} bytes, in {duration} ({performance})")

        # correlate the LOBs
        if not errors and migration.cd_step == MigStep.CORRELATE_LOBDATA:
            logger.info(msg="Started correlating the LOBs")

            # ignore warnings from 'boto3' and 'minio' packages
            # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
            warnings.filterwarnings(action="ignore")

            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int, int] = correlate_lobdata(migration=migration,
                                                             session=session,
                                                             migration_threads=migration_threads,
                                                             migrated_tables=migrated_tables,
                                                             migration_warnings=migration_warnings,
                                                             errors=errors,
                                                             logger=logger)
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            op_report.update({
                "total-lob-count": counts[0],
                "total-lob-deletes": counts[1],
                "total-lob-inserts": counts[2],
                "total-lob-duration": duration
            })
            logger.info(msg="Finished correlating the LOBs")

        # correlate/synchronize the plain data
        if not errors and migration.cd_step in [MigStep.CORRELATE_PLAINDATA, MigStep.SYNCHRONIZE_PLAINDATA]:
            op: str = "correlating" if migration.cd_step == MigStep.CORRELATE_PLAINDATA else "synchronizing"
            logger.info(msg=f"Started {op} the plain data")
            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int, int] = synchronize_plaindata(migration=migration,
                                                                 session=session,
                                                                 migration_threads=migration_threads,
                                                                 migrated_tables=migrated_tables,
                                                                 # migration_warnings=migration_warnings,
                                                                 errors=errors,
                                                                 logger=logger)
            finished: datetime = datetime.now(tz=TZ_LOCAL)
            duration: str = timestamp_duration(start=started,
                                               finish=finished)
            op_report.update({
                "total-plain-deletes": counts[0],
                "total-plain-inserts": counts[1],
                "total-plain-updates": counts[2],
                "total-plain-duration": duration
            })
            logger.info(msg=f"Finished {op} the plain data")

    # update the migration and session instances
    if not errors:
        migration_works: list[MigrationWork] = migration.get_migration_works(refresh=True,
                                                                             db_engine=PYDB_DB_ENGINE,
                                                                             errors=errors)
        if not errors:
            is_finished: bool = True
            for migration_work in migration_works:
                if migration_work.ts_finish is None:
                    is_finished = False
                    break
            if is_finished:
                migration.ts_finish = datetime.now(tz=TZ_LOCAL)
                migration.update(db_engine=PYDB_DB_ENGINE,
                                 errors=errors)
                if not errors:
                    session: Session = Session.get_instance([Migration],
                                                            where_data={Session.Db.ID: migration.id_session},
                                                            db_engine=PYDB_DB_ENGINE,
                                                            errors=errors)
                    if not errors:
                        migrations: list[Migration] = session.get_migrations(db_engine=PYDB_DB_ENGINE)
                        for mig in migrations or []:
                            if mig.ts_finish is None:
                                is_finished = False
                                break
                    if is_finished:
                        session.cd_session = SessionState.FINISHED

    migration_finished: datetime = datetime.now(tz=TZ_LOCAL)
    op_report.update({
        "total-tables": len(migrated_tables),
        "migrated-tables": migrated_tables,
        "started": migration_started.strftime(format=DatetimeFormat.INV),
        "finished": migration_finished.strftime(format=DatetimeFormat.INV),
        "duration": timestamp_duration(start=migration_started,
                                       finish=migration_finished)
    })

    try:
        __log_migration(badge=migration.nm_badge,
                        threads=migration_threads,
                        errors=errors,
                        log_json=op_report)
    except Exception as e:
        exc_err: str = str_sanitize(exc_format(exc=e,
                                               exc_info=sys.exc_info()))
        logger.error(msg=exc_err)
        MigrationIssue.new_issue(id_migration=migration.id,
                                 cd_type=IssueType.ERROR,
                                 ds_issue=exc_err)
        # 101: {}
        errors.append(validate_format_error(101,
                                            exc_err))


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

    # sent the files to the S3 storage
