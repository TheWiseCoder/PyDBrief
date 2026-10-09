import json
import os
import sys
import threading
import warnings
from datetime import datetime
from io import BytesIO
from logging import Logger
from pypomes_core import (
    TZ_LOCAL, DatetimeFormat, Mimetype,
    timestamp_duration, env_is_docker, pypomes_versions,
    dict_jsonify, str_sanitize, exc_format
)
from pypomes_logging import logging_get_entries, logging_get_params
from pypomes_s3 import s3_get_client, s3_file_store, s3_item_exists
from pathlib import Path
from typing import Any
from urlobject import URLObject

from app_constants import (
    REGISTRY_DOCKER, REGISTRY_HOST,
    PYDB_DB_ENGINE, PYDB_S3_ENGINE, PYDB_S3_BASE_FOLDER, InputParam, MigState, MigStep
)
from app_ident import get_env_keys
from entities.database import Database
from entities.migration import Migration, minded_migrations
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_report import MigrationReport
from entities.migration_table import MigrationTable
from entities.session import Session
from migration.steps.pydb_correlate_lobdata import correlate_lobdata
from migration.steps.pydb_migrate_lobdata import migrate_lobdata
from migration.steps.pydb_migrate_metadata import migrate_metadata
from migration.steps.pydb_migrate_plaindata import migrate_plaindata
from migration.steps.pydb_sync_plaindata import synchronize_plaindata


def migrate(migration: Migration,
            session: Session,
            mig_step: MigStep,
            app_name: str,
            app_version: str,
            base_url: str,
            requester: str,
            logger: Logger) -> None:

    # time the migration start
    migration_started: datetime = datetime.now(tz=TZ_LOCAL)

    # initialize the errors list
    errors: list[str] = []

    # establish the migration state
    mig_key: str = f"{mig_step}-{migration.id}"
    minded_migrations[mig_key] = MigState.MIGRATING

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
        InputParam.STEP: mig_step.anyval,
        InputParam.SESSION: session.get_inputs(),
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

    if not migration.ts_start:
        migration.ts_start = migration_started
        migration.update(db_engine=PYDB_DB_ENGINE)

    # log the migration start
    logger.info(msg=json.dumps(obj=dict_jsonify(op_report),
                               ensure_ascii=False))
    logger.info(msg="Started discovering the metadata")
    migrated_tables: dict[str, Any] = migrate_metadata(migration=migration,
                                                       session=session,
                                                       mig_step=mig_step,
                                                       migration_warnings=migration_warnings,
                                                       errors=errors,
                                                       logger=logger) or {}
    logger.info(msg="Finished discovering the metadata")
    effected_tables: list[str] = migrated_tables.pop("effected-tables", [])

    # initialize the thread registration
    migration_threads: list[int] = [threading.get_ident()]

    # proceed, if migration/synchronization/correlation has been indicated
    if not errors and migrated_tables and mig_step != MigStep.MIGRATE_METADATA:

        # migrate the plain data
        if mig_step == MigStep.MIGRATE_PLAINDATA:
            logger.info("Started migrating the plain data")
            started: datetime = datetime.now(tz=TZ_LOCAL)
            count: int = migrate_plaindata(migration=migration,
                                           session=session,
                                           mig_step=mig_step,
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
        if not errors and mig_step == MigStep.MIGRATE_LOBDATA:
            logger.info("Started migrating the LOBs")

            # ignore warnings from 'boto3' and 'minio' packages
            # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
            if session.id_target_s3:
                warnings.filterwarnings(action="ignore")

            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int] = migrate_lobdata(migration=migration,
                                                      session=session,
                                                      mig_step=mig_step,
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
        if not errors and mig_step == MigStep.CORRELATE_LOBDATA:
            logger.info(msg="Started correlating the LOBs")

            # ignore warnings from 'boto3' and 'minio' packages
            # (they generate the warning "datetime.datetime.utcnow() is deprecated...")
            warnings.filterwarnings(action="ignore")

            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int, int] = correlate_lobdata(migration=migration,
                                                             session=session,
                                                             mig_step=mig_step,
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
        if not errors and mig_step in [MigStep.CORRELATE_PLAINDATA, MigStep.SYNCHRONIZE_PLAINDATA]:
            op: str = "correlating" if mig_step == MigStep.CORRELATE_PLAINDATA else "synchronizing"
            logger.info(msg=f"Started {op} the plain data")
            started: datetime = datetime.now(tz=TZ_LOCAL)
            counts: tuple[int, int, int] = synchronize_plaindata(migration=migration,
                                                                 session=session,
                                                                 mig_step=mig_step,
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

    # update the 'Migration' and 'Session' instances
    curr_errors: list[str] = []
    if not errors:
        migration.ts_finish = datetime.now(tz=TZ_LOCAL)
        migration.update(db_engine=PYDB_DB_ENGINE,
                         errors=curr_errors)
        errors.extend(curr_errors)
    if not errors:
        session.update(db_engine=PYDB_DB_ENGINE,
                       errors=curr_errors)
        errors.extend(curr_errors)
    MigrationIssue.new_issues(id_migration=migration.id,
                              cd_step=mig_step,
                              cd_type=IssueType.ERROR,
                              ds_issues=errors)

    # establish the migration state
    mig_key: str = f"{mig_step}-{migration.id}"
    if errors:
        minded_migrations[mig_key] = MigState.ERROR
    elif minded_migrations.get(mig_key) != MigState.ABORTING:
        minded_migrations[mig_key] = MigState.MIGRATED
    else:
        minded_migrations[mig_key] = MigState.ABORTED

    migration_finished: datetime = datetime.now(tz=TZ_LOCAL)
    op_report.update({
        "started": migration_started.strftime(format=DatetimeFormat.INV),
        "finished": migration_finished.strftime(format=DatetimeFormat.INV),
        "duration": timestamp_duration(start=migration_started,
                                       finish=migration_finished)
    })

    # prune the migrated tables list
    display_tables: dict[str, Any] = {k: v for k, v in migrated_tables.items() if k in effected_tables} \
        if mig_step == MigStep.MIGRATE_METADATA else migrated_tables
    op_report["total-tables"] = len(display_tables)

    # prune the display
    for k, v in display_tables.items():
        if mig_step != MigStep.MIGRATE_METADATA:
            v.pop("columns")
        if mig_step not in [MigStep.MIGRATE_LOBDATA, MigStep.CORRELATE_LOBDATA]:
            v.pop("lob-count", None)
            v.pop("lob-duration", None)
            v.pop("lob-status", None)
            v.pop("lob-bytes", None)
        if mig_step not in [MigStep.MIGRATE_PLAINDATA, MigStep.SYNCHRONIZE_PLAINDATA]:
            v.pop("plain-count", None)
            v.pop("plain-duration", None)
            v.pop("plain-status", None)
    op_report["migrated-tables"] = display_tables

    try:
        __log_migration(migration=migration,
                        session=session,
                        mig_step=mig_step,
                        threads=migration_threads,
                        log_json=op_report,
                        errors=errors,
                        logger=logger)
    except Exception as e:
        exc_err: str = str_sanitize(exc_format(exc=e,
                                               exc_info=sys.exc_info()))
        logger.error(msg=exc_err)
        MigrationIssue.new_issue(id_migration=migration.id,
                                 cd_step=mig_step,
                                 cd_type=IssueType.ERROR,
                                 ds_issue=exc_err)


# 'errors' contains the errors incident upon the migration activity, if any
def __log_migration(migration: Migration,
                    session: Session,
                    mig_step: MigStep,
                    threads: list[int],
                    log_json: dict[str, Any],
                    errors: list[str],
                    logger: Logger) -> None:

    # define the needed data
    database: Database = session.get_target_db()
    nm_badge: str = migration.nm_badge.replace("-", "/")
    pos: int = nm_badge.rfind("/")
    badge_path: Path = Path(nm_badge[:pos])
    badge_name: str = f"{nm_badge[pos+1:]}_{mig_step.lower()}"
    base_path: Path = Path(REGISTRY_DOCKER if REGISTRY_DOCKER and env_is_docker() else REGISTRY_HOST,
                           badge_path)

    # obtain the S3 client (errors will be added to JSON report)
    s3_client: Any = s3_get_client(engine=PYDB_S3_ENGINE,
                                   errors=errors) if PYDB_S3_ENGINE and PYDB_S3_BASE_FOLDER else None

    # create intermediate missing folders in host filesystem
    log_file: Path = Path(base_path,
                          f"{badge_name}.log")
    log_file.parent.mkdir(parents=True,
                          exist_ok=True)

    # establish the version, to avoid overwriting existing documents (errors will be added to JSON report)
    seq: int = __establish_version(s3_client=s3_client,
                                   database=database,
                                   base_path=base_path,
                                   badge_path=badge_path,
                                   badge_name=badge_name,
                                   errors=errors)

    # write the log file to the host filesystem
    log_file: Path = Path(base_path,
                          f"{badge_name}_{seq}.log")
    log_content: bytes = b""
    log_entries: BytesIO = logging_get_entries(log_threads=list(map(str, set(threads))),
                                               errors=errors)
    if log_entries:
        log_entries.seek(0)
        log_content = log_entries.getvalue()
    with log_file.open("wb") as f:
        f.write(log_content)

    # write the JSON file to the host filesystem
    if errors:
        log_json = log_json.copy()
        log_json["errors"] = errors
    json_data: str = json.dumps(obj=log_json,
                                ensure_ascii=False,
                                indent=2)
    json_file: Path = Path(base_path,
                           f"{badge_name}_{seq}.json")
    with json_file.open("w") as f:
        f.write(json_data)
    errors.clear()

    # send the files to the S3 storage
    if s3_client:
        url: URLObject = URLObject(database.nm_host)
        # 'url.hostname' returns 'None' for 'localhost'
        host: str = f"{database.cd_type}@{url.hostname or str(url)}"
        s3_prefix: Path = Path(host,
                               PYDB_S3_BASE_FOLDER,
                               badge_path)
        s3_file_store(identifier=log_file.name,
                      filepath=log_file,
                      mimetype=Mimetype.TEXT,
                      prefix=s3_prefix,
                      engine=PYDB_S3_ENGINE,
                      client=s3_client,
                      errors=errors)
        if not errors:
            # HAZARD: 'ds_path' is a UNIQUE attribute
            ds_path: str = Path(s3_prefix,
                                log_file.name).as_posix()
            # uncondionally delete entry, ignoring errors
            MigrationReport.erase(where_data={MigrationReport.Db.DS_PATH: ds_path},
                                  db_engine=PYDB_DB_ENGINE,
                                  errors=errors)
            mig_report: MigrationReport = MigrationReport(db_engine=PYDB_DB_ENGINE)
            mig_report.id_migration = migration.id
            mig_report.cd_step = mig_step
            mig_report.ds_path = ds_path
            if (mig_report.insert(db_engine=PYDB_DB_ENGINE,
                                  errors=errors) and
                s3_file_store(identifier=json_file.name,
                              filepath=json_file,
                              mimetype=Mimetype.JSON,
                              prefix=s3_prefix,
                              engine=PYDB_S3_ENGINE,
                              client=s3_client,
                              errors=errors)):
                # HAZARD: 'ds_path' is a UNIQUE attribute
                ds_path: str = Path(s3_prefix,
                                    json_file.name).as_posix()
                # uncondionally delete entry (errors are logged)
                MigrationReport.erase(where_data={MigrationReport.Db.DS_PATH: ds_path},
                                      db_engine=PYDB_DB_ENGINE,
                                      errors=errors)
                mig_report: MigrationReport = MigrationReport(db_engine=PYDB_DB_ENGINE)
                mig_report.id_migration = migration.id
                mig_report.cd_step = mig_step
                mig_report.ds_path = ds_path
                mig_report.insert(db_engine=PYDB_DB_ENGINE,
                                  errors=errors)
        for error in errors:
            MigrationIssue.new_issue(id_migration=migration.id,
                                     cd_step=mig_step,
                                     cd_type=IssueType.ERROR,
                                     ds_issue=error)
            logger.error(errors)


def __establish_version(s3_client: Any,
                        database: Database,
                        base_path: Path,
                        badge_path: Path,
                        badge_name: str,
                        errors: list[str]) -> int:

    # initialize the return variable
    result: int = 1

    log_file: Path = Path(base_path,
                          f"{badge_name}_1.log")
    if s3_client:
        # S3 storage has precedence
        url: URLObject = URLObject(database.nm_host)
        # 'url.hostname' returns 'None' for 'localhost'
        host: str = f"{database.cd_type}@{url.hostname or str(url)}"
        s3_prefix: Path = Path(host,
                               PYDB_S3_BASE_FOLDER,
                               badge_path)
        curr_errors: list[str] = []
        while s3_item_exists(identifier=log_file.name,
                             prefix=s3_prefix,
                             engine=PYDB_S3_ENGINE,
                             client=s3_client,
                             errors=curr_errors) and not curr_errors:
            result += 1
            log_file = Path(base_path,
                            f"{badge_name}_{result}.log")
        errors.extend(curr_errors)
    else:
        # host filesystem is the alternative
        while log_file.exists():
            result += 1
            log_file = Path(base_path,
                            f"{badge_name}_{result}.log")
    return result
