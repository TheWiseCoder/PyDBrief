import json
import os
import sys
import threading
import warnings
from datetime import datetime
from logging import Logger
from pypomes_core import (
    TZ_LOCAL, timestamp_duration, pypomes_versions, dict_jsonify,
    str_is_int, str_splice, str_sanitize,
    validate_format_error, exc_format
)
from pypomes_db import DbEngine, db_connect, db_commit, db_rollback, db_close
from pypomes_logging import logging_get_entries, logging_get_params
from typing import Any, Type

from app_constants import PYDB_DB_ENGINE, InputParam, MigIncremental
from app_ident import get_env_keys
from entities.migration import Migration, MigStep
from entities.migration_span import MigrationSpan
from entities.migration_table import MigrationTable
from entities.migration_work import MigrationWork
from entities.database import Database
from entities.session import Session, SessionState
from entities.s3 import S3
from migration.pydb_types_old import name_to_type
from migration.steps.pydb_migrate_lobdata import migrate_lobdata
from migration.steps.pydb_migrate_metadata import migrate_metadata
from migration.steps.pydb_migrate_plaindata import migrate_plaindata


def migrate(migration: Migration,
            session: Session,
            app_name: str,
            app_version: str,
            base_url: str,
            requester: str,
            logger: Logger) -> None:

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

    # proceed, if migration of plain data and/or LOB data has been indicated
    if (not errors and migrated_tables and
        migration.cd_step in [MigStep.MIGRATE_PLAINDATA, MigStep.MIGRATE_LOBDATA, MigStep.CORRELATE_PLAINDATA,
                              MigStep.CORRELATE_LOBDATA, MigStep.SYNCHRONIZE_PLAINDATA]):

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


def process_override_columns(override_columns: list[str],
                             db_engine: DbEngine | str,
                             errors: list[str]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the override columns list
    try:
        for override_column in override_columns:
            # format of 'override_column' is <table_name>.<column_name>=<column_type>
            column_name: str = override_column[:int(f"{override_column.rindex('=')}")].lower()
            type_name: str = override_column.replace(column_name, "", 1)[1:].lower()
            column_type: Type = name_to_type(type_name=type_name,
                                             db_engine=db_engine)
            if column_name and column_type:
                result[column_name] = column_type
            else:
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(142,
                                                    type_name,
                                                    f"not a valid column type for dtabase engine {db_engine}"))
    except Exception as e:
        exc_err: str = str_sanitize(exc_format(exc=e,
                                               exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            f"@{InputParam.OVERRIDE_COLUMNS}"))
    return result


def process_incremental_migrations(incremental_migrations: list[str],
                                   def_size: int) -> dict[str, dict[MigIncremental, int]]:

    # initialize the return variable
    result: dict[str, dict[MigIncremental, int]] = {}

    # format of 'incremental_migrations' is [<table-name>[=<size>[:<offset>],...]
    for incremental_table in incremental_migrations:
        if ":" not in incremental_table:
            incremental_table += ":"
        # noinspection PyTypeChecker
        terms: tuple[str, str, str] = str_splice(incremental_table,
                                                 seps=["=", ":"])
        size: int = int(terms[1]) if str_is_int(terms[1]) else def_size
        offset: int = int(terms[2]) if str_is_int(terms[2]) else 0
        result[terms[0]] = {
            MigIncremental.COUNT: size,
            MigIncremental.OFFSET: offset
        }

    return result
