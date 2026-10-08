from typing import Any
from pypomes_core import (
    DatetimeFormat, validate_format_error, validate_str, validate_enum
)
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, MigStep
from entities.migration import Migration
from entities.migration_span import MigrationSpan
from entities.migration_work import MigrationWork


def retrieve_migration_works(input_params: dict[str, Any],
                             errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # validate the input data
    valid_params: list[str] = [InputParam.BADGE, InputParam.TABLE, InputParam.STEP]
    migration_work_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                             valid_params=valid_params,
                                                             errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            where_data: dict[str, Any] = {Migration.Db.NM_BADGE: migration_work_params.get(InputParam.BADGE),
                                          MigrationWork.Db.CD_STEP: migration_work_params.get(InputParam.STEP)}
            if migration_work_params.get(InputParam.TABLE):
                where_data[MigrationWork.Db.NM_TABLE] = migration_work_params.get(InputParam.TABLE)
            migration_works: list[MigrationWork] = MigrationWork.get_instances(
                joins=[(Migration, (Migration.Db.ID, MigrationWork.Db.ID_MIGRATION))],
                where_data=where_data,
                orderby_clause=MigrationWork.Db.NM_TABLE,
                db_engine=PYDB_DB_ENGINE,
                errors=errors
            )
            if migration_works:
                mig_tables: dict[str, Any] = {}
                for migration_work in migration_works:
                    mig_tables[migration_work.nm_table] = {
                        InputParam.START: migration_work.ts_start.strftime(format=DatetimeFormat.LATIN),
                        InputParam.DURATION: migration_work.nr_duration_millis,
                        InputParam.ROW_COUNT:  migration_work.nr_row_count
                    }
                    migration_spans: list[MigrationSpan] = \
                        migration_work.get_migration_spans(db_engine=PYDB_DB_ENGINE,
                                                           db_conn=db_conn,
                                                           errors=errors)
                    if errors:
                        break
                    mig_spans: list[dict[str, Any]] = []
                    for migration_span in migration_spans:
                        mig_spans.append({InputParam.FIRST_ROW: migration_span.nr_first_row,
                                          InputParam.ROW_COUNT: migration_span.nr_row_count,
                                          InputParam.DONE: migration_span.is_done})
                    mig_tables[migration_work.nm_table][InputParam.SPANS] = mig_spans

                if not errors:
                    result = {
                        InputParam.BADGE: migration_work_params.get(InputParam.BADGE),
                        InputParam.STEP: migration_work_params.get(InputParam.STEP).anyval,
                        InputParam.TABLES: mig_tables
                    }

        # conclude the operation
        if errors:
            db_rollback(connection=db_conn,
                        engine=PYDB_DB_ENGINE)
        else:
            db_commit(connection=db_conn,
                      engine=PYDB_DB_ENGINE,
                      errors=errors)
        db_close(connection=db_conn,
                 engine=PYDB_DB_ENGINE)

    return result


def __validate_input(input_params: dict[str, Any],
                     valid_params: list[str],
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    # identify the migration table instance (UPDATE and DELETE operations)
    nm_badge: str = validate_str(source=input_params,
                                 attr=InputParam.BADGE,
                                 required=True,
                                 errors=errors)
    if nm_badge:
        result[InputParam.BADGE] = nm_badge

    mig_step: MigStep = validate_enum(source=input_params,
                                      attr=InputParam.STEP,
                                      enum_class=MigStep,
                                      required=True,
                                      errors=errors)
    if mig_step:
        result[InputParam.STEP] = mig_step

    nm_table: str = validate_str(source=input_params,
                                 attr=InputParam.TABLE,
                                 max_length=64,
                                 errors=errors)
    if nm_table:
        result[InputParam.TABLE] = nm_table

    return result
