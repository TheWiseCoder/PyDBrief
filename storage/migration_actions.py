from typing import Any
from pypomes_core import (
    DatetimeFormat, validate_format_error,
    validate_bool, validate_int, validate_enum, validate_str, validate_strs
)
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.migration import (
    Migration, MigStep,
    SPAN_LOBDATA_CHANNELS, SPAN_LOBDATA_CHANNEL_SIZE,
    SPAN_PLAINDATA_CHANNELS, SPAN_PLAINDATA_CHANNEL_SIZE
)
from entities.database import Database
from entities.migration_issue import MigrationIssue
from entities.migration_report import MigrationReport
from entities.migration_table import MigrationTable
from entities.migration_span import MigrationSpan
from entities.migration_work import MigrationWork
from entities.s3 import S3
from entities.session import Session


def create_migration(input_params: dict[str, Any],
                     errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[i[0] for i in Migration.ATTRS_INPUT],
                                                            op=OpType.CREATE,
                                                            db_conn=None,
                                                            errors=errors)
        if not errors:
            # create and persist the migration
            migration: Migration = Migration()
            migration.id_session = migration_params.pop(InputParam.SESSION).id
            migration.set(migration_params)
            migration.insert(db_engine=PYDB_DB_ENGINE,
                             db_conn=db_conn,
                             errors=errors)

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


def update_migration(input_params: dict[str, Any],
                     errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = [InputParam.MIGRATION_ID] + [i[0] for i in Migration.ATTRS_INPUT]
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=valid_params,
                                                            op=OpType.UPDATE,
                                                            db_conn=None,
                                                            errors=errors)
        if not errors:
            migration: Migration = migration_params.pop(InputParam.MIGRATION)
            if InputParam.SESSION in migration_params:
                migration.id_session = migration_params.pop(InputParam.SESSION).id
            migration.set(data=migration_params)
            migration.update(db_engine=PYDB_DB_ENGINE,
                             db_conn=db_conn,
                             errors=errors)

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


def delete_migration(input_params: dict[str, Any],
                     errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[InputParam.MIGRATION_ID],
                                                            op=OpType.DELETE,
                                                            db_conn=db_conn,
                                                            errors=errors)
        if not errors:
            # obtain and delete the migration
            migration: Migration = migration_params[InputParam.MIGRATION]
            migration.delete(db_engine=PYDB_DB_ENGINE,
                             db_conn=db_conn,
                             errors=errors)

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


def retrieve_migrations(input_params: dict[str, Any],
                        errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = [InputParam.BADGE, InputParam.SESSION]
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=valid_params,
                                                            op=OpType.RETRIEVE,
                                                            db_conn=db_conn,
                                                            errors=errors)
        if not errors:
            where_data: dict[str, Any] | None = None
            if Migration.Db.NM_BADGE in migration_params:
                where_data = {Migration.Db.NM_BADGE: migration_params[Migration.Db.NM_BADGE]}
            elif InputParam.SESSION in migration_params:
                where_data = {Migration.Db.ID_SESSION: migration_params[InputParam.SESSION].id}

            if where_data:
                migrations: list[Migration] = Migration.get_instances(where_data=where_data,
                                                                      db_engine=PYDB_DB_ENGINE,
                                                                      db_conn=db_conn,
                                                                      errors=errors)
                for migration in migrations or []:
                    mig_data: dict[str, Any] = migration.get_inputs()
                    values: list[int] = Session.get_values(attrs=Session.Db.CD_SESSION,
                                                           where_data={Session.Db.ID: migration.id_session},
                                                           max_count=1,
                                                           min_count=1,
                                                           db_engine=PYDB_DB_ENGINE,
                                                           db_conn=db_conn,
                                                           errors=errors)
                    if errors:
                        break
                    mig_data[InputParam.SESSION] = values[0]

                    mig_issues: list[dict[str, Any]] = []
                    migration_issues: list[MigrationIssue] = \
                        migration.get_migration_issues(db_engine=PYDB_DB_ENGINE,
                                                       db_conn=db_conn,
                                                       errors=errors)
                    if errors:
                        break
                    for migration_issue in migration_issues:
                        mig_issues.append({InputParam.TYPE: migration_issue.cd_type,
                                           InputParam.DESCRIPTION: migration_issue.ds_issue,
                                           InputParam.ONSET: migration_issue.ts_onset.strftime(DatetimeFormat.LATIN)})
                    mig_data[InputParam.ISSUES] = mig_issues

                    mig_reports: list[dict[str, Any]] = []
                    migration_reports: list[MigrationReport] = \
                        migration.get_migration_reports(db_engine=PYDB_DB_ENGINE,
                                                        db_conn=db_conn,
                                                        errors=errors)
                    if errors:
                        break
                    for migration_report in migration_reports:
                        mig_reports.append(
                            {InputParam.PATH: migration_report.ds_path,
                             InputParam.CREATION: migration_report.ts_creation.strftime(DatetimeFormat.LATIN)})
                    mig_data[InputParam.REPORTS] = mig_reports

                    mig_tables: list[dict[str, Any]] = []
                    migration_tables: list[MigrationTable] = migration.get_migration_tables(db_engine=PYDB_DB_ENGINE,
                                                                                            db_conn=db_conn,
                                                                                            errors=errors)
                    if errors:
                        break
                    for migration_table in migration_tables:
                        mig_table: dict[str, Any] = migration_table.get_inputs()
                        mig_tables.append(mig_table)

                    if errors:
                        break
                    mig_data[InputParam.CUSTOM_TABLES] = mig_tables

                    mig_works: list[dict[str, Any]] = []
                    migration_works: list[MigrationWork] = migration.get_migration_works(db_engine=PYDB_DB_ENGINE,
                                                                                         db_conn=db_conn,
                                                                                         errors=errors)
                    if errors:
                        break
                    for migration_work in migration_works:
                        mig_work: dict[str, Any] = {InputParam.NAME: migration_work.nm_table}
                        if migration_work.ts_start:
                            mig_work[InputParam.START] = \
                                migration_work.ts_start.strftime(format=DatetimeFormat.LATIN)
                        if migration_work.ts_finish:
                            mig_work[InputParam.FINISH] = \
                                migration_work.ts_finish.strftime(format=DatetimeFormat.LATIN)

                        mig_spans: list[dict[str, Any]] = []
                        migration_spans: list[MigrationSpan] = \
                            migration_work.get_migration_spans(db_engine=PYDB_DB_ENGINE,
                                                               db_conn=db_conn,
                                                               errors=errors)
                        if errors:
                            break
                        for migration_span in migration_spans:
                            mig_spans.append({InputParam.FIRST_ROW: migration_span.nr_first_row,
                                              InputParam.LAST_ROW: migration_span.nr_last_row,
                                              InputParam.DONE: migration_span.is_done})
                        mig_work[InputParam.SPANS] = mig_spans
                        mig_tables.append(mig_work)
                    if errors:
                        break
                    mig_data[InputParam.WORK_TABLES] = mig_works

                    result[migration.nm_badge] = mig_data
            else:
                # 100: {} (omits the attribute "code")
                errors.append(validate_format_error(100,
                                                    "Either 'BADGE' or 'SESSION' must be specified"))
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


def verify_migration(input_params: dict[str, Any],
                     errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[InputParam.CD_BADGE],
                                                            op=OpType.VERIFY,
                                                            db_conn=db_conn,
                                                            errors=errors)
        if not errors:
            migration: Migration = Migration(nm_badge=migration_params.get(Migration.Db.NM_BADGE),
                                             db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)
            if not errors:
                session: Session = Session(migration.id_session,
                                           db_engine=PYDB_DB_ENGINE,
                                           db_conn=db_conn,
                                           errors=errors)
                if not errors:
                    database: Database = session.get_source_db(db_engine=PYDB_DB_ENGINE,
                                                               db_conn=db_conn,
                                                               errors=errors)
                    if not errors:
                        conn: Any = db_connect(engine=database.cd_engine,
                                               errors=errors)
                        if not errors:
                            db_close(conn,
                                     engine=PYDB_DB_ENGINE)
                            database = session.get_target_db(db_engine=PYDB_DB_ENGINE,
                                                             db_conn=db_conn,
                                                             errors=errors)
                            if not errors:
                                conn: Any = db_connect(engine=database.cd_engine,
                                                       errors=errors)
                                if not errors:
                                    db_close(conn,
                                             engine=PYDB_DB_ENGINE)
                                _s3: S3 = session.get_target_s3(db_engine=PYDB_DB_ENGINE,
                                                                db_conn=db_conn,
                                                                errors=errors)


def __validate_input(input_params: dict[str, Any],
                     valid_params: list[str],
                     op: OpType,
                     db_conn: Any,
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    # identify the migration instance (UPDATE and DELETE operations)
    migration_id: str = validate_str(source=input_params,
                                     attr=InputParam.MIGRATION_ID,
                                     required=op in [OpType.UPDATE, OpType.DELETE],
                                     errors=errors)
    if migration_id:
        result[InputParam.MIGRATION] = Migration(nm_badge=migration_id,
                                                 db_engine=PYDB_DB_ENGINE,
                                                 db_conn=db_conn,
                                                 errors=errors)

    # identify the session instance (CREATE and UPDATE operations)
    cd_session: str = validate_str(source=input_params,
                                   attr=InputParam.SESSION,
                                   max_length=64,
                                   required=op == OpType.CREATE,
                                   errors=errors)
    if cd_session:
        result[InputParam.SESSION] = Session(cd_session=cd_session,
                                             db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)

    nm_badge: str = validate_str(source=input_params,
                                 attr=InputParam.BADGE,
                                 max_length=64,
                                 required=op == OpType.CREATE,
                                 errors=errors)
    if nm_badge:
        result[Migration.Db.NM_BADGE] = nm_badge

    cd_step: MigStep = validate_enum(source=input_params,
                                     attr=InputParam.STEP,
                                     enum_class=MigStep,
                                     required=op == OpType.CREATE,
                                     errors=errors)
    if cd_step:
        result[Migration.Db.CD_STEP] = cd_step

    is_flatten_storage: bool = validate_bool(source=input_params,
                                             attr=InputParam.FLATTEN_STORAGE,
                                             errors=errors)
    if isinstance(is_flatten_storage, bool):
        result[Migration.Db.IS_FLATTEN_STORAGE] = is_flatten_storage

    is_optimize_pks: bool = validate_bool(source=input_params,
                                          attr=InputParam.OPTIMIZE_PKS,
                                          errors=errors)
    if isinstance(is_optimize_pks, bool):
        result[Migration.Db.IS_OPTIMIZE_PKS] = is_optimize_pks

    is_process_indexes: bool = validate_bool(source=input_params,
                                             attr=InputParam.PROCESS_INDEXES,
                                             errors=errors)
    if isinstance(is_process_indexes, bool):
        result[Migration.Db.IS_PROCESS_INDEXES] = is_process_indexes

    is_process_views: bool = validate_bool(source=input_params,
                                           attr=InputParam.PROCESS_VIEWS,
                                           errors=errors)
    if isinstance(is_process_views, bool):
        result[Migration.Db.IS_PROCESS_VIEWS] = is_process_views

    is_reflect_filetype: bool = validate_bool(source=input_params,
                                              attr=InputParam.REFLECT_FILETYPE,
                                              errors=errors)
    if isinstance(is_reflect_filetype, bool):
        result[Migration.Db.IS_REFLECT_FILETYPE] = is_reflect_filetype

    is_relax_reflection: bool = validate_bool(source=input_params,
                                              attr=InputParam.RELAX_REFLECTION,
                                              errors=errors)
    if isinstance(is_relax_reflection, bool):
        result[Migration.Db.IS_RELAX_REFLECTION] = is_relax_reflection

    is_skip_nonempty: bool = validate_bool(source=input_params,
                                           attr=InputParam.SKIP_NONEMPTY,
                                           errors=errors)
    if isinstance(is_skip_nonempty, bool):
        result[Migration.Db.IS_SKIP_NONEMPTY] = is_skip_nonempty

    nr_lobdata_channels: int = validate_int(source=input_params,
                                            attr=InputParam.LOBDATA_CHANNELS,
                                            min_val=SPAN_LOBDATA_CHANNELS[0],
                                            max_val=SPAN_LOBDATA_CHANNELS[1],
                                            errors=errors)
    if nr_lobdata_channels:
        result[Migration.Db.NR_LOBDATA_CHANNELS] = nr_lobdata_channels

    nr_lobdata_channel_size: int = validate_int(source=input_params,
                                                attr=InputParam.LOBDATA_CHANNEL_SIZE,
                                                min_val=SPAN_LOBDATA_CHANNEL_SIZE[0],
                                                max_val=SPAN_LOBDATA_CHANNEL_SIZE[1],
                                                errors=errors)
    if nr_lobdata_channel_size:
        result[Migration.Db.NR_LOBDATA_CHANNEL_SIZE] = nr_lobdata_channel_size

    nr_plaindata_channels: int = validate_int(source=input_params,
                                              attr=InputParam.PLAINDATA_CHANNELS,
                                              min_val=SPAN_PLAINDATA_CHANNELS[0],
                                              max_val=SPAN_PLAINDATA_CHANNELS[1],
                                              errors=errors)
    if nr_plaindata_channels:
        result[Migration.Db.NR_PLAINDATA_CHANNELS] = nr_plaindata_channels

    nr_plaindata_channel_size: int = validate_int(source=input_params,
                                                  attr=InputParam.PLAINDATA_CHANNEL_SIZE,
                                                  min_val=SPAN_PLAINDATA_CHANNEL_SIZE[0],
                                                  max_val=SPAN_PLAINDATA_CHANNEL_SIZE[1],
                                                  errors=errors)
    if nr_plaindata_channel_size:
        result[Migration.Db.NR_PLAINDATA_CHANNEL_SIZE] = nr_plaindata_channel_size

    exclude_relations: list[str] = validate_strs(source=input_params,
                                                 attr=InputParam.EXCLUDE_RELATIONS,
                                                 errors=errors)
    if exclude_relations:
        result[Migration.Db.DS_EXCLUDE_RELATIONS] = (",".join([i for i in exclude_relations])).lower()

    include_relations: list[str] = validate_strs(source=input_params,
                                                 attr=InputParam.INCLUDE_RELATIONS,
                                                 errors=errors)
    if include_relations:
        result[Migration.Db.DS_INCLUDE_RELATIONS] = (",".join([i for i in include_relations])).lower()

    return result
