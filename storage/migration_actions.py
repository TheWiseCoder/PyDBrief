from typing import Any
from pypomes_core import (
    DatetimeFormat, validate_int, validate_str, validate_enum, validate_format_error
)
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.migration import (
    Migration, MigStep,
    SPAN_BATCH_SIZE_IN, SPAN_BATCH_SIZE_OUT,
    SPAN_CHUNK_SIZE, SPAN_INCREMENTAL_SIZE,
    SPAN_LOBDATA_CHANNELS, SPAN_LOBDATA_CHANNEL_SIZE,
    SPAN_PLAINDATA_CHANNELS, SPAN_PLAINDATA_CHANNEL_SIZE
)
from entities.database import Database
from entities.migration_issue import MigrationIssue
from entities.migration_span import MigrationSpan
from entities.migration_spec import MigrationSpec
from entities.migration_table import MigrationTable
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
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[i[0] for i in Migration.ATTRS_INPUT],
                                                            op=OpType.UPDATE,
                                                            db_conn=None,
                                                            errors=errors)
        if not errors:
            migration: Migration = Migration(nm_badge=migration_params.get(Migration.Db.NM_BADGE),
                                             db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)
            if not errors:
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
                                                            valid_params=[InputParam.BADGE],
                                                            op=OpType.DELETE,
                                                            db_conn=db_conn,
                                                            errors=errors)
        if not errors:
            # obtain and delete the migration
            migration: Migration = Migration(nm_badge=migration_params.get(Migration.Db.NM_BADGE),
                                             db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)
            if not errors:
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
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[InputParam.SESSION, InputParam.BADGE],
                                                            op=OpType.RETRIEVE,
                                                            db_conn=db_conn,
                                                            errors=errors)
        if not errors:
            where_data: dict[str, Any] | None = None
            if Migration.Db.NM_BADGE in migration_params:
                where_data = {Migration.Db.NM_BADGE: migration_params.get(Migration.Db.NM_BADGE)}
            elif Migration.Db.ID_SESSION in migration_params:
                where_data = {Migration.Db.ID_SESSION: migration_params.get(Migration.Db.ID_SESSION)}

            if where_data:
                migrations: list[Migration] = Migration.retrieve(where_data=where_data,
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

                    mig_specs: list[dict[str, Any]] = []
                    migration_specs: list[MigrationSpec] = \
                        migration.get_migration_specs(db_engine=PYDB_DB_ENGINE,
                                                      db_conn=db_conn,
                                                      errors=errors)
                    if errors:
                        break
                    for migration_spec in migration_specs:
                        mig_specs.append({migration_spec.cd_spec: migration_spec.vl_spec})
                    mig_data[InputParam.SPECS] = mig_specs

                    mig_tables: list[dict[str, Any]] = []
                    migration_tables: list[MigrationTable] = migration.get_migration_tables(db_engine=PYDB_DB_ENGINE,
                                                                                            db_conn=db_conn,
                                                                                            errors=errors)
                    if errors:
                        break
                    for migration_table in migration_tables:
                        mig_table: dict[str, Any] = {InputParam.NAME: migration_table.nm_table}
                        if migration_table.ts_start:
                            mig_table[InputParam.START] = \
                                migration_table.ts_start.strftime(format=DatetimeFormat.LATIN)
                        if migration_table.ts_finish:
                            mig_table[InputParam.FINISH] = \
                                migration_table.ts_finish.strftime(format=DatetimeFormat.LATIN)

                        mig_spans: list[dict[str, Any]] = []
                        migration_spans: list[MigrationSpan] = \
                            migration_table.get_migration_spans(db_engine=PYDB_DB_ENGINE,
                                                                db_conn=db_conn,
                                                                errors=errors)
                        if errors:
                            break
                        for migration_span in migration_spans:
                            mig_spans.append({InputParam.FIRST_ROW: migration_span.nr_first_row,
                                              InputParam.LAST_ROW: migration_span.nr_last_row,
                                              InputParam.DONE: migration_span.is_done})
                        mig_table[InputParam.SPANS] = mig_spans
                        mig_tables.append(mig_table)
                    if errors:
                        break
                    mig_data[InputParam.TABLES] = mig_tables

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

    # initialize the return variable
    result: dict[str, Any] = {}

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[InputParam.BADGE],
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

    nm_badge: str = validate_str(source=input_params,
                                 attr=InputParam.BADGE,
                                 max_length=64,
                                 required=op != OpType.RETRIEVE,
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

    cd_session: str = validate_str(source=input_params,
                                   attr=InputParam.SESSION,
                                   max_length=64,
                                   required=op == OpType.CREATE,
                                   errors=errors)
    if cd_session:
        values: list[int] = Session.get_values(attrs=Session.Db.ID,
                                               where_data={Session.Db.CD_SESSION: cd_session},
                                               min_count=1,
                                               max_count=1,
                                               db_engine=PYDB_DB_ENGINE,
                                               db_conn=db_conn)
        if values:
            result[Migration.Db.ID_SESSION] = values[0]

    nr_batch_size_in: int = validate_int(source=input_params,
                                         attr=InputParam.BATCH_SIZE_IN,
                                         min_val=SPAN_BATCH_SIZE_IN[0],
                                         max_val=SPAN_BATCH_SIZE_IN[2],
                                         errors=errors)
    if nr_batch_size_in:
        result[Migration.Db.NR_BATCH_SIZE_IN] = nr_batch_size_in

    nr_batch_size_out: int = validate_int(source=input_params,
                                          attr=InputParam.BATCH_SIZE_OUT,
                                          min_val=SPAN_BATCH_SIZE_OUT[0],
                                          max_val=SPAN_BATCH_SIZE_OUT[2],
                                          errors=errors)
    if nr_batch_size_out:
        result[Migration.Db.NR_BATCH_SIZE_OUT] = nr_batch_size_out

    nr_chunk_size: int = validate_int(source=input_params,
                                      attr=InputParam.CHUNK_SIZE,
                                      min_val=SPAN_CHUNK_SIZE[0],
                                      max_val=SPAN_CHUNK_SIZE[2],
                                      errors=errors)
    if nr_chunk_size:
        result[Migration.Db.NR_CHUNK_SIZE] = nr_chunk_size

    nr_incremental_size: int = validate_int(source=input_params,
                                            attr=InputParam.INCREMENTAL_SIZE,
                                            min_val=SPAN_INCREMENTAL_SIZE[0],
                                            max_val=SPAN_INCREMENTAL_SIZE[2],
                                            errors=errors)
    if nr_incremental_size:
        result[Migration.Db.NR_INCREMENTAL_SIZE] = nr_incremental_size

    nr_lobdata_channels: int = validate_int(source=input_params,
                                            attr=InputParam.LOBDATA_CHANNELS,
                                            min_val=SPAN_LOBDATA_CHANNELS[0],
                                            max_val=SPAN_LOBDATA_CHANNELS[2],
                                            errors=errors)
    if nr_lobdata_channels:
        result[Migration.Db.NR_LOBDATA_CHANNELS] = nr_lobdata_channels

    nr_lobdata_channel_size: int = validate_int(source=input_params,
                                                attr=InputParam.LOBDATA_CHANNEL_SIZE,
                                                min_val=SPAN_LOBDATA_CHANNEL_SIZE[0],
                                                max_val=SPAN_LOBDATA_CHANNEL_SIZE[2],
                                                errors=errors)
    if nr_lobdata_channel_size:
        result[Migration.Db.NR_LOBDATA_CHANNEL_SIZE] = nr_lobdata_channel_size

    nr_plaindata_channels: int = validate_int(source=input_params,
                                              attr=InputParam.PLAINDATA_CHANNELS,
                                              min_val=SPAN_PLAINDATA_CHANNELS[0],
                                              max_val=SPAN_PLAINDATA_CHANNELS[2],
                                              errors=errors)
    if nr_plaindata_channels:
        result[Migration.Db.NR_PLAINDATA_CHANNELS] = nr_plaindata_channels

    nr_plaindata_channel_size: int = validate_int(source=input_params,
                                                  attr=InputParam.PLAINDATA_CHANNEL_SIZE,
                                                  min_val=SPAN_PLAINDATA_CHANNEL_SIZE[0],
                                                  max_val=SPAN_PLAINDATA_CHANNEL_SIZE[2],
                                                  errors=errors)
    if nr_plaindata_channel_size:
        result[Migration.Db.NR_PLAINDATA_CHANNEL_SIZE] = nr_plaindata_channel_size

    return result
