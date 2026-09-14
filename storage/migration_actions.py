from typing import Any
from pypomes_core import (
    validate_int, validate_str, validate_format_error
)
from pypomes_db import db_connect, db_commit, db_rollback, db_close, db_get_engines

from app_consts import PYDB_DB_ENGINE, InputParam, OpType
from entities.migration import (
    Migration,
    SPAN_BATCH_SIZE_IN, SPAN_BATCH_SIZE_OUT,
    SPAN_CHUNK_SIZE, SPAN_INCREMENTAL_SIZE,
    SPAN_LOBDATA_CHANNELS, SPAN_LOBDATA_CHANNEL_SIZE,
    SPAN_PLAINDATA_CHANNELS, SPAN_PLAINDATA_CHANNEL_SIZE
)
from entities.session import Session
from entities.migration_span import MigrationSpan
from entities.migration_table import MigrationTable


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
            miration: Migration = Migration()
            miration.set(migration_params)
            miration.insert(db_engine=PYDB_DB_ENGINE,
                            db_conn=db_conn,
                            errors=errors)

        # conclude the operation
        if errors:
            db_rollback(connection=db_conn)
        else:
            db_commit(connection=db_conn,
                      errors=errors)
        db_close(connection=db_conn)


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
                                             db_conn=db_conn,
                                             errors=errors)
            if not errors:
                migration.set(data=migration_params)
                migration.update(db_conn=db_conn,
                                 errors=errors)

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def delete_migration(input_params: dict[str, Any],
                     errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=[InputParam.BADGE],
                                                            op=OpType.CREATE,
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
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


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
            elif Migration.Db.ID_SESSION:
                where_data = {Migration.Db.ID_SESSION: migration_params.get(Migration.Db.ID_SESSION)}

            if where_data:
                migrations: list[Migration] = Migration.retrieve(where_data=where_data,
                                                                 db_engine=PYDB_DB_ENGINE,
                                                                 db_conn=db_conn,
                                                                 errors=errors)
                for migration in migrations or []:
                    migration_data: dict[str, Any] = migration.get_inputs()
                    values: list[int] = Session.get_values(attrs=Session.Db.CD_SESSION,
                                                           where_data={Session.Db.ID: migration.id_session},
                                                           max_count=1,
                                                           min_count=1,
                                                           db_engine=PYDB_DB_ENGINE,
                                                           db_conn=db_conn,
                                                           errors=errors)
                    if errors:
                        break
                    migration_data[InputParam.SESSION] = values[0]

                    migration_tables: list[MigrationTable] = migration.get_all_tables(db_engine=PYDB_DB_ENGINE,
                                                                                      db_conn=db_conn,
                                                                                      errors=errors)
                    if errors:
                        break

                    result[migration.nm_badge] = migration_data
            else:
                # 100: {} (omits the attribute "code")
                errors.append(validate_format_error(100,
                                                    "Either 'BADGE' or 'SESSION' must be specified"))

        # conclude the operation
        if errors:
            db_rollback(connection=db_conn)
        else:
            db_commit(connection=db_conn,
                      errors=errors)
        db_close(connection=db_conn)

    return result


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
                                         attr=InputParam.MIGRATION_BADGE,
                                         min_val=SPAN_BATCH_SIZE_IN[0],
                                         max_val=SPAN_BATCH_SIZE_IN[2],
                                         required=op == OpType.CREATE,
                                         errors=errors)
    if nr_batch_size_in:
        result[Migration.Db.NR_BATCH_SIZE_IN] = nr_batch_size_in

    nr_batch_size_out: int = validate_int(source=input_params,
                                          attr=InputParam.MIGRATION_BADGE,
                                          min_val=SPAN_BATCH_SIZE_OUT[0],
                                          max_val=SPAN_BATCH_SIZE_OUT[2],
                                          required=op == OpType.CREATE,
                                          errors=errors)
    if nr_batch_size_out:
        result[Migration.Db.NR_BATCH_SIZE_OUT] = nr_batch_size_out

    nr_chunk_size: int = validate_int(source=input_params,
                                      attr=InputParam.MIGRATION_BADGE,
                                      min_val=SPAN_CHUNK_SIZE[0],
                                      max_val=SPAN_CHUNK_SIZE[2],
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if nr_chunk_size:
        result[Migration.Db.NR_CHUNK_SIZE] = nr_chunk_size

    nr_incremental_size: int = validate_int(source=input_params,
                                            attr=InputParam.MIGRATION_BADGE,
                                            min_val=SPAN_INCREMENTAL_SIZE[0],
                                            max_val=SPAN_INCREMENTAL_SIZE[2],
                                            required=op == OpType.CREATE,
                                            errors=errors)
    if nr_incremental_size:
        result[Migration.Db.NR_INCREMENTAL_SIZE] = nr_incremental_size

    nr_lobdata_channels: int = validate_int(source=input_params,
                                            attr=InputParam.MIGRATION_BADGE,
                                            min_val=SPAN_LOBDATA_CHANNELS[0],
                                            max_val=SPAN_LOBDATA_CHANNELS[2],
                                            required=op == OpType.CREATE,
                                            errors=errors)
    if nr_lobdata_channels:
        result[Migration.Db.NR_LOBDATA_CHANNELS] = nr_lobdata_channels

    nr_lobdata_channel_size: int = validate_int(source=input_params,
                                                attr=InputParam.MIGRATION_BADGE,
                                                min_val=SPAN_LOBDATA_CHANNEL_SIZE[0],
                                                max_val=SPAN_LOBDATA_CHANNEL_SIZE[2],
                                                required=op == OpType.CREATE,
                                                errors=errors)
    if nr_lobdata_channel_size:
        result[Migration.Db.NR_LOBDATA_CHANNEL_SIZE] = nr_lobdata_channel_size

    nr_plaindata_channels: int = validate_int(source=input_params,
                                              attr=InputParam.MIGRATION_BADGE,
                                              min_val=SPAN_PLAINDATA_CHANNELS[0],
                                              max_val=SPAN_PLAINDATA_CHANNELS[2],
                                              required=op == OpType.CREATE,
                                              errors=errors)
    if nr_plaindata_channels:
        result[Migration.Db.NR_LOBDATA_CHANNELS] = nr_plaindata_channels

    nr_plaindata_channel_size: int = validate_int(source=input_params,
                                                  attr=InputParam.MIGRATION_BADGE,
                                                  min_val=SPAN_PLAINDATA_CHANNEL_SIZE[0],
                                                  max_val=SPAN_PLAINDATA_CHANNEL_SIZE[2],
                                                  required=op == OpType.CREATE,
                                                  errors=errors)
    if nr_plaindata_channel_size:
        result[Migration.Db.NR_PLAINDATA_CHANNEL_SIZE] = nr_plaindata_channel_size

    return result
