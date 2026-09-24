from typing import Any
from pypomes_core import (
    validate_format_error, validate_bool, validate_int, validate_str, validate_strs
)
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.migration import Migration
from entities.migration_table import (
    MigrationTable, SPAN_BATCH_SIZE_IN, SPAN_BATCH_SIZE_OUT, SPAN_CHUNK_SIZE
)
from entities.session import Session


def create_migration_table(input_params: dict[str, Any],
                           errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_table_params: dict[str, Any] = \
            __validate_input(input_params=input_params,
                             valid_params=[i[0] for i in MigrationTable.ATTRS_INPUT],
                             op=OpType.CREATE,
                             db_conn=db_conn,
                             errors=errors)
        if not errors:
            # create and persist the migration table instance
            migration: Migration = Migration(nm_badge=migration_table_params.pop(InputParam.BADGE),
                                             db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)
            if not errors:
                migration_table: MigrationTable = MigrationTable()
                migration_table.id_migration = migration.id
                migration_table.set(migration_table_params)
                migration_table.insert(db_engine=PYDB_DB_ENGINE,
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


def update_migration_table(input_params: dict[str, Any],
                           errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = ([InputParam.MIGRATION_ID, InputParam.TABLE_ID] +
                                   [i[0] for i in MigrationTable.ATTRS_INPUT])
        migration_table_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                  valid_params=valid_params,
                                                                  op=OpType.UPDATE,
                                                                  db_conn=db_conn,
                                                                  errors=errors)
        if not errors:
            # obtain and update the migration table instance
            migration_table: MigrationTable = migration_table_params.pop(InputParam.MIGRATION_TABLE)
            migration_table.set(data=migration_table_params)
            migration_table.update(db_engine=PYDB_DB_ENGINE,
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


def delete_migration_table(input_params: dict[str, Any],
                           errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = ([InputParam.MIGRATION_ID, InputParam.TABLE_ID])
        migration_table_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                  valid_params=valid_params,
                                                                  op=OpType.DELETE,
                                                                  db_conn=db_conn,
                                                                  errors=errors)
        if not errors:
            # obtain and delete the migration table
            migration_table: MigrationTable = migration_table_params.pop(InputParam.MIGRATION_TABLE)
            migration_table.delete(db_engine=PYDB_DB_ENGINE,
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


def retrieve_migration_tables(input_params: dict[str, Any],
                              errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = [InputParam.BADGE, InputParam.TABLE]
        migration_table_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                  valid_params=valid_params,
                                                                  op=OpType.RETRIEVE,
                                                                  db_conn=db_conn,
                                                                  errors=errors)
        if not errors:
            values: list[int] = Migration.get_values(
                attrs=Migration.Db.ID,
                where_data={Migration.Db.NM_BADGE: migration_table_params.get(InputParam.BADGE)},
                db_engine=PYDB_DB_ENGINE,
                db_conn=db_conn,
                errors=errors
            )
            if values:
                where_data: dict[str, Any] | None = {MigrationTable.Db.ID_MIGRATION: values[0]}
                if InputParam.TABLE in migration_table_params:
                    where_data[MigrationTable.Db.NM_TABLE] = migration_table_params.get(InputParam.TABLE)

                migration_tables: list[MigrationTable] = MigrationTable.get_instances(where_data=where_data,
                                                                                      db_engine=PYDB_DB_ENGINE,
                                                                                      db_conn=db_conn,
                                                                                      errors=errors)
                for migration_table in migration_tables or []:
                    mig_table_data: dict[str, Any] = migration_table.get_inputs()
                    values: list[int] = Session.get_values(attrs=Session.Db.CD_SESSION,
                                                           where_data={Session.Db.ID: migration_table.id_session},
                                                           max_count=1,
                                                           min_count=1,
                                                           db_engine=PYDB_DB_ENGINE,
                                                           db_conn=db_conn,
                                                           errors=errors)
                    if errors:
                        break
                    mig_table_data[InputParam.SESSION] = values[0]

                    if migration_table.nr_batch_size_in is not None:
                        mig_table_data[InputParam.BATCH_SIZE_IN] = migration_table.nr_batch_size_in
                    if migration_table.nr_batch_size_out is not None:
                        mig_table_data[InputParam.BATCH_SIZE_OUT] = migration_table.nr_batch_size_out
                    if migration_table.nr_chunk_size is not None:
                        mig_table_data[InputParam.CHUNK_SIZE] = migration_table.nr_chunk_size
                    if migration_table.ds_exclude_columns is not None:
                        mig_table_data[InputParam.EXCLUDE_COLUMNS] = migration_table.ds_exclude_columns
                    if migration_table.ds_exclude_constraints is not None:
                        mig_table_data[InputParam.EXCLUDE_CONSTRAINTS] = migration_table.ds_exclude_constraints
                    if migration_table.ds_named_lobdata is not None:
                        mig_table_data[InputParam.NAMED_LOBDATA] = migration_table.ds_named_lobdata
                    if migration_table.ds_omit_defaults is not None:
                        mig_table_data[InputParam.OMIT_DEFAULTS] = migration_table.ds_omit_defaults
                    if migration_table.ds_override_columns is not None:
                        mig_table_data[InputParam.OVERRIDE_COLUMNS] = migration_table.ds_override_columns
                    if migration_table.is_remove_ctrlchars is not None:
                        mig_table_data[InputParam.REMOVE_CTRLCHARS] = migration_table.is_remove_ctrlchars

                    result[migration_table.nm_table] = mig_table_data
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
                     op: OpType,
                     db_conn: Any,
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    # identify the migration table instance (UPDATE and DELETE operations)
    migration_id: str = validate_str(source=input_params,
                                     attr=InputParam.MIGRATION_ID,
                                     required=op in [OpType.UPDATE, OpType.DELETE],
                                     errors=errors)
    table_id: str = validate_str(source=input_params,
                                 attr=InputParam.TABLE_ID,
                                 max_length=64,
                                 required=op in [OpType.UPDATE, OpType.DELETE],
                                 errors=errors)
    if table_id:
        if InputParam.MIGRATION in result:
            where_data: dict[str, Any] = \
                {f"{Migration.get_alias()}.{Migration.Db.NM_BADGE}": migration_id,
                 f"{MigrationTable.get_alias()}.{MigrationTable.Db.NM_TABLE}": table_id.lower()}
            migration_table: MigrationTable = \
                MigrationTable.get_instance(joins=[(Migration, (Migration.Db.ID, MigrationTable.Db.ID_MIGRATION))],
                                            where_data=where_data,
                                            db_engine=PYDB_DB_ENGINE,
                                            db_conn=db_conn,
                                            errors=errors)
            if not errors:
                result[InputParam.MIGRATION_TABLE] = migration_table

    badge: str = validate_str(source=input_params,
                              attr=InputParam.BADGE,
                              max_length=64,
                              required=op in [OpType.CREATE, OpType.RETRIEVE],
                              errors=errors)
    if badge:
        result[InputParam.BADGE] = badge

    table: str = validate_str(source=input_params,
                              attr=InputParam.TABLE,
                              max_length=64,
                              required=op in [OpType.CREATE],
                              errors=errors)
    if table:
        result[MigrationTable.Db.NM_TABLE] = table.lower()

    nr_batch_size_in: int = validate_int(source=input_params,
                                         attr=InputParam.BATCH_SIZE_IN,
                                         min_val=SPAN_BATCH_SIZE_IN[0],
                                         max_val=SPAN_BATCH_SIZE_IN[2],
                                         errors=errors)
    if nr_batch_size_in:
        result[MigrationTable.Db.NR_BATCH_SIZE_IN] = nr_batch_size_in

    nr_batch_size_out: int = validate_int(source=input_params,
                                          attr=InputParam.BATCH_SIZE_OUT,
                                          min_val=SPAN_BATCH_SIZE_OUT[0],
                                          max_val=SPAN_BATCH_SIZE_OUT[2],
                                          errors=errors)
    if nr_batch_size_out:
        result[MigrationTable.Db.NR_BATCH_SIZE_OUT] = nr_batch_size_out

    nr_chunk_size: int = validate_int(source=input_params,
                                      attr=InputParam.CHUNK_SIZE,
                                      min_val=SPAN_CHUNK_SIZE[0],
                                      max_val=SPAN_CHUNK_SIZE[2],
                                      errors=errors)
    if nr_chunk_size:
        result[MigrationTable.Db.NR_CHUNK_SIZE] = nr_chunk_size

    exclude_columns: list[str] = validate_strs(source=input_params,
                                               attr=InputParam.EXCLUDE_COLUMNS,
                                               errors=errors)
    if exclude_columns:
        result[MigrationTable.Db.DS_EXCLUDE_COLUMNS] = (",".join([i for i in exclude_columns])).lower()

    exclude_constraints: list[str] = validate_strs(source=input_params,
                                                   attr=InputParam.EXCLUDE_CONSTRAINTS,
                                                   errors=errors)
    if exclude_constraints:
        result[MigrationTable.Db.DS_EXCLUDE_CONSTRAINTS] = (",".join([i for i in exclude_constraints])).lower()

    incremental_count: int = validate_int(source=input_params,
                                          attr=InputParam.INCREMENTAL_COUNT,
                                          errors=errors)
    if incremental_count:
        result[MigrationTable.Db.NR_INCREMENTAL_COUNT] = incremental_count

    incremental_offset: int = validate_int(source=input_params,
                                           attr=InputParam.INCREMENTAL_OFFSET,
                                           errors=errors)
    if incremental_offset:
        result[MigrationTable.Db.NR_INCREMENTAL_OFFSET] = incremental_offset

    named_lobdata: list[str] = validate_strs(source=input_params,
                                             attr=InputParam.NAMED_LOBDATA,
                                             errors=errors)
    if named_lobdata:
        result[MigrationTable.Db.DS_NAMED_LOBDATA] = ",".join([i for i in named_lobdata])

    omit_defaults: list[str] = validate_strs(source=input_params,
                                             attr=InputParam.OMIT_DEFAULTS,
                                             errors=errors)
    if omit_defaults:
        result[MigrationTable.Db.DS_OMIT_DEFAULTS] = (",".join([i for i in omit_defaults])).lower()

    override_columns: list[str] = __validate_override_columns(input_params=input_params,
                                                              errors=errors)
    if override_columns:
        result[MigrationTable.Db.DS_OVERRIDE_COLUMNS] = (",".join([i for i in override_columns])).lower()

    remove_ctrlchars: bool = validate_bool(source=input_params,
                                           attr=InputParam.REMOVE_CTRLCHARS,
                                           errors=errors)
    if isinstance(remove_ctrlchars, bool):
        result[MigrationTable.Db.IS_REMOVE_CTRLCHARS] = remove_ctrlchars

    return result


def __validate_override_columns(input_params: dict[str, str],
                                errors: list[str]) -> list[str]:

    # initialize the return variable
    result: list[str] | None = validate_strs(source=input_params,
                                             attr=InputParam.OVERRIDE_COLUMNS,
                                             errors=errors)
    for item in result or []:
        # format of item is <column_name>=<column_type>
        pos: int = item.find('=')
        if pos < 3 or pos > len(item) - 4:
            # 101: {}
            errors.append(validate_format_error(101,
                                                f"Invalid value",
                                                f"@{InputParam.OVERRIDE_COLUMNS}"))
            result = None
            break

    return result
