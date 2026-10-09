from logging import Logger
from typing import Any
from pypomes_core import (
    validate_format_error, validate_bool, validate_enum, validate_int, validate_str, validate_strs
)
from pypomes_db import (
    DbEngine, DbConnectionPool, DbPoolEvent,
    db_get_pool, db_get_engines, db_get_type,
    db_setup, db_startup, db_connect, db_commit, db_rollback, db_close
)
from pypomes_s3 import s3_get_engines, s3_setup, s3_startup

from app_constants import PYDB_DB_ENGINE, InputParam, MigState, MigStep, OpType
from entities.migration import SPAN_CHANNEL_COUNT, SPAN_CHANNEL_SIZE, Migration, minded_migrations
from entities.database import Database
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
                                                            db_conn=db_conn,
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
                                                            db_conn=db_conn,
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
                    # display the known states
                    mig_data[InputParam.SESSION] = values[0]
                    mig_states: dict[str, MigState] = {}
                    for k, v in minded_migrations:
                        if k[3:] == str(migration.id):
                            mig_states[k[:2]] = v
                    if mig_states:
                        mig_data[InputParam.STATES] = mig_states

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
                    mig_data[InputParam.TABLE_SPECS] = mig_tables
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


def abort_migration(input_params: dict[str, Any] | Session,
                    errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        vald_params: list[InputParam] = [InputParam.MIGRATION_ID, InputParam.STEP]
        migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                            valid_params=vald_params,
                                                            op=OpType.VERIFY,
                                                            db_conn=db_conn,
                                                            errors=errors)
        if not errors:
            migration: Migration = migration_params.get(InputParam.MIGRATION)
            mig_step: MigStep = migration_params.get(InputParam.STEP)
            mig_key = f"{mig_step}-{migration.nm_badge}"
            if minded_migrations.get(mig_key) == MigState.MIGRATING:
                minded_migrations[mig_key] = MigState.ABORTING
            else:
                # 100: {}
                errors.append(validate_format_error(100,
                                                    f"Migration '{migration.nm_badge}', "
                                                    f"step '{mig_step}',not running"))


def verify_migration(input_params: dict[str, Any] | Session,
                     errors: list[str],
                     logger: Logger) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        session: Session | None = None
        if isinstance(input_params, dict):
            # validate the input data
            vald_params: list[InputParam] = [InputParam.MIGRATION_ID, InputParam.STEP]
            migration_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                valid_params=vald_params,
                                                                op=OpType.VERIFY,
                                                                db_conn=db_conn,
                                                                errors=errors)
            if not errors:
                session = Session(migration_params[InputParam.MIGRATION].id_session,
                                  db_engine=PYDB_DB_ENGINE,
                                  db_conn=db_conn,
                                  errors=errors)
        else:
            session = input_params

        if not errors:
            db_engines: list[str] = db_get_engines()
            database: Database = session.get_source_db(db_engine=PYDB_DB_ENGINE,
                                                       db_conn=db_conn,
                                                       errors=errors)
            if database:
                __validate_db_engine(database=database,
                                     db_engines=db_engines,
                                     errors=errors,
                                     logger=logger)
            # validate target db regardless of source db validation
            database = session.get_target_db(db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)
            if database:
                __validate_db_engine(database=database,
                                     db_engines=db_engines,
                                     errors=errors,
                                     logger=logger)
        if not errors:
            s3_engines: list[str] = s3_get_engines()
            s3: S3 = session.get_target_s3(db_engine=PYDB_DB_ENGINE,
                                           db_conn=db_conn,
                                           errors=errors)
            if s3:
                if s3 in s3_engines:
                    s3_startup(engine=s3.cd_engine,
                               errors=errors)
                else:
                    # noinspection PyProtectedMember
                    s3_setup(engine=s3.cd_engine,
                             endpoint_url=s3.ds_endpoint_url,
                             bucket_name=s3.nm_bucket,
                             access_key=s3.nm_access_key,
                             secret_key=s3._nm_secret_key,
                             region_name=s3.nm_region,
                             secure_access=s3.is_secure_access,
                             logger=logger) and s3_startup(engine=s3.cd_engine,
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
                                     required=op in [OpType.ABORT, OpType.DELETE, OpType.UPDATE, OpType.VERIFY],
                                     errors=errors)
    if migration_id:
        result[InputParam.MIGRATION] = Migration(nm_badge=migration_id,
                                                 db_engine=PYDB_DB_ENGINE,
                                                 db_conn=db_conn,
                                                 errors=errors)

    # identity the step
    mig_step: MigStep = validate_enum(source=input_params,
                                      attr=InputParam.STEP,
                                      enum_class=MigStep,
                                      required=op in [OpType.ABORT, OpType.VERIFY],
                                      errors=errors)
    if mig_step:
        result[InputParam.STEP] = mig_step

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

    is_flatten_storage: bool = validate_bool(source=input_params,
                                             attr=InputParam.FLATTEN_STORAGE,
                                             errors=errors)
    if isinstance(is_flatten_storage, bool) or \
            (InputParam.FLATTEN_STORAGE in input_params and is_flatten_storage is None):
        result[Migration.Db.IS_FLATTEN_STORAGE] = is_flatten_storage

    is_optimize_pks: bool = validate_bool(source=input_params,
                                          attr=InputParam.OPTIMIZE_PKS,
                                          errors=errors)
    if isinstance(is_optimize_pks, bool) or \
            (InputParam.OPTIMIZE_PKS in input_params and is_optimize_pks is None):
        result[Migration.Db.IS_OPTIMIZE_PKS] = is_optimize_pks

    is_process_indexes: bool = validate_bool(source=input_params,
                                             attr=InputParam.PROCESS_INDEXES,
                                             errors=errors)
    if isinstance(is_process_indexes, bool) or \
            (InputParam.PROCESS_INDEXES in input_params and is_process_indexes is None):
        result[Migration.Db.IS_PROCESS_INDEXES] = is_process_indexes

    is_process_views: bool = validate_bool(source=input_params,
                                           attr=InputParam.PROCESS_VIEWS,
                                           errors=errors)
    if isinstance(is_process_views, bool) or \
            (InputParam.PROCESS_VIEWS in input_params and is_process_views is None):
        result[Migration.Db.IS_PROCESS_VIEWS] = is_process_views

    is_reflect_filetype: bool = validate_bool(source=input_params,
                                              attr=InputParam.REFLECT_FILETYPE,
                                              errors=errors)
    if isinstance(is_reflect_filetype, bool) or \
            (InputParam.REFLECT_FILETYPE in input_params and is_reflect_filetype is None):
        result[Migration.Db.IS_REFLECT_FILETYPE] = is_reflect_filetype

    is_relax_reflection: bool = validate_bool(source=input_params,
                                              attr=InputParam.RELAX_REFLECTION,
                                              errors=errors)
    if isinstance(is_relax_reflection, bool) or \
            (InputParam.RELAX_REFLECTION in input_params and is_relax_reflection is None):
        result[Migration.Db.IS_RELAX_REFLECTION] = is_relax_reflection

    is_skip_nonempty: bool = validate_bool(source=input_params,
                                           attr=InputParam.SKIP_NONEMPTY,
                                           errors=errors)
    if isinstance(is_skip_nonempty, bool) or \
            (InputParam.SKIP_NONEMPTY in input_params and is_skip_nonempty is None):
        result[Migration.Db.IS_SKIP_NONEMPTY] = is_skip_nonempty

    nr_channel_count: int = validate_int(source=input_params,
                                         attr=InputParam.CHANNEL_COUNT,
                                         min_val=SPAN_CHANNEL_COUNT[0],
                                         max_val=SPAN_CHANNEL_COUNT[1],
                                         errors=errors)
    if nr_channel_count or \
            (InputParam.CHANNEL_COUNT in input_params and nr_channel_count is None):
        result[Migration.Db.NR_CHANNEL_COUNT] = nr_channel_count

    nr_channel_size: int = validate_int(source=input_params,
                                        attr=InputParam.CHANNEL_SIZE,
                                        min_val=SPAN_CHANNEL_SIZE[0],
                                        max_val=SPAN_CHANNEL_SIZE[1],
                                        errors=errors)
    if nr_channel_size or \
            (InputParam.CHANNEL_SIZE in input_params and nr_channel_size is None):
        result[Migration.Db.NR_CHANNEL_SIZE] = nr_channel_size

    exclude_relations: list[str] = validate_strs(source=input_params,
                                                 attr=InputParam.EXCLUDE_RELATIONS,
                                                 errors=errors)
    if exclude_relations:
        result[Migration.Db.DS_EXCLUDE_RELATIONS] = (",".join([i for i in exclude_relations])).lower()
    elif InputParam.EXCLUDE_RELATIONS in input_params and exclude_relations is None:
        result[Migration.Db.DS_EXCLUDE_RELATIONS] = None

    include_relations: list[str] = validate_strs(source=input_params,
                                                 attr=InputParam.INCLUDE_RELATIONS,
                                                 errors=errors)
    if include_relations:
        result[Migration.Db.DS_INCLUDE_RELATIONS] = (",".join([i for i in include_relations])).lower()
    elif InputParam.INCLUDE_RELATIONS in input_params and include_relations is None:
        result[Migration.Db.DS_INCLUDE_RELATIONS] = None

    pre_sql: list[str] = validate_strs(source=input_params,
                                       attr=InputParam.PRE_SQL,
                                       errors=errors)
    if pre_sql:
        result[Migration.Db.DS_PRE_SQL] = (",".join([i for i in pre_sql]))
    elif InputParam.PRE_SQL in input_params and pre_sql is None:
        result[Migration.Db.DS_PRE_SQL] = None

    reify_mviews: list[str] = validate_strs(source=input_params,
                                            attr=InputParam.REIFY_MVIEWS,
                                            errors=errors)
    if reify_mviews:
        result[Migration.Db.DS_REIFY_MVIEWS] = (",".join([i for i in reify_mviews])).lower()
    elif InputParam.REIFY_MVIEWS in input_params and reify_mviews is None:
        result[Migration.Db.DS_REIFY_MVIEWS] = None

    return result


def __validate_db_engine(database: Database,
                         db_engines: list[str],
                         errors: list[str],
                         logger: Logger) -> None:

    if database.cd_engine in db_engines:
        db_startup(engine=database.cd_engine,
                   errors=errors)
    else:
        # noinspection PyProtectedMember
        if db_setup(engine=database.cd_engine,
                    db_name=database.cd_name,
                    db_user=database.nm_user,
                    db_pwd=database._nm_pwd,
                    db_host=database.nm_host,
                    db_port=database.nr_port,
                    db_type=database.cd_type,
                    db_client=database.nm_client,
                    db_driver=database.ds_driver,
                    logger=logger):
            __pool_setup(db_engine=database.cd_engine,
                         errors=errors)
            if not errors:
                db_startup(engine=database.cd_engine,
                           errors=errors)


def __pool_setup(db_engine: str,
                 errors: list[str]) -> None:

    curr_errors: list[str] = []
    pool: DbConnectionPool = (db_get_pool(engine=db_engine) or
                              DbConnectionPool(db_engine,
                                               pool_size=20,
                                               errors=curr_errors))
    if not curr_errors:
        stmts: list[str] = []
        db_type: DbEngine = db_get_type(engine=db_engine)
        # fine-tune all database sessions, as needed
        # (Oracle and SQLServer do not have session-scope commands for disabling triggers and/or rules)
        match db_type:
            case DbEngine.MYSQL:
                stmts.append("SET @@SESSION.DISABLE_TRIGGERS = 1")
            case DbEngine.ORACLE:
                stmts.extend(["ALTER SESSION SET NLS_SORT = BINARY",
                              "ALTER SESSION SET NLS_COMP = BINARY"])
            case DbEngine.POSTGRES:
                stmts.append("set session_replication_role = replica")
        if stmts:
            pool.on_event_actions(event=DbPoolEvent.CREATE,
                                  stmts=stmts)
    elif isinstance(errors, list):
        errors.extend(curr_errors)
