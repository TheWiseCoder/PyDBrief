from typing import Any
from pypomes_core import (
    validate_int, validate_enum, validate_str, validate_format_error
)
from pypomes_db import DbEngine, db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.database import Database


def create_database(input_params: dict[str, Any],
                    errors: list[str]) -> None:

    # validate the input data
    database_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                       valid_params=[i[0] for i in Database.ATTRS_INPUT],
                                                       op=OpType.CREATE,
                                                       errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            # create and persist the database
            database: Database = Database()
            if InputParam.DB_PWD in database_params:
                database._nm_pwd = database_params.pop(InputParam.DB_PWD)
            database.set(database_params)
            database.insert(db_engine=PYDB_DB_ENGINE,
                            db_conn=db_conn,
                            errors=errors)

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def update_database(input_params: dict[str, Any],
                    errors: list[str]) -> None:

    # validate the input data
    database_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                       valid_params=[i[0] for i in Database.ATTRS_INPUT],
                                                       op=OpType.UPDATE,
                                                       errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            database: Database = Database(cd_engine=database_params.get(Database.Db.CD_ENGINE),
                                          db_engine=PYDB_DB_ENGINE,
                                          db_conn=db_conn,
                                          errors=errors)
            if not errors:
                if InputParam.DB_PWD in database_params:
                    database._nm_pwd = database_params.pop(InputParam.DB_PWD)
                database.set(data=database_params)
                database.update(db_conn=db_conn,
                                errors=errors)

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def delete_database(input_params: dict[str, Any],
                    errors: list[str]) -> None:

    # validate the input data
    database_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                       valid_params=[InputParam.DB_ENGINE],
                                                       op=OpType.DELETE,
                                                       errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            # obtain and delete the database
            database: Database = Database(cd_engine=database_params.get(Database.Db.CD_ENGINE),
                                          db_engine=PYDB_DB_ENGINE,
                                          db_conn=db_conn,
                                          errors=errors)
            if not errors:
                database.delete(db_engine=PYDB_DB_ENGINE,
                                db_conn=db_conn,
                                errors=errors)
            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def retrieve_databases(input_params: dict[str, Any],
                       errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # validate the input data
    database_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                       valid_params=[InputParam.DB_ENGINE],
                                                       op=OpType.RETRIEVE,
                                                       errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            where_data: dict[str, Any] | None = None
            if Database.Db.CD_ENGINE in database_params:
                where_data = {Database.Db.CD_ENGINE: database_params.get(Database.Db.CD_ENGINE)}
            databases: list[Database] = Database.retrieve(where_data=where_data,
                                                          db_engine=PYDB_DB_ENGINE,
                                                          db_conn=db_conn,
                                                          errors=errors)
            for database in databases or []:
                result[database.cd_engine] = database.get_inputs()

    return result


def __validate_input(input_params: dict[str, Any],
                     valid_params: list[str],
                     op: OpType,
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    db_engine: str = validate_str(source=input_params,
                                  attr=InputParam.DB_ENGINE,
                                  max_length=64,
                                  required=op != OpType.RETRIEVE,
                                  errors=errors)
    if db_engine:
        result[Database.Db.CD_ENGINE] = db_engine

    db_type: DbEngine = validate_enum(source=input_params,
                                      attr=InputParam.DB_TYPE,
                                      enum_class=DbEngine,
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if db_type:
        if db_type == DbEngine.MYSQL:
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                db_type,
                                                "engine not yet ratified for migration",
                                                f"@{InputParam.DB_TYPE}"))
        else:
            result[Database.Db.CD_TYPE] = db_type

    # not directly mapped to a database column
    db_pwd: str = validate_str(source=input_params,
                               attr=InputParam.DB_PWD,
                               required=op == OpType.CREATE,
                               errors=errors)
    if db_pwd:
        result[InputParam.DB_PWD] = db_pwd

    db_name: str = validate_str(source=input_params,
                                attr=InputParam.DB_NAME,
                                max_length=64,
                                required=op == OpType.CREATE,
                                errors=errors)
    if db_name:
        result[Database.Db.CD_NAME] = db_name

    db_host: str = validate_str(source=input_params,
                                attr=InputParam.DB_HOST,
                                max_length=64,
                                required=op == OpType.CREATE,
                                errors=errors)
    if db_host:
        result[Database.Db.NM_HOST] = db_host

    db_port: int = validate_int(source=input_params,
                                attr=InputParam.DB_PORT,
                                min_val=1,
                                required=op == OpType.CREATE,
                                errors=errors)
    if db_port:
        result[Database.Db.NR_PORT] = db_port

    db_user: str = validate_str(source=input_params,
                                attr=InputParam.DB_USER,
                                max_length=64,
                                required=op == OpType.CREATE,
                                errors=errors)
    if db_user:
        result[Database.Db.NM_USER] = db_user

    db_client: str = validate_str(source=input_params,
                                  attr=InputParam.DB_CLIENT,
                                  errors=errors)
    if db_client:
        if result[Database.Db.CD_TYPE] != DbEngine.ORACLE:
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                db_client,
                                                "client is specific to Oracle databases",
                                                f"@{InputParam.DB_CLIENT}"))
        else:
            result[Database.Db.NM_CLIENT] = db_client

    db_driver: str = validate_str(source=input_params,
                                  attr=InputParam.DB_DRIVER,
                                  errors=errors)
    if db_driver:
        if result[Database.Db.CD_TYPE] != DbEngine.ORACLE:
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                db_client,
                                                "driver is specific to SQLServer databases",
                                                f"@{InputParam.DB_DRIVER}"))
        else:
            result[Database.Db.DS_DRIVER] = db_driver

    return result
