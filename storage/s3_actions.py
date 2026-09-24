from typing import Any

from pypomes_core import (
    validate_bool, validate_enum, validate_str, validate_format_error
)
from pypomes_db import DbEngine, db_connect, db_commit, db_rollback, db_close
from pypomes_s3 import S3Engine

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.s3 import S3


def create_s3(input_params: dict[str, Any],
              errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        s3_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                     valid_params=[i[0] for i in S3.ATTRS_INPUT],
                                                     op=OpType.CREATE,
                                                     db_conn=db_conn,
                                                     errors=errors)
        if not errors:
            # create and persist the database
            s3: S3 = S3()
            s3._nm_secret_key = s3_params.pop(InputParam.S3_SECRET_KEY)
            s3.set(s3_params)
            s3.insert(db_engine=PYDB_DB_ENGINE,
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


def update_s3(input_params: dict[str, Any],
              errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = [InputParam.ENGINE_ID] + [i[0] for i in S3.ATTRS_INPUT]
        s3_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                     valid_params=valid_params,
                                                     op=OpType.UPDATE,
                                                     db_conn=db_conn,
                                                     errors=errors)
        if not errors:
            # obtain and update the S3 instance
            s3: S3 = s3_params.pop(InputParam.S3)
            if InputParam.S3_SECRET_KEY in s3_params:
                s3._nm_secret_key = s3_params.pop(InputParam.S3_SECRET_KEY)
            s3.set(data=s3_params)
            s3.update(db_conn=db_conn,
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


def delete_s3(input_params: dict[str, Any],
              errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        s3_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                     valid_params=[InputParam.ENGINE_ID],
                                                     op=OpType.DELETE,
                                                     db_conn=db_conn,
                                                     errors=errors)
        if not errors:
            # obtain and delete the database
            s3: S3 = s3_params[InputParam.S3]
            s3.delete(db_engine=PYDB_DB_ENGINE,
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


def retrieve_s3s(input_params: dict[str, Any],
                 errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = [InputParam.S3_ENGINE, InputParam.S3_TYPE]
        s3_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                     valid_params=valid_params,
                                                     op=OpType.RETRIEVE,
                                                     db_conn=db_conn,
                                                     errors=errors)
        if not errors:
            where_data: dict[str, Any] | None = None
            if S3.Db.CD_ENGINE in s3_params:
                where_data = {S3.Db.CD_ENGINE: s3_params.get(S3.Db.CD_ENGINE)}
            elif S3.Db.CD_TYPE in s3_params:
                where_data = {S3.Db.CD_TYPE: s3_params.get(S3.Db.CD_TYPE)}
            s3s: list[S3] = S3.get_instances(where_data=where_data,
                                             db_engine=PYDB_DB_ENGINE,
                                             db_conn=db_conn,
                                             errors=errors)
            for s3 in s3s or []:
                result[s3.cd_engine] = s3.get_inputs()

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

    # identify the S3 instance (UPDATE and DELETE operations)
    engine_id: str = validate_str(source=input_params,
                                  attr=InputParam.ENGINE_ID,
                                  required=op in [OpType.UPDATE, OpType.DELETE],
                                  errors=errors)
    if engine_id:
        result[InputParam.S3] = S3(cd_engine=engine_id.lower(),
                                   db_engine=PYDB_DB_ENGINE,
                                   db_conn=db_conn,
                                   errors=errors)

    s3_engine: str = validate_str(source=input_params,
                                  attr=InputParam.S3_ENGINE,
                                  max_length=64,
                                  required=op == OpType.CREATE,
                                  errors=errors)
    if s3_engine:
        result[S3.Db.CD_ENGINE] = s3_engine.lower()

    s3_type: DbEngine = validate_enum(source=input_params,
                                      attr=InputParam.S3_TYPE,
                                      enum_class=S3Engine,
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if s3_type:
        result[S3.Db.CD_TYPE] = s3_type

    # not directly mapped to a database column
    s3_secret_key: str = validate_str(source=input_params,
                                      attr=InputParam.S3_SECRET_KEY,
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if s3_secret_key:
        result[InputParam.S3_SECRET_KEY] = s3_secret_key

    s3_bucket_name: str = validate_str(source=input_params,
                                       attr=InputParam.S3_BUCKET_NAME,
                                       max_length=64,
                                       required=op == OpType.CREATE,
                                       errors=errors)
    if s3_bucket_name:
        result[S3.Db.NM_BUCKET] = s3_bucket_name

    s3_access_key: str = validate_str(source=input_params,
                                      attr=InputParam.S3_ACCESS_KEY,
                                      max_length=64,
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if s3_access_key:
        result[S3.Db.NM_ACCESS_KEY] = s3_access_key

    s3_endpoint_url: str = validate_str(source=input_params,
                                        attr=InputParam.S3_ENDPOINT_URL,
                                        max_length=256,
                                        required=op == OpType.CREATE,
                                        errors=errors)
    if s3_endpoint_url:
        result[S3.Db.DS_ENDPOINT_URL] = s3_endpoint_url

    s3_secure_access: bool = validate_bool(source=input_params,
                                           attr=InputParam.S3_SECURE_ACCESS,
                                           required=op == OpType.CREATE,
                                           errors=errors)
    if isinstance(s3_secure_access, bool):
        result[S3.Db.IS_SECURE_ACCESS] = s3_secure_access

    return result
