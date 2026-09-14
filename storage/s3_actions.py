from typing import Any

from pypomes_core import (
    validate_bool, validate_enum, validate_str, validate_format_error
)
from pypomes_db import DbEngine, db_connect, db_commit, db_rollback, db_close

from app_consts import PYDB_DB_ENGINE, InputParam, OpType
from entities.s3 import S3, S3Engine


def create_s3(input_params: dict[str, Any],
              errors: list[str]) -> None:

    # validate the input data
    database_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                       valid_params=[i[0] for i in S3.ATTRS_INPUT],
                                                       op=OpType.CREATE,
                                                       errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            # create and persist the database
            s3: S3 = S3()
            s3.set(database_params)
            s3.insert(db_engine=PYDB_DB_ENGINE,
                      db_conn=db_conn,
                      errors=errors)

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def update_s3(input_params: dict[str, Any],
              errors: list[str]) -> None:

    # validate the input data
    database_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                       valid_params=[i[0] for i in S3.ATTRS_INPUT],
                                                       op=OpType.UPDATE,
                                                       errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            s3: S3 = S3(cd_engine=database_params.get(S3.Db.CD_ENGINE),
                        db_conn=db_conn,
                        errors=errors)
            if not errors:
                s3.set(data=database_params)
                s3.update(db_conn=db_conn,
                          errors=errors)

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def delete_s3(input_params: dict[str, Any],
              errors: list[str]) -> None:

    # validate the input data
    s3_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                 valid_params=[InputParam.S3_ENGINE],
                                                 op=OpType.DELETE,
                                                 errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            # obtain and delete the database
            s3: S3 = S3(cd_engine=s3_params.get(S3.Db.CD_ENGINE),
                        db_engine=PYDB_DB_ENGINE,
                        db_conn=db_conn,
                        errors=errors)
            if not errors:
                s3.delete(db_engine=PYDB_DB_ENGINE,
                          db_conn=db_conn,
                          errors=errors)

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn)
            else:
                db_commit(connection=db_conn,
                          errors=errors)
            db_close(connection=db_conn)


def retrieve_s3s(input_params: dict[str, Any],
                 errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # validate the input data
    s3_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                 valid_params=[InputParam.DB_ENGINE],
                                                 op=OpType.RETRIEVE,
                                                 errors=errors)
    where_data: dict[str, Any] | None = None
    if S3.Db.CD_ENGINE in s3_params:
        where_data = {S3.Db.CD_ENGINE: s3_params.get(S3.Db.CD_ENGINE)}
    s3s: list[S3] = S3.retrieve(where_data=where_data,
                                db_engine=PYDB_DB_ENGINE,
                                errors=errors)
    for s3 in s3s or []:
        result[s3.cd_engine] = s3.get_inputs()

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

    s3_engine: str = validate_str(source=input_params,
                                  attr=InputParam.S3_ENGINE,
                                  max_length=64,
                                  required=op != OpType.RETRIEVE,
                                  errors=errors)
    if s3_engine:
        result[S3.Db.CD_ENGINE] = s3_engine

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
