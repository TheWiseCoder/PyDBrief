from typing import Any
from pypomes_core import validate_str, validate_format_error
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.database import Database
from entities.s3 import S3
from entities.session import Session, SessionState


def create_session(input_params: dict[str, Any],
                   errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[InputParam] = [InputParam.SESSION] + [item[0] for item in Session.ATTRS_INPUT]
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=valid_params,
                                                          op=OpType.CREATE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            # create and persist the database
            session: Session = Session()
            session.id_source_db = session_params.pop(InputParam.SOURCE_DB).id
            session.id_target_db = session_params.pop(InputParam.TARGET_DB).id
            if InputParam.TARGET_S3 in session_params:
                session.id_target_db = session_params.pop(InputParam.TARGET_S3)
            session.set(session_params)
            session.insert(db_engine=PYDB_DB_ENGINE,
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


def update_session(input_params: dict[str, Any],
                   errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[InputParam] = [InputParam.SESSION_ID] + [i[0] for i in Session.ATTRS_INPUT]
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=valid_params,
                                                          op=OpType.UPDATE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            session: Session = session_params.pop(InputParam.SESSION)
            if InputParam.SOURCE_DB in session_params:
                session.id_source_db = session_params.pop(InputParam.SOURCE_DB).id
            if InputParam.TARGET_DB in session_params:
                session.id_target_db = session_params.pop(InputParam.TARGET_DB).id
            if InputParam.TARGET_S3 in session_params:
                session.id_target_db = session_params.pop(InputParam.TARGET_S3).id
            session.set(data=session_params)
            session.update(db_conn=db_conn,
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


def delete_session(input_params: dict[str, Any],
                   errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=[InputParam.SESSION_ID],
                                                          op=OpType.DELETE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            # obtain and delete the database
            session: Session = session_params[InputParam.SESSION]
            session.delete(db_engine=PYDB_DB_ENGINE,
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


def retrieve_sessions(input_params: dict[str, Any],
                      errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=[InputParam.SESSION],
                                                          op=OpType.RETRIEVE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            where_data: dict[str, Any]
            if Session.Db.CD_SESSION in session_params:
                where_data = {Session.Db.CD_SESSION: session_params.get(Session.Db.CD_SESSION)}
            else:
                where_data = {Session.Db.CD_STATE: [SessionState.CREATED, SessionState.STARTED]}
            sessions: list[Session] = Session.retrieve(where_data=where_data,
                                                       db_engine=PYDB_DB_ENGINE,
                                                       db_conn=db_conn,
                                                       errors=errors)
            for session in sessions or []:
                session_data: dict[str, Any] = session.get_inputs()
                session_data[InputParam.STATE] = session.cd_state.name

                source_db: Database = session.get_source_db(db_engine=PYDB_DB_ENGINE,
                                                            db_conn=db_conn,
                                                            errors=errors)
                if errors:
                    break
                session_data[InputParam.SOURCE_DB] = source_db.get_inputs()

                target_db: Database = session.get_target_db(db_engine=PYDB_DB_ENGINE,
                                                            db_conn=db_conn,
                                                            errors=errors)
                if errors:
                    break
                session_data[InputParam.TARGET_DB] = target_db.get_inputs()

                target_s3: S3 = session.get_target_s3(db_engine=PYDB_DB_ENGINE,
                                                      db_conn=db_conn,
                                                      errors=errors)
                if errors:
                    break
                if target_s3:
                    session_data[InputParam.TARGET_S3] = target_s3.get_inputs()

                result[session.cd_session] = session_data

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
                     valid_params: list[InputParam],
                     op: OpType,
                     db_conn: Any,
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    # identify the session instance (UPDATE and DELETE operations)
    session_id: str = validate_str(source=input_params,
                                   attr=InputParam.SESSION_ID,
                                   max_length=64,
                                   required=op in [OpType.UPDATE, OpType.DELETE],
                                   errors=errors)
    if session_id:
        result[InputParam.SESSION_ID] = session_id

    cd_session: str = validate_str(source=input_params,
                                   attr=InputParam.SESSION,
                                   max_length=64,
                                   required=op == OpType.CREATE,
                                   errors=errors)
    if cd_session:
        result[Session.Db.CD_SESSION] = Session(cd_session=cd_session,
                                                db_engine=PYDB_DB_ENGINE,
                                                db_conn=db_conn,
                                                errors=errors)

    # identify the source database instance (CREATE and UPDATE operations)
    source_db: str = validate_str(source=input_params,
                                  attr=InputParam.SOURCE_DB,
                                  required=op == OpType.CREATE,
                                  errors=errors)
    if source_db:
        result[InputParam.SOURCE_DB] = Database(cd_engine=source_db,
                                                db_engine=PYDB_DB_ENGINE,
                                                db_conn=db_conn,
                                                errors=errors)

    # identify the target database instance (CREATE and UPDATE operations)
    target_db: str = validate_str(source=input_params,
                                  attr=InputParam.TARGET_DB,
                                  required=op == OpType.CREATE,
                                  errors=errors)
    if target_db:
        if target_db == source_db:
            # 100: {}
            errors.append(validate_format_error(100,
                                                "Source and target databases cannot be the same"))
        else:
            result[InputParam.TARGET_DB] = Database(cd_engine=target_db,
                                                    db_engine=PYDB_DB_ENGINE,
                                                    db_conn=db_conn,
                                                    errors=errors)

    target_s3: str = validate_str(source=input_params,
                                  attr=InputParam.TARGET_S3,
                                  errors=errors)
    if target_s3:
        result[InputParam.TARGET_S3] = S3(cd_engine=target_s3,
                                          db_engine=PYDB_DB_ENGINE,
                                          db_conn=db_conn,
                                          errors=errors)

    source_schema: str = validate_str(source=input_params,
                                      attr=InputParam.SOURCE_SCHEMA,
                                      max_length=64,
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if source_schema:
        result[Session.Db.NM_SOURCE_SCHEMA] = source_schema

    target_schema: str = validate_str(source=input_params,
                                      attr=InputParam.TARGET_SCHEMA,
                                      max_length=64,
                                      required=op == OpType.CREATE,
                                      errors=errors)
    if target_schema:
        result[Session.Db.NM_TARGET_SCHEMA] = target_schema

    return result
