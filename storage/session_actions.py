from typing import Any
from pypomes_core import validate_str, validate_format_error
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_consts import PYDB_DB_ENGINE, InputParam, OpType
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
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=[item[0] for item in Session.ATTRS_INPUT],
                                                          op=OpType.CREATE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            # create and persist the database
            session: Session = Session()
            session.set(session_params)
            session.insert(db_engine=PYDB_DB_ENGINE,
                           db_conn=db_conn,
                           errors=errors)

        # conclude the operation
        if errors:
            db_rollback(connection=db_conn)
        else:
            db_commit(connection=db_conn,
                      errors=errors)
        db_close(connection=db_conn)


def update_session(input_params: dict[str, Any],
                   errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=[item[0] for item in Session.ATTRS_INPUT],
                                                          op=OpType.UPDATE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            session: Session = Session(db_engine=PYDB_DB_ENGINE,
                                       db_conn=db_conn,
                                       errors=errors)
            if not errors:
                session.set(data=session_params)
                session.update(db_conn=db_conn,
                               errors=errors)

        # conclude the operation
        if errors:
            db_rollback(connection=db_conn)
        else:
            db_commit(connection=db_conn,
                      errors=errors)
        db_close(connection=db_conn)


def delete_session(input_params: dict[str, Any],
                   errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        session_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                          valid_params=[InputParam.CD_SESSION],
                                                          op=OpType.CREATE,
                                                          db_conn=db_conn,
                                                          errors=errors)
        if not errors:
            # obtain and delete the database
            session: Session = Session(cd_session=session_params.get(Session.Db.CD_SESSION),
                                       db_engine=PYDB_DB_ENGINE,
                                       db_conn=db_conn,
                                       errors=errors)
            if not errors:
                session.delete(db_engine=PYDB_DB_ENGINE,
                               db_conn=db_conn,
                               errors=errors)

        # conclude the operation
        if errors:
            db_rollback(connection=db_conn)
        else:
            db_commit(connection=db_conn,
                      errors=errors)
        db_close(connection=db_conn)


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
                                                          valid_params=[InputParam.DB_ENGINE],
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
            db_rollback(connection=db_conn)
        else:
            db_commit(connection=db_conn,
                      errors=errors)
        db_close(connection=db_conn)

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

    cd_session: str = validate_str(source=input_params,
                                   attr=InputParam.CD_SESSION,
                                   max_length=64,
                                   required=op != OpType.RETRIEVE,
                                   errors=errors)
    if cd_session:
        result[Session.Db.CD_SESSION] = cd_session

    source_db: str = validate_str(source=input_params,
                                  attr=InputParam.SOURCE_DB,
                                  max_length=64,
                                  required=op == OpType.CREATE,
                                  errors=errors)
    if source_db:
        values: list[int] = Database.get_values(attrs=Database.Db.ID,
                                                where_data={Database.Db.CD_ENGINE: source_db},
                                                min_count=1,
                                                max_count=1,
                                                db_engine=PYDB_DB_ENGINE,
                                                db_conn=db_conn,
                                                errors=errors)
        if values:
            result[Session.Db.ID_SOURCE_DB] = values[0]

    target_db: str = validate_str(source=input_params,
                                  attr=InputParam.SOURCE_DB,
                                  max_length=64,
                                  required=op == OpType.CREATE,
                                  errors=errors)
    if target_db:
        values: list[int] = Database.get_values(attrs=Database.Db.ID,
                                                where_data={Database.Db.CD_ENGINE: target_db},
                                                min_count=1,
                                                max_count=1,
                                                db_engine=PYDB_DB_ENGINE,
                                                db_conn=db_conn,
                                                errors=errors)
        if values:
            result[Session.Db.ID_TARGET_DB] = values[0]

    target_s3: str = validate_str(source=input_params,
                                  attr=InputParam.TARGET_S3,
                                  max_length=64,
                                  errors=errors)
    if target_s3:
        values: list[int] = S3.get_values(attrs=S3.Db.ID,
                                          where_data={S3.Db.CD_ENGINE: target_s3},
                                          min_count=1,
                                          max_count=1,
                                          db_engine=PYDB_DB_ENGINE,
                                          db_conn=db_conn,
                                          errors=errors)
        if values:
            result[Session.Db.ID_TARGET_S3] = values[0]

    return result
