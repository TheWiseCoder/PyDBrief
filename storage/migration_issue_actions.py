from typing import Any
from pypomes_core import validate_format_error, validate_int, validate_str, validate_enum
from pypomes_db import db_connect, db_commit, db_rollback, db_close

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.migration import Migration
from entities.migration_issue import MigrationIssue, IssueType


def create_migration_issue(input_params: dict[str, Any],
                           errors: list[str]) -> None:
    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_issue_params: dict[str, Any] = \
            __validate_input(input_params=input_params,
                             valid_params=[i[0] for i in MigrationIssue.ATTRS_INPUT],
                             op=OpType.CREATE,
                             errors=errors)
        if not errors:
            # create and persist the migration issue
            migration_issue: MigrationIssue = MigrationIssue()
            migration_issue.set(migration_issue_params)
            migration_issue.insert(db_engine=PYDB_DB_ENGINE,
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


def update_migration_issue(input_params: dict[str, Any],
                           errors: list[str]) -> None:

    # validate the input data
    valid_params: list[str] = [InputParam.CD_ISSUE] + [i[0] for i in MigrationIssue.ATTRS_INPUT]
    migration_issue_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                              valid_params=valid_params,
                                                              op=OpType.UPDATE,
                                                              errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            migration_issue: MigrationIssue = \
                MigrationIssue(migration_issue_params.get(InputParam.CD_ISSUE),
                               db_engine=PYDB_DB_ENGINE,
                               db_conn=db_conn,
                               errors=errors)
            if not errors:
                migration_issue.set(data=migration_issue_params)
                migration_issue.update(db_engine=PYDB_DB_ENGINE,
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


def delete_migration_issue(input_params: dict[str, Any],
                           errors: list[str]) -> None:

    # validate the input data
    migration_issue_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                              valid_params=[InputParam.CD_ISSUE],
                                                              op=OpType.DELETE,
                                                              errors=errors)
    if not errors:
        # obtain DB connection
        db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                                  errors=errors)
        if db_conn:
            migration_issue: MigrationIssue = MigrationIssue(migration_issue_params.get(InputParam.CD_ISSUE),
                                                             db_engine=PYDB_DB_ENGINE,
                                                             db_conn=db_conn,
                                                             errors=errors)
            if not errors:
                migration_issue.delete(db_engine=PYDB_DB_ENGINE,
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


def retrieve_migration_issues(input_params: dict[str, Any],
                              errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_issue_params: dict[str, Any] = \
            __validate_input(input_params=input_params,
                             valid_params=[InputParam.S3_ENGINE, InputParam.S3_TYPE],
                             op=OpType.RETRIEVE,
                             errors=errors)
        if not errors:
            where_data: dict[str, Any] | None = None
            if InputParam.CD_ISSUE in migration_issue_params:
                where_data = {MigrationIssue.Db.ID: migration_issue_params.get(InputParam.CD_ISSUE)}
            elif InputParam.CD_BADGE in migration_issue_params:
                values: list[int] = Migration.get_values(
                    attrs=Migration.Db.ID,
                    where_data={Migration.Db.NM_BADGE: migration_issue_params.get(InputParam.CD_BADGE)},
                    db_engine=PYDB_DB_ENGINE,
                    db_conn=db_conn)
                if values:
                    where_data = {MigrationIssue.Db.ID_MIGRATION: values[0]}

            if where_data:
                migration_issues: list[MigrationIssue] = MigrationIssue.retrieve(where_data=where_data,
                                                                                 db_engine=PYDB_DB_ENGINE,
                                                                                 db_conn=db_conn,
                                                                                 errors=errors)
                for migration_issue in migration_issues or []:
                    mig_issue_data: dict[str, Any] = migration_issue.get_inputs()
                    result[migration_issue.id] = mig_issue_data
            else:
                # 100: {} (omits the attribute "code")
                errors.append(validate_format_error(100,
                                                    "Either 'CD-ISSUE' or 'BADGE' must be specified"))
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
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    # this identifies the migration issue instance
    cd_issue: int = validate_int(source=input_params,
                                 attr=InputParam.CD_ISSUE,
                                 required=op in [OpType.UPDATE, OpType.DELETE],
                                 errors=errors)
    if cd_issue:
        result[InputParam.CD_ISSUE] = cd_issue

    # this identifies the migration instance
    cd_badge: str = validate_str(source=input_params,
                                 attr=InputParam.BADGE,
                                 errors=errors)
    if cd_badge:
        result[InputParam.CD_BADGE] = cd_badge

    cd_type: IssueType = validate_enum(source=input_params,
                                       attr=InputParam.TYPE,
                                       enum_class=IssueType,
                                       required=op == OpType.CREATE,
                                       errors=errors)
    if cd_type:
        result[MigrationIssue.Db.CD_TYPE] = cd_type

    # this is the value assigned to the attribute
    ds_issue: str = validate_str(source=input_params,
                                 attr=InputParam.DESCRIPTION,
                                 max_length=64,
                                 errors=errors)
    if ds_issue:
        result[MigrationIssue.Db.DS_ISSUE] = ds_issue

    return result
