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
        valid_params: list[str] = [i[0] for i in MigrationIssue.ATTRS_INPUT]
        migration_issue_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                  valid_params=valid_params,
                                                                  op=OpType.CREATE,
                                                                  db_conn=db_conn,
                                                                  errors=errors)
        if not errors:
            # create and persist the migration issue instance
            values: list[int] = Migration.get_values(
                Migration.Db.ID,
                where_data={Migration.Db.NM_BADGE: migration_issue_params.pop(InputParam.BADGE)},
                db_engine=PYDB_DB_ENGINE,
                db_conn=db_conn,
                errors=errors)
            if values:
                migration_issue: MigrationIssue = MigrationIssue()
                migration_issue.id_migration = values[0]
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

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        valid_params: list[str] = [InputParam.CD_ISSUE] + [i[0] for i in MigrationIssue.ATTRS_INPUT]
        migration_issue_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                  valid_params=valid_params,
                                                                  op=OpType.UPDATE,
                                                                  db_conn=db_conn,
                                                                  errors=errors)
        if not errors:
            # obtain and update the migration issue instance
            migration_issue: MigrationIssue = migration_issue_params.pop(InputParam.MIGRATION_ISSUE)
            if InputParam.BADGE in migration_issue_params:
                values: list[int] = Migration.get_values(
                    Migration.Db.ID,
                    where_data={Migration.Db.NM_BADGE: migration_issue_params.pop(InputParam.BADGE)},
                    db_engine=PYDB_DB_ENGINE,
                    db_conn=db_conn,
                    errors=errors)
                if values:
                    migration_issue.id_migration = values[0]
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

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_issue_params: dict[str, Any] = __validate_input(input_params=input_params,
                                                                  valid_params=[InputParam.ISSUE_ID],
                                                                  op=OpType.DELETE,
                                                                  db_conn=db_conn,
                                                                  errors=errors)
        if not errors:
            migration_issue: MigrationIssue = migration_issue_params.pop(InputParam.MIGRATION_ISSUE)
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
                             valid_params=[InputParam.BADGE, InputParam.TYPE],
                             op=OpType.RETRIEVE,
                             db_conn=db_conn,
                             errors=errors)
        if not errors:
            values: list[int] = Migration.get_values(
                attrs=Migration.Db.ID,
                where_data={Migration.Db.NM_BADGE: migration_issue_params.get(InputParam.BADGE)},
                db_engine=PYDB_DB_ENGINE,
                db_conn=db_conn,
                errors=errors)
            if values:
                where_data: dict[str, Any] = {MigrationIssue.Db.ID_MIGRATION: values[0]}
                if InputParam.TYPE in migration_issue_params:
                    where_data[MigrationIssue.Db.CD_TYPE] = migration_issue_params[InputParam.TYPE]

                result[InputParam.MIGRATION] = migration_issue_params.get(InputParam.BADGE)
                result[InputParam.ISSUES]: list[dict[str, Any]] = []
                migration_issues: list[MigrationIssue] = MigrationIssue.retrieve(where_data=where_data,
                                                                                 db_engine=PYDB_DB_ENGINE,
                                                                                 db_conn=db_conn,
                                                                                 errors=errors)
                for migration_issue in migration_issues or []:
                    result[InputParam.ISSUES].append(migration_issue.get_inputs())

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

    # identify the migration issue instance (in UPDATE and DELETE operations)
    issue_id: int = validate_int(source=input_params,
                                 attr=InputParam.ISSUE_ID,
                                 required=op in [OpType.UPDATE, OpType.DELETE],
                                 errors=errors)
    if issue_id:
        result[InputParam.ISSUE] = MigrationIssue(issue_id,
                                                  db_engine=PYDB_DB_ENGINE,
                                                  db_conn=db_conn,
                                                  errors=errors)

    badge: str = validate_str(source=input_params,
                              attr=InputParam.BADGE,
                              required=op in [OpType.CREATE, OpType.RETRIEVE],
                              errors=errors)
    if badge:
        result[InputParam.BADGE] = badge

    # HAZARD: 'type' is a builtin name
    cd_type: IssueType = validate_enum(source=input_params,
                                       attr=InputParam.TYPE,
                                       enum_class=IssueType,
                                       required=op == OpType.CREATE,
                                       errors=errors)
    if cd_type:
        result[MigrationIssue.Db.CD_TYPE] = cd_type

    issue: str = validate_str(source=input_params,
                              attr=InputParam.ISSUE,
                              max_length=2048,
                              required=op == OpType.CREATE,
                              errors=errors)
    if issue:
        result[MigrationIssue.Db.DS_ISSUE] = issue

    return result
