from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import TZ_LOCAL
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_constants import PYDB_DB_ENGINE, InputParam


class IssueType(StrEnum):
    """
    Types of migration issues.
    """
    COMMENT = "C"
    ERROR = "E"
    WARNING = "W"


class MigrationIssue(PySob):
    """
    Entity *MigrationIssue*.
    """
    class Db(StrEnum):
        TABLE = "migration_issue"
        ID = auto()
        CD_TYPE = auto()
        DS_ISSUE = auto()
        ID_MIGRATION = auto()
        TS_ONSET = auto()

    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.ISSUE, Db.DS_ISSUE),
        (InputParam.TYPE, Db.CD_TYPE),
        (InputParam.BADGE, None)
    ]
    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_TYPE: IssueType
    }
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.cd_type: IssueType | None = None
        self.ds_issue: str | None = None
        self.id_migration: int | None = None
        self.ts_onset: datetime = datetime.now(tz=TZ_LOCAL)

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationIssue.Db.ID: __id}

        super().__init__(where_data=where_data,
                         db_engine=db_engine,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    @classmethod
    def new_issue(cls,
                  id_migration: int,
                  cd_type: IssueType,
                  ds_issue: str,
                  db_engine: DbEngine | str = PYDB_DB_ENGINE,
                  db_conn: Any = None,
                  committable: bool = None,
                  errors: list[str] = None) -> None:

        migration_issue: MigrationIssue = MigrationIssue()
        migration_issue.id_migration = id_migration
        migration_issue.cd_type = cd_type
        migration_issue.ds_issue = ds_issue
        migration_issue.ts_onset = datetime.now(tz=TZ_LOCAL)
        migration_issue.insert(db_engine=db_engine,
                               db_conn=db_conn,
                               committable=committable,
                               errors=errors)

    @classmethod
    def new_issues(cls,
                   id_migration: int,
                   cd_type: IssueType,
                   ds_issues: list[str],
                   db_engine: DbEngine | str = PYDB_DB_ENGINE,
                   db_conn: Any = None,
                   committable: bool = None,
                   errors: list[str] = None) -> None:

        curr_errors: list[str] = []
        for ds_issue in ds_issues:
            MigrationIssue.new_issue(id_migration=id_migration,
                                     cd_type=cd_type,
                                     ds_issue=ds_issue,
                                     db_engine=db_engine,
                                     db_conn=db_conn,
                                     committable=committable,
                                     errors=curr_errors)
            if curr_errors and isinstance(errors, list):
                errors.extend(curr_errors)
                break


MigrationIssue.initialize(db_specs=(MigrationIssue.Db, int),
                          attrs_enum=MigrationIssue.ATTRS_ENUM,
                          logger=MigrationIssue.LOGGER)
