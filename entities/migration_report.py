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


class MigrationReport(PySob):
    """
    Entity *MigrationReport*.
    """
    class Db(StrEnum):
        TABLE = "migration_report"
        ID = auto()
        DS_PATH = auto()
        ID_MIGRATION = auto()
        TS_CREATION = auto()

    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.PATH, Db.DS_PATH),
        (InputParam.BADGE, None)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.ds_path: str | None = None
        self.id_migration: int | None = None
        self.ts_creation: datetime = datetime.now(tz=TZ_LOCAL)

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationReport.Db.ID: __id}

        super().__init__(where_data=where_data,
                         db_engine=db_engine,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)


MigrationReport.initialize(db_specs=(MigrationReport.Db, int),
                           attrs_input=MigrationReport.ATTRS_INPUT,
                           logger=MigrationReport.LOGGER)
