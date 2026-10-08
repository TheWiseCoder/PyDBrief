from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import TZ_LOCAL
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final, get_args, get_origin

from app_constants import PYDB_DB_ENGINE, InputParam, MigStep
from entities.migration_span import MigrationSpan


class MigrationWork(PySob):
    """
    Entity *MigrationWork*.
    """
    class Db(StrEnum):
        TABLE = "migration_work"
        ID = auto()
        CD_STEP = auto()
        ID_MIGRATION = auto()
        NM_TABLE = auto()
        NR_DURATION_MILLIS = auto()
        NR_ROW_COUNT = auto()
        TS_START = auto()

    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.ROW_COUNT, Db.NR_ROW_COUNT),
        (InputParam.STEP, Db.CD_STEP),
        (InputParam.TABLE, Db.NM_TABLE),
        (InputParam.BADGE, None)
    ]
    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_MIGRATION, Db.CD_STEP, Db.NM_TABLE)
    ]
    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_STEP: MigStep
    }
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __references: type[list[MigrationSpan]] = None,
                 __id: int = None,
                 /,
                 id_migration: int = None,
                 cd_step: MigStep = None,
                 nm_table: str = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_migration: int | None = None
        self.cd_step: MigStep | None = None
        self.nm_table: str | None = None
        self.nr_duration_millis: int = 0
        self.nr_row_count: int = 0
        self.ts_start: datetime = datetime.now(tz=TZ_LOCAL)

        # references (lists)
        self.__migration_spans: list[MigrationSpan] | None = None
        self.__id_migration_spans: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationWork.Db.ID: __id}
        elif id_migration and cd_step and nm_table:
            where_data = {MigrationWork.Db.ID_MIGRATION: id_migration,
                          MigrationWork.Db.CD_STEP: cd_step,
                          MigrationWork.Db.NM_TABLE: nm_table}

        super().__init__(__references,
                         where_data=where_data,
                         db_engine=db_engine,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def get_migration_spans(self,
                            refresh: bool = False,
                            db_engine: DbEngine | str = PYDB_DB_ENGINE,
                            db_conn: Any = None,
                            committable: bool = None,
                            errors: list[str] = None):
        if refresh:
            self.__id_migration_spans = None
        self.load_references(list[MigrationSpan],
                             db_engine=db_engine,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_spans

    def load_references(self,
                        __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                        /,
                        db_engine: DbEngine | str = PYDB_DB_ENGINE,
                        db_conn: Any = None,
                        committable: bool = None,
                        errors: list[str] = None) -> None:

        if not isinstance(errors, list):
            errors = []
        for reference in __references if isinstance(__references, list) else [__references]:
            cls: type = get_origin(tp=reference) or reference
            if not errors and cls is list:
                cls = get_args(tp=reference)[0]
                if not errors and cls is MigrationSpan:
                    if not self.id:
                        self.__migration_spans = None
                        self.__id_migration_spans = None
                    elif self.__id_migration_spans != self.id:
                        self.__migration_spans = MigrationSpan.get_instances(
                            where_data={MigrationSpan.Db.ID_MIGRATION_WORK: self.id},
                            db_engine=db_engine,
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_spans = self.id


MigrationWork.initialize(db_specs=(MigrationWork.Db, int),
                         attrs_enum=MigrationWork.ATTRS_ENUM,
                         attrs_unique=MigrationWork.ATTRS_UNIQUE,
                         attrs_input=MigrationWork.ATTRS_INPUT,
                         logger=MigrationWork.LOGGER)
