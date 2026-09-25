from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final, get_args, get_origin

from app_constants import PYDB_DB_ENGINE
from entities.migration_span import MigrationSpan


class MigrationWork(PySob):
    """
    Entity *MigrationTableWork*.
    """
    class Db(StrEnum):
        TABLE = "migration_work"
        ID = auto()
        ID_MIGRATION = auto()
        IS_CREATED = auto()
        NM_TABLE = auto()
        TS_START = auto()
        TS_FINISH = auto()

    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_MIGRATION, Db.NM_TABLE)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __references: type[list[MigrationSpan]] = None,
                 __id: int = None,
                 /,
                 id_migration: int = None,
                 nm_table: str = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_migration: int | None = None
        self.nm_table: str | None = None

        # nullables in DB
        self.is_created: bool | None = None
        self.ts_start: datetime | None = None
        self.ts_finish: datetime | None = None

        # references (lists)
        self.__migration_spans: list[MigrationSpan] | None = None
        self.__id_migration_spans: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationWork.Db.ID: __id}
        elif id_migration and nm_table:
            where_data = {MigrationWork.Db.ID_MIGRATION: id_migration,
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
                         attrs_unique=MigrationWork.ATTRS_UNIQUE,
                         logger=MigrationWork.LOGGER)
