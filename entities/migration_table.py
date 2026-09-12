from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final, get_args, get_origin

from entities.migration_span import MigrationSpan


class MigrationTable(PySob):
    """
    Entity *MigrationTable*.
    """
    class Db(StrEnum):
        TABLE = "migration_table"
        ID = auto()
        ID_MIGRATION = auto()
        NM_TABLE = auto()
        TS_START = auto()
        TS_FINISH = auto()

    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_MIGRATION, Db.NM_TABLE)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __references: type[list[MigrationSpan]],
                 __id: int = None,
                 /,
                 id_migration: int = None,
                 nm_table: str = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_migration: int | None = None
        self.nm_table: str | None = None

        # nullables in DB
        self.ts_start: datetime | None = None
        self.ts_finish: datetime | None = None

        # references (lists)
        self.__migration_spans: list[MigrationSpan] | None = None
        self.__id_migration_spans: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationTable.Db.ID: __id}
        elif id_migration and nm_table:
            where_data = {MigrationTable.Db.ID_MIGRATION: id_migration,
                          MigrationTable.Db.NM_TABLE: nm_table}

        super().__init__(where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def get_migration_spans(self,
                            db_conn: Any = None,
                            committable: bool = None,
                            errors: list[str] = None):

        self.load_references(list[MigrationSpan],
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_spans

    def load_references(self,
                        __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                        /,
                        db_engine: Any = None,  # noqa: ARG002 - unused method argument
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
                        self.__migration_spans = MigrationSpan.retrieve(
                            where_data={MigrationSpan.Db.ID_MIGRATION_TABLE: self.id},
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_spans = self.id


MigrationTable.initialize(db_specs=(MigrationTable.Db, int),
                          attrs_unique=MigrationTable.ATTRS_UNIQUE,
                          logger=MigrationTable.LOGGER)
