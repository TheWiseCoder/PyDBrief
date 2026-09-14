from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_consts import PYDB_DB_ENGINE


class MigrationSpan(PySob):
    """
    Entity *MigrationSpan*.
    """
    class Db(StrEnum):
        TABLE = "migration_span"
        ID = auto()
        ID_MIGRATION_TABLE = auto()
        IS_FINISHED = auto()
        NR_FIRST_ROW = auto()
        NR_LAST_ROW = auto()

    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_MIGRATION_TABLE, Db.NR_FIRST_ROW)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 id_migration_table: int = None,
                 nr_first_row: int = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_migration_table: int | None = None
        self.is_finished: bool = False
        self.nr_first_row: int | None = None
        self.nr_last_row: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationSpan.Db.ID: __id}
        elif id_migration_table and isinstance(nr_first_row, int):
            where_data = {MigrationSpan.Db.ID_MIGRATION_TABLE: id_migration_table,
                          MigrationSpan.Db.NR_FIRST_ROW: nr_first_row}

        super().__init__(where_data=where_data,
                         db_engine=db_engine,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)


MigrationSpan.initialize(db_specs=(MigrationSpan.Db, int),
                         attrs_unique=MigrationSpan.ATTRS_UNIQUE,
                         logger=MigrationSpan.LOGGER)
