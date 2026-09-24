from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_constants import PYDB_DB_ENGINE, InputParam


# values are (min, max, default)
SPAN_BATCH_SIZE_IN: Final[tuple[int, int, int]] = (1000, 1000000, 1000000)
SPAN_BATCH_SIZE_OUT: Final[tuple[int, int, int]] = (1000, 1000000, 1000000)
SPAN_CHUNK_SIZE: Final[tuple[int, int, int]] = (1024, 16777216, 1048576)


class MigrationTable(PySob):
    """
    Entity *MigrationTable*.
    """
    class Db(StrEnum):
        TABLE = "migration_table"
        ID = auto()
        ID_MIGRATION = auto()
        DS_EXCLUDE_COLUMNS = auto()
        DS_EXCLUDE_CONSTRAINTS = auto()
        DS_NAMED_LOBDATA = auto()
        DS_OMIT_DEFAULTS = auto()
        DS_OVERRIDE_COLUMNS = auto()
        IS_REMOVE_CTRLCHARS = auto()
        NM_TABLE = auto()
        NR_BATCH_SIZE_IN = auto()
        NR_BATCH_SIZE_OUT = auto()
        NR_CHUNK_SIZE = auto()
        NR_INCREMENTAL_COUNT = auto()
        NR_INCREMENTAL_OFFSET = auto()

    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_MIGRATION, Db.NM_TABLE)
    ]
    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.BATCH_SIZE_IN, Db.NR_BATCH_SIZE_IN),
        (InputParam.BATCH_SIZE_OUT, Db.NR_BATCH_SIZE_OUT),
        (InputParam.CHUNK_SIZE, Db.NR_CHUNK_SIZE),
        (InputParam.EXCLUDE_COLUMNS, Db.DS_EXCLUDE_COLUMNS),
        (InputParam.EXCLUDE_CONSTRAINTS, Db.DS_EXCLUDE_CONSTRAINTS),
        (InputParam.NAMED_LOBDATA, Db.DS_NAMED_LOBDATA),
        (InputParam.OMIT_DEFAULTS, Db.DS_OMIT_DEFAULTS),
        (InputParam.OVERRIDE_COLUMNS, Db.DS_OVERRIDE_COLUMNS),
        (InputParam.REMOVE_CTRLCHARS, Db.IS_REMOVE_CTRLCHARS),
        (InputParam.TABLE, Db.NM_TABLE),
        (InputParam.BADGE, None)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 id_migration: int = None,
                 nm_table: str = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_session: int | None = None
        self.nm_table: str | None = None

        # nullables in DB
        self.ds_exclude_columns: str | None = None
        self.ds_exclude_constraints: str | None = None
        self.ds_named_lobdata: str | None = None
        self.ds_omit_defaults: str | None = None
        self.ds_override_columns: str | None = None
        self.is_remove_ctrlchars: bool | None = None
        self.nr_batch_size_in: int | None = None
        self.nr_batch_size_out: int | None = None
        self.nr_chunk_size: int | None = None
        self.nr_incremental_count: int | None = None
        self.nr_incremental_offset: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationTable.Db.ID: __id}
        elif id_migration and nm_table:
            where_data = {MigrationTable.Db.ID_MIGRATION: id_migration,
                          MigrationTable.Db.NM_TABLE: nm_table}

        super().__init__(db_engine=db_engine,
                         where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)


MigrationTable.initialize(db_specs=(MigrationTable.Db, int),
                          attrs_input=MigrationTable.ATTRS_INPUT,
                          attrs_unique=MigrationTable.ATTRS_UNIQUE,
                          logger=MigrationTable.LOGGER)
