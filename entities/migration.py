from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import StrEnumDesc
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final, get_args, get_origin

from app_constants import InputParam
from entities.migration_spec import MigrationSpec
from entities.migration_table import MigrationTable


# values are (min, max, default)
SPAN_BATCH_SIZE_IN: Final[tuple[int, int, int]] = (1000, 1000000, 1000000)
SPAN_BATCH_SIZE_OUT: Final[tuple[int, int, int]] = (1000, 1000000, 1000000)
SPAN_CHUNK_SIZE: Final[tuple[int, int, int]] = (1024, 16777216, 1048576)
SPAN_INCREMENTAL_SIZE: Final[tuple[int, int, int]] = (1000, 10000000, 100000)
SPAN_LOBDATA_CHANNELS: Final[tuple[int, int, int]] = (1, 128, 1)
SPAN_LOBDATA_CHANNEL_SIZE: Final[tuple[int, int, int]] = (1000, 100000, 10000)
SPAN_PLAINDATA_CHANNELS: Final[tuple[int, int, int]] = (1, 128, 1)
SPAN_PLAINDATA_CHANNEL_SIZE: Final[tuple[int, int, int]] = (10000, 1000000, 100000)


class MigStep(StrEnumDesc):
    """
    Steps for migration.
    """
    CORRELATE_LOBDATA = ("CL", "correlate-lobdata")
    CORRELATE_PLAINDATA = ("CP", "correlate-plaindata")
    MIGRATE_LOBDATA = ("ML", "migrate-lobdata")
    MIGRATE_METADATA = ("MM", "migrate-metadata")
    MIGRATE_PLAINDATA = ("MP", "migrate-plaindata")
    SYNCHRONIZE_LOBDATA = ("SL", "synchronize-lobdata")
    SYNCHRONIZE_PLAINDATA = ("SP", "synchronize-plaindata")


class MigMetric(StrEnum):
    """
    Metrics for migration.
    """
    BATCH_SIZE_IN = "batch-size-in"
    BATCH_SIZE_OUT = "batch-size-out"
    CHUNK_SIZE = "chunk-size"
    INCREMENTAL_SIZE = "incremental-size"
    LOBDATA_CHANNELS = "lobdata-channels"
    LOBDATA_CHANNEL_SIZE = "lobdata-channel-size"
    PLAINDATA_CHANNELS = "plaindata-channels"
    PLAINDATA_CHANNEL_SIZE = "plaindata-channel-size"


class Migration(PySob):
    """
    Entity *Migration*.
    """
    class Db(StrEnum):
        TABLE = "migration"
        ID = auto()
        CD_STEP = auto()
        ID_SESSION = auto()
        NM_BADGE = auto()
        NR_BATCH_SIZE_IN = auto()
        NR_BATCH_SIZE_OUT = auto()
        NR_CHUNK_SIZE = auto()
        NR_INCREMENTAL_SIZE = auto()
        TS_START = auto()
        TS_FINISH = auto()

    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_STEP: MigStep
    }
    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_SESSION, Db.CD_STEP)
    ]
    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 __references: type[MigrationSpec | MigrationTable] = None,
                 /,
                 id_session: int = None,
                 cd_step: MigStep = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_session: int | None = None
        self.nm_badge: str | None = None
        self.nr_batch_size_in: int = SPAN_BATCH_SIZE_IN[2]
        self.nr_batch_size_out: int = SPAN_BATCH_SIZE_OUT[2]
        self.nr_chunk_size: int = SPAN_CHUNK_SIZE[2]
        self.nr_incremental_size: int = SPAN_INCREMENTAL_SIZE[2]

        # nullables in DB
        self.ts_start: datetime | None = None
        self.ts_finish: datetime | None = None

        # references (lists)
        self.__migration_specs: list[MigrationSpec] | None = None
        self.__id_migration_specs: int | None = None
        self.__migration_tables: list[MigrationTable] | None = None
        self.__id_migration_tables: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {Migration.Db.ID: __id}
        elif id_session and cd_step:
            where_data = {Migration.Db.ID_SESSION: id_session,
                          Migration.Db.CD_STEP: cd_step}

        super().__init__(__references,
                         where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def get_migration_specs(self,
                            db_conn: Any = None,
                            committable: bool = None,
                            errors: list[str] = None) -> list[MigrationSpec] | None:

        self.load_references(list[MigrationSpec],
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_specs

    def get_migration_tables(self,
                             db_conn: Any = None,
                             committable: bool = None,
                             errors: list[str] = None) -> list[MigrationTable] | None:

        self.load_references(list[MigrationTable],
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_tables

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
                if not errors and cls is MigrationSpec:
                    if not self.id:
                        self.__migration_specs = None
                        self.__id_migration_specs = None
                    elif self.__id_migration_specs != self.id:
                        self.__migration_specs = MigrationSpec.retrieve(
                            where_data={MigrationSpec.Db.ID_MIGRATION: self.id},
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_specs = self.id

                if not errors and cls is MigrationTable:
                    if not self.id:
                        self.__migration_tables = None
                        self.__id_migration_tables = None
                    elif self.__id_migration_tables != self.id:
                        self.__migration_tables = MigrationTable.retrieve(
                            where_data={MigrationTable.Db.ID_MIGRATION: self.id},
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_tables = self.id


Migration.initialize(db_specs=(Migration.Db, int),
                     logger=Migration.LOGGER)
