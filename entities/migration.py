from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import EnumUseAny, StrEnumAny
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final, get_args, get_origin

from app_constants import PYDB_DB_ENGINE, InputParam
from entities.migration_issue import MigrationIssue
from entities.migration_report import MigrationReport
from entities.migration_table import MigrationTable
from entities.migration_span import MigrationSpan
from entities.migration_work import MigrationWork

# values are (min, max, default)
SPAN_LOBDATA_CHANNELS: Final[tuple[int, int, int]] = (1, 127, 1)
SPAN_LOBDATA_CHANNEL_SIZE: Final[tuple[int, int, int]] = (1000, 100000, 10000)
SPAN_PLAINDATA_CHANNELS: Final[tuple[int, int, int]] = (1, 127, 1)
SPAN_PLAINDATA_CHANNEL_SIZE: Final[tuple[int, int, int]] = (10000, 1000000, 100000)


class MigStep(EnumUseAny, StrEnumAny):
    """
    Steps for migration.
    """
    CORRELATE_LOBDATA = ("CL", "correlate-lobdata")
    CORRELATE_PLAINDATA = ("CP", "correlate-plaindata")
    MIGRATE_LOBDATA = ("ML", "migrate-lobdata")
    MIGRATE_METADATA = ("MM", "migrate-metadata")
    MIGRATE_PLAINDATA = ("MP", "migrate-plaindata")
    SYNCHRONIZE_PLAINDATA = ("SP", "synchronize-plaindata")


class Migration(PySob):
    """
    Entity *Migration*.
    """
    class Db(StrEnum):
        TABLE = "migration"
        ID = auto()
        CD_STEP = auto()
        DS_EXCLUDE_RELATIONS = auto()
        DS_INCLUDE_RELATIONS = auto()
        ID_SESSION = auto()
        IS_FLATTEN_STORAGE = auto()
        IS_OPTIMIZE_PKS = auto()
        IS_PROCESS_INDEXES = auto()
        IS_PROCESS_VIEWS = auto()
        IS_REFLECT_FILETYPE = auto()
        IS_RELAX_REFLECTION = auto()
        IS_SKIP_NONEMPTY = auto()
        NM_BADGE = auto()
        NR_LOBDATA_CHANNELS = auto()
        NR_LOBDATA_CHANNEL_SIZE = auto()
        NR_PLAINDATA_CHANNELS = auto()
        NR_PLAINDATA_CHANNEL_SIZE = auto()
        TS_START = auto()
        TS_FINISH = auto()

    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_STEP: MigStep
    }
    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.NM_BADGE,),
        (Db.ID_SESSION, Db.CD_STEP)
    ]
    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.BADGE, Db.NM_BADGE),
        (InputParam.STEP, Db.CD_STEP),
        (InputParam.EXCLUDE_RELATIONS, Db.DS_EXCLUDE_RELATIONS),
        (InputParam.FLATTEN_STORAGE, Db.IS_FLATTEN_STORAGE),
        (InputParam.INCLUDE_RELATIONS, Db.DS_INCLUDE_RELATIONS),
        (InputParam.LOBDATA_CHANNEL_SIZE, Db.NR_LOBDATA_CHANNEL_SIZE),
        (InputParam.LOBDATA_CHANNELS, Db.NR_LOBDATA_CHANNELS),
        (InputParam.OPTIMIZE_PKS, Db.IS_OPTIMIZE_PKS),
        (InputParam.PLAINDATA_CHANNEL_SIZE, Db.NR_PLAINDATA_CHANNEL_SIZE),
        (InputParam.PLAINDATA_CHANNELS, Db.NR_PLAINDATA_CHANNELS),
        (InputParam.PROCESS_INDEXES, Db.IS_PROCESS_INDEXES),
        (InputParam.PROCESS_VIEWS, Db.IS_PROCESS_VIEWS),
        (InputParam.REFLECT_FILETYPE, Db.IS_REFLECT_FILETYPE),
        (InputParam.RELAX_REFLECTION, Db.IS_RELAX_REFLECTION),
        (InputParam.SKIP_NONEMPTY, Db.IS_SKIP_NONEMPTY),
        (InputParam.SESSION, None)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 __references: type[list[MigrationIssue] |
                                    list[MigrationTable] | list[MigrationWork]] | list[type] = None,
                 /,
                 nm_badge: str = None,
                 id_session: int = None,
                 cd_step: MigStep = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.cd_step: MigStep | None = None
        self.id_session: int | None = None
        self.nm_badge: str | None = None

        # nullables in DB
        self.ds_exclude_relations: str | None = None
        self.ds_include_relations: str | None = None
        self.ds_omit_defaults: str | None = None
        self.is_flatten_storage: bool = False
        self.is_optimize_pks: bool = False
        self.is_process_indexes: bool = False
        self.is_process_views: bool = False
        self.is_reflect_filetype: bool = False
        self.is_relax_reflection: bool = False
        self.is_skip_nonempty: bool = False
        self.nr_lobdata_channels: int | None = None
        self.nr_lobdata_channel_size: int | None = None
        self.nr_plaindata_channels: int | None = None
        self.nr_plaindata_channel_size: int | None = None
        self.ts_start: datetime | None = None
        self.ts_finish: datetime | None = None

        # references (lists)
        self.__migration_issues: list[MigrationIssue] | None = None
        self.__id_migration_issues: int | None = None
        self.__migration_reports: list[MigrationTable] | None = None
        self.__id_migration_reports: int | None = None
        self.__migration_tables: list[MigrationTable] | None = None
        self.__id_migration_tables: int | None = None
        self.__migration_works: list[MigrationWork] | None = None
        self.__id_migration_works: int | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {Migration.Db.ID: __id}
        elif nm_badge:
            where_data = {Migration.Db.NM_BADGE: nm_badge}
        elif id_session and cd_step:
            where_data = {Migration.Db.ID_SESSION: id_session,
                          Migration.Db.CD_STEP: cd_step}

        super().__init__(__references,
                         db_engine=db_engine,
                         where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def get_migration_issues(self,
                             db_engine: DbEngine | str = PYDB_DB_ENGINE,
                             db_conn: Any = None,
                             committable: bool = None,
                             errors: list[str] = None) -> list[MigrationIssue] | None:

        self.load_references(list[MigrationIssue],
                             db_engine=db_engine,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_issues

    def get_migration_reports(self,
                              db_engine: DbEngine | str = PYDB_DB_ENGINE,
                              db_conn: Any = None,
                              committable: bool = None,
                              errors: list[str] = None) -> list[MigrationReport] | None:

        self.load_references(list[MigrationReport],
                             db_engine=db_engine,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_reports

    def get_migration_tables(self,
                             db_engine: DbEngine | str = PYDB_DB_ENGINE,
                             db_conn: Any = None,
                             committable: bool = None,
                             errors: list[str] = None) -> list[MigrationTable] | None:

        self.load_references(list[MigrationTable],
                             db_engine=db_engine,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__migration_tables

    def get_migration_works(self,
                            __references: list[type[MigrationSpan]] = None,
                            db_engine: DbEngine | str = PYDB_DB_ENGINE,
                            db_conn: Any = None,
                            committable: bool = None,
                            errors: list[str] = None) -> list[MigrationWork] | None:

        if not isinstance(errors, list):
            errors = []
        self.load_references(list[MigrationWork],
                             db_engine=db_engine,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        if not errors and __references and self.__migration_works:
            for table in self.__migration_works:
                table.load_references(__references,
                                      db_engine=db_engine,
                                      db_conn=db_conn,
                                      committable=committable,
                                      errors=errors)
        return self.__migration_works

    def load_references(self,
                        # HAZARD: may fail on direct external invocations
                        __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]],
                        /,
                        db_engine: Any = PYDB_DB_ENGINE,
                        db_conn: Any = None,
                        committable: bool = None,
                        errors: list[str] = None) -> None:

        if not isinstance(errors, list):
            errors = []
        for reference in __references if isinstance(__references, list) else [__references]:
            cls: type = get_origin(tp=reference) or reference
            if not errors and cls is list:
                cls = get_args(tp=reference)[0]
                if not errors and cls is MigrationIssue:
                    if not self.id:
                        self.__migration_issues = None
                        self.__id_migration_issues = None
                    elif self.__id_migration_issues != self.id:
                        self.__migration_issues = MigrationIssue.retrieve(
                            where_data={MigrationIssue.Db.ID_MIGRATION: self.id},
                            db_engine=db_engine,
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_issues = self.id

                if not errors and cls is MigrationReport:
                    if not self.id:
                        self.__migration_reports = None
                        self.__id_migration_reports = None
                    elif self.__id_migration_tables != self.id:
                        self.__migration_reports = MigrationReport.retrieve(
                            where_data={MigrationReport.Db.ID_MIGRATION: self.id},
                            db_engine=db_engine,
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_reports = self.id

                if not errors and cls is MigrationTable:
                    if not self.id:
                        self.__migration_tables = None
                        self.__id_migration_tables = None
                    elif self.__id_migration_tables != self.id:
                        self.__migration_tables = MigrationTable.retrieve(
                            where_data={MigrationTable.Db.ID_MIGRATION: self.id},
                            db_engine=db_engine,
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_tables = self.id

                if not errors and cls is MigrationWork:
                    if not self.id:
                        self.__migration_works = None
                        self.__id_migration_works = None
                    elif self.__id_migration_works != self.id:
                        self.__migration_works = MigrationWork.retrieve(
                            where_data={MigrationWork.Db.ID_MIGRATION: self.id},
                            db_engine=db_engine,
                            db_conn=db_conn,
                            committable=committable,
                            errors=errors)
                        if not errors:
                            self.__id_migration_works = self.id


Migration.initialize(db_specs=(Migration.Db, int),
                     attrs_enum=Migration.ATTRS_ENUM,
                     attrs_input=Migration.ATTRS_INPUT,
                     attrs_unique=Migration.ATTRS_UNIQUE,
                     logger=Migration.LOGGER)
