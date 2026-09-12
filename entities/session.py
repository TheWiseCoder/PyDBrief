from __future__ import annotations  # allow forward references
from datetime import datetime
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import TZ_LOCAL
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final, get_args, get_origin

from entities.database import Database
from entities.migration import Migration
from entities.migration_table import MigrationTable
from entities.s3 import S3


class SessionState(StrEnum):
    """
    Possible states for a migration session.
    """
    CREATED = auto()
    ACTIVE = auto()
    INACTIVE = auto()
    MIGRATING = auto()
    ABORTING = auto()
    ABORTED = auto()
    FINISHED = auto()


class Session(PySob):
    """
    Entity *Session*.
    """
    class Db(StrEnum):
        TABLE = "session"
        ID = auto()
        CD_SESSION = auto()
        ID_SOURCE_DB = auto()
        ID_TARGET_DB = auto()
        ID_TARGET_S3 = auto()
        NM_SOURCE_SCHEMA = auto()
        NM_TARGET_SCHEMA = auto()
        TS_CREATION = auto()

    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.CD_SESSION,)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 __references: type[Database | Migration] = None,
                 /,
                 nm_session: str = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.nm_source_schema: str | None = None
        self.nm_target_schema: str | None = None
        self.ts_creation: datetime | None = datetime.now(tz=TZ_LOCAL)

        # nullables in DB
        self.id_source_db: int | None = None
        self.id_target_db: int | None = None
        self.id_target_s3: int | None = None

        # references (scalars)
        self.__source_db: Database | None = None
        self.__target_db: Database | None = None
        self.__target_s3: S3 | None = None

        # references (lists)
        self.__active_migrations: list[Migration] | None = None
        self.__id_active_migrations: int | None = None
        self.__all_migrations: list[Migration] | None = None
        self.__id_all_migrations: int | None = None

        # transients
        self.__flag_active: bool = True
        self.__flag_source: bool = True

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {Session.Db.ID: __id}
        elif nm_session:
            where_data = {Session.Db.CD_SESSION: nm_session}

        super().__init__(__references,
                         where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def is_active(self,
                  db_conn: Any = None,
                  committable: bool = None,
                  errors: list[str] = None) -> bool:

        self.__flag_active = True
        self.load_references(list[Migration],
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return isinstance(self.__active_migrations, list) and len(self.__active_migrations) > 0

    def get_source_db(self,
                      db_conn: Any = None,
                      committable: bool = None,
                      errors: list[str] = None) -> Database | None:

        self.__flag_source = True
        self.load_references(Database,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__source_db

    def get_target_db(self,
                      db_conn: Any = None,
                      committable: bool = None,
                      errors: list[str] = None) -> Database | None:

        self.__flag_source = False
        self.load_references(Database,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__target_db

    def get_target_s3(self,
                      db_conn: Any = None,
                      committable: bool = None,
                      errors: list[str] = None) -> S3 | None:

        self.load_references(S3,
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__target_s3

    def get_active_migrations(self,
                              db_conn: Any = None,
                              committable: bool = None,
                              errors: list[str] = None) -> list[Migration] | None:

        self.__flag_active = True
        self.load_references(list[Migration],
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__active_migrations

    def get_all_migrations(self,
                           db_conn: Any = None,
                           committable: bool = None,
                           errors: list[str] = None):

        self.__flag_active = False
        self.load_references(list[Migration],
                             db_conn=db_conn,
                             committable=committable,
                             errors=errors)
        return self.__all_migrations

    def load_references(self,
                        # HAZARD: may fail on direct external invocations
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
            if not errors and cls is Database:
                # Note: 'self.__flag_source' must have been properly set
                if self.__flag_source:
                    if not self.id_source_db:
                        self.__source_db = None
                    elif not (self.__source_db and
                              self.__source_db.id == self.id_source_db):
                        self.__source_db = Database(self.id_source_db,
                                                    db_conn=db_conn,
                                                    committable=committable,
                                                    errors=errors)
                else:
                    if not self.id_target_db:
                        self.__target_db = None
                    elif not (self.__target_db and
                              self.__target_db == self.id_target_db):
                        self.__target_db = Database(self.id_target_db,
                                                    db_conn=db_conn,
                                                    committable=committable,
                                                    errors=errors)
            if not errors and cls is S3:
                if not self.id_target_s3:
                    self.__target_s3 = None
                elif not (self.__target_s3 and
                          self.__target_s3.id == self.id_target_s3):
                    self.__target_s3 = S3(self.id_target_s3,
                                          db_conn=db_conn,
                                          committable=committable,
                                          errors=errors)
            if not errors and cls is list:
                cls = get_args(tp=reference)[0]
                if not errors and cls is Migration:
                    if self.__flag_active:
                        if not self.id:
                            self.__active_migrations = None
                            self.__id_active_migrations = None
                        elif self.__id_active_migrations != self.id:
                            self.__active_migrations = Migration.retrieve(
                                where_data={Migration.Db.ID_SESSION: self.id,
                                            Migration.Db.TS_START: None},
                                db_conn=db_conn,
                                committable=committable,
                                errors=errors)
                            if not errors:
                                self.__id_active_migrations = self.id
                    else:
                        if not self.id:
                            self.__all_migrations = None
                            self.__id_all_migrations = None
                        elif self.__id_all_migrations != self.id:
                            self.__all_migrations = Migration.retrieve(
                                where_data={Migration.Db.ID_SESSION: self.id},
                                db_conn=db_conn,
                                committable=committable,
                                errors=errors)
                            if not errors:
                                self.__id_all_migrations = self.id

    @staticmethod
    def get_active_sessions(db_conn: Any = None,
                            committable: bool = None,
                            errors: list[str] = None) -> list[Session]:

        from entities.migration_span import MigrationSpan
        from entities.migration_spec import MigrationSpec

        result: list[Session] | None = None

        # make sure 'errors' is a list
        if errors is None:
            errors = []

        # retrieve only sessions with at least one active migration
        sessions: list[Session] = Session.retrieve(
            joins=[(Migration, (Session.Db.ID, Migration.Db.ID_SESSION))],
            where_data={f"{Migration.get_alias()}.{Migration.Db.TS_FINISH}": None},
            db_conn=db_conn,
            committable=committable,
            errors=errors)

        # make sure lists of active migrations are filled
        for session in sessions or []:
            migrations: list[Migration] = session.get_active_migrations(errors=errors)
            if errors:
                break
            # make sure lists of migration specs and active tables are filled
            for migration in migrations:
                migration.load_references(list[MigrationSpec],
                                          errors=errors)
                if errors:
                    break
                _migration_tables: list[MigrationTable] = migration.get_active_tables([MigrationSpan],
                                                                                      errors=errors)
                if errors:
                    break
        if not errors:
            result = sessions

        return result


Session.initialize(db_specs=(Session.Db, int),
                   attrs_unique=Session.ATTRS_UNIQUE,
                   logger=Session.LOGGER)
