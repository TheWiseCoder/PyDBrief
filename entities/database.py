from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_constants import InputParam


class DbEngine(StrEnum):
    """
    Possible database engines.
    """
    POSTGRES = auto()
    ORACLE = auto()
    SQLSERVER = auto()
    MYSQL = auto()


class Database(PySob):
    """
    Entity *Database*.
    """
    class Db(StrEnum):
        TABLE = "database"
        ID = auto()
        CD_ENGINE = auto()
        CD_NAME = auto()
        CD_TYPE = auto()
        DS_DRIVER = auto()
        DS_VERSION = auto()
        NM_CLIENT = auto()
        NM_HOST = auto()
        NM_PWD = auto()
        NM_USER = auto()
        NR_PORT = auto()

    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_TYPE: DbEngine
    }
    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.CD_ENGINE,)
    ]
    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.DB_DRIVER, Db.DS_DRIVER),
        (InputParam.DB_CLIENT, Db.NM_CLIENT),
        (InputParam.DB_ENGINE, Db.CD_ENGINE),
        (InputParam.DB_HOST, Db.NM_HOST),
        (InputParam.DB_NAME, Db.CD_NAME),
        (InputParam.DB_PORT, Db.NR_PORT),
        (InputParam.DB_PWD, Db.NM_PWD),
        (InputParam.DB_TYPE, Db.CD_TYPE),
        (InputParam.DB_USER, Db.NM_USER)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 db_engine: str = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.cd_engine: str | None = None
        self.cd_name: str | None = None
        self.cd_type: DbEngine | None = None
        self.nm_host: str | None = None
        self.nm_pwd: str | None = None
        self.nm_user: str | None = None
        self.nr_port: int | None = None

        # nullables in DB
        self.ds_driver: str | None = None
        self.ds_version: str | None = None
        self.nm_client: str | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {Database.Db.ID: __id}
        elif db_engine:
            where_data = {Database.Db.CD_ENGINE: db_engine}

        super().__init__(where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)


Database.initialize(db_specs=(Database.Db, int),
                    attrs_enum=Database.ATTRS_ENUM,
                    attrs_unique=Database.ATTRS_UNIQUE,
                    attrs_input=Database.ATTRS_INPUT,
                    logger=Database.LOGGER)
