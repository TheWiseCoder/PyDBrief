from __future__ import annotations  # allow forward references
import sys
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import exc_format
from pypomes_crypto import crypto_decrypt, crypto_encrypt
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final

from app_constants import InputParam

ENCRYPTION_KEY: Final[bytes] = b"\x9f\x1c\xbd\x4a\x72\xeb\x0e\x39\x6d\x8a\xf1\x54\x2c\x83\x60\x1e"
#                              b"\xbb\xd7\x42\x3f\xa0\x15\x99\x6c\x4e\xd2\x7b\x5d\x88\x01\xef\xfa"


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
        BN_PWD = auto()
        CD_ENGINE = auto()
        CD_NAME = auto()
        CD_TYPE = auto()
        DS_DRIVER = auto()
        DS_VERSION = auto()
        NM_CLIENT = auto()
        NM_HOST = auto()
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
        self.bn_pwd: bytes | None = None
        self.cd_engine: str | None = None
        self.cd_name: str | None = None
        self.cd_type: DbEngine | None = None
        self.nm_host: str | None = None
        self.nm_user: str | None = None
        self.nr_port: int | None = None

        # nullables in DB
        self.ds_driver: str | None = None
        self.ds_version: str | None = None
        self.nm_client: str | None = None

        # not mapped to DB
        self.nm_pwd: str | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {Database.Db.ID: __id}
        elif db_engine:
            where_data = {Database.Db.CD_ENGINE: db_engine}

        super().__init__(where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def load(self,
             __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
             /,
             omit_nulls: bool = True,
             db_engine: DbEngine = None,
             db_conn: Any = None,
             committable: bool = None,
             errors: list[str] = None) -> bool:

        result: bool = False

        if super().load(__references,
                        omit_nulls=omit_nulls,
                        db_engine=db_engine,
                        db_conn=db_conn,
                        committable=committable,
                        errors=errors):
            plaintext: bytes = crypto_decrypt(ciphertext=self.bn_pwd,
                                              key=ENCRYPTION_KEY,
                                              errors=errors)
            if plaintext:
                try:
                    self.nm_pwd = plaintext.decode(encoding="utf-8")
                    result = True
                except UnicodeDecodeError as e:
                    if isinstance(errors, list):
                        exc_error: str = exc_format(exc=e,
                                                    exc_info=sys.exc_info())
                        errors.append(exc_error)
        return result

    def insert(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:

        result: bool = False

        self.bn_pwd = crypto_encrypt(plaintext=self.nm_pwd,
                                     key=ENCRYPTION_KEY,
                                     errors=errors)
        if not errors:
            result = super().insert(db_engine=db_engine,
                                    db_conn=db_conn,
                                    committable=committable,
                                    errors=errors)
        return result

    def update(self,
               db_engine: DbEngine = None,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:

        result: bool = False

        self.bn_pwd = crypto_encrypt(plaintext=self.nm_pwd,
                                     key=ENCRYPTION_KEY,
                                     errors=errors)
        if not errors:
            result = super().update(db_engine=db_engine,
                                    db_conn=db_conn,
                                    committable=committable,
                                    errors=errors)
        return result


Database.initialize(db_specs=(Database.Db, int),
                    attrs_enum=Database.ATTRS_ENUM,
                    attrs_unique=Database.ATTRS_UNIQUE,
                    attrs_input=Database.ATTRS_INPUT,
                    logger=Database.LOGGER)
