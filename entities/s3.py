from __future__ import annotations  # allow forward references
import sys
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import exc_format
from pypomes_crypto import crypto_decrypt, crypto_encrypt
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob, Sob
from typing import Any, Final

from app_consts import PYDB_DB_ENGINE, InputParam
from entities.database import DbEngine

ENCRYPTION_KEY: Final[bytes] = b"\x9f\x1c\xbd\x4a\x72\xeb\x0e\x39\x6d\x8a\xf1\x54\x2c\x83\x60\x1e"
#                              b"\xbb\xd7\x42\x3f\xa0\x15\x99\x6c\x4e\xd2\x7b\x5d\x88\x01\xef\xfa"


class S3Engine(StrEnum):
    """
    Possible s3 engines.
    """
    AWS = auto()
    S3 = auto()


class S3(PySob):
    """
    Entity *S3*.
    """
    class Db(StrEnum):
        TABLE = "s3"
        ID = auto()
        BN_SECRET_KEY = auto()
        CD_ENGINE = auto()
        CD_TYPE = auto()
        DS_ENDPOINT_URL = auto()
        DS_VERSION = auto()
        IS_SECURE_ACCESS = auto()
        NM_ACCESS_KEY = auto()
        NM_BUCKET = auto()

    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_TYPE: S3Engine
    }
    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.CD_ENGINE,)
    ]
    ATTRS_INPUT: Final[list[tuple[InputParam, Db]]] = [
        (InputParam.S3_ACCESS_KEY, Db.NM_ACCESS_KEY),
        (InputParam.S3_BUCKET_NAME, Db.NM_BUCKET),
        (InputParam.S3_ENDPOINT_URL, Db.DS_ENDPOINT_URL),
        (InputParam.S3_ENGINE, Db.CD_ENGINE),
        (InputParam.S3_SECURE_ACCESS, Db.IS_SECURE_ACCESS),
        (InputParam.S3_TYPE, Db.CD_TYPE),
        (InputParam.S3_SECRET_KEY, None)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 cd_engine: str | None = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.bn_secret_key: bytes | None = None
        self.cd_engine: str | None = None
        self.cd_type: S3Engine | None = None
        self.ds_endpoint_url: str | None = None
        self.is_secure_access: bool = False
        self.nm_access_key: str | None = None
        self.nm_bucket: str | None = None
        self.nm_secret_key: str | None = None

        # nullables in DB
        self.ds_version: str | None = None

        # not mapped to db
        self.nm_secret_key: str | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {S3.Db.ID: __id}
        elif cd_engine:
            where_data = {S3.Db.CD_ENGINE: cd_engine}

        super().__init__(where_data=where_data,
                         db_engine=db_engine,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)

    def load(self,
             __references: type[Sob | list[Sob]] | list[type[Sob | list[Sob]]] = None,
             /,
             omit_nulls: bool = True,
             db_engine: DbEngine | str = PYDB_DB_ENGINE,
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
            plaintext: bytes = crypto_decrypt(ciphertext=self.bn_secret_key,
                                              key=ENCRYPTION_KEY,
                                              errors=errors)
            if plaintext:
                try:
                    self.nm_secret_key = plaintext.decode(encoding="utf-8")
                    result = True
                except UnicodeDecodeError as e:
                    if isinstance(errors, list):
                        exc_error: str = exc_format(exc=e,
                                                    exc_info=sys.exc_info())
                        errors.append(exc_error)
        return result

    def insert(self,
               db_engine: DbEngine | str = PYDB_DB_ENGINE,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:

        result: bool = False

        self.bn_secret_key = crypto_encrypt(plaintext=self.nm_secret_key,
                                            key=ENCRYPTION_KEY,
                                            errors=errors)
        if not errors:
            result = super().insert(db_engine=db_engine,
                                    db_conn=db_conn,
                                    committable=committable,
                                    errors=errors)
        return result

    def update(self,
               db_engine: DbEngine | str = PYDB_DB_ENGINE,
               db_conn: Any = None,
               committable: bool = None,
               errors: list[str] = None) -> bool:

        result: bool = False

        self.bn_secret_key = crypto_encrypt(plaintext=self.nm_secret_key,
                                            key=ENCRYPTION_KEY,
                                            errors=errors)
        if not errors:
            result = super().update(db_engine=db_engine,
                                    db_conn=db_conn,
                                    committable=committable,
                                    errors=errors)
        return result


S3.initialize(db_specs=(S3.Db, int),
              attrs_enum=S3.ATTRS_ENUM,
              attrs_unique=S3.ATTRS_UNIQUE,
              attrs_input=S3.ATTRS_INPUT,
              logger=S3.LOGGER)
