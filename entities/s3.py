from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_constants import InputParam


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
        CD_ENGINE = auto()
        CD_TYPE = auto()
        DS_ENDPOINT_URL = auto()
        DS_VERSION = auto()
        IS_SECURE_ACCESS = auto()
        NM_ACCESS_KEY = auto()
        NM_BUCKET = auto()
        NM_SECRET_KEY = auto()

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
        (InputParam.S3_SECRET_KEY, Db.NM_SECRET_KEY),
        (InputParam.S3_SECURE_ACCESS, Db.IS_SECURE_ACCESS),
        (InputParam.S3_TYPE, Db.CD_TYPE),
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 cd_engine: str | None = None,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.cd_engine: str | None = None
        self.cd_type: S3Engine | None = None
        self.ds_endpoint_url: str | None = None
        self.is_secure_access: bool = False
        self.nm_access_key: str | None = None
        self.nm_bucket: str | None = None
        self.nm_secret_key: str | None = None

        # nullables in DB
        self.ds_version: str | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {S3.Db.ID: __id}
        elif cd_engine:
            where_data = {S3.Db.CD_ENGINE: cd_engine}

        super().__init__(where_data=where_data,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)


S3.initialize(db_specs=(S3.Db, int),
              attrs_enum=S3.ATTRS_ENUM,
              attrs_unique=S3.ATTRS_UNIQUE,
              attrs_input=S3.ATTRS_INPUT,
              logger=S3.LOGGER)
