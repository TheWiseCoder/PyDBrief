from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import StrEnumAny
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_consts import PYDB_DB_ENGINE, InputParam


class MigSpec(StrEnumAny):
    """
    Spec keys for migration.
    """
    EXCLUDE_COLUMNS = (InputParam.EXCLUDE_COLUMNS, list[str])
    EXCLUDE_CONSTRAINTS = (InputParam.EXCLUDE_CONSTRAINTS, list[str])
    EXCLUDE_RELATIONS = (InputParam.INCLUDE_RELATIONS, list[str])
    FLATTEN_STORAGE = (InputParam.FLATTEN_STORAGE, bool)
    INCLUDE_RELATIONS = (InputParam.INCLUDE_RELATIONS, list[str])
    INCREMENTAL_MIGRATIONS = (InputParam.INCREMENTAL_MIGRATIONS, list[str])
    NAMED_LOBDATA = (InputParam.NAMED_LOBDATA, list[str])
    OMIT_DEFAULTS = (InputParam.OMIT_DEFAULTS, list[str])
    OPTIMIZE_PKS =  (InputParam.OPTIMIZE_PKS, bool)
    OVERRIDE_COLUMNS = (InputParam.OVERRIDE_COLUMNS, list[str])
    PROCESS_INDEXES = (InputParam.PROCESS_INDEXES, bool),
    PROCESS_VIEWS = (InputParam.PROCESS_VIEWS, bool)
    REFLECT_FILETYPE = (InputParam.REFLECT_FILETYPE, bool)
    RELAX_REFLECTION = (InputParam.RELAX_REFLECTION, bool)
    REMOVE_CTRLCHARS = (InputParam.REMOVE_CTRLCHARS, list[str])
    SKIP_NONEMPTY = (InputParam.SKIP_NONEMPTY, bool)


class MigrationSpec(PySob):
    """
    Entity *MigrationSpec*.
    """
    class Db(StrEnum):
        TABLE = "migration_spec"
        ID = auto()
        ID_MIGRATION = auto()
        CD_SPEC = auto()
        VL_SPEC = auto()

    ATTRS_ENUM: Final[dict[Db, type[StrEnum]]] = {
        Db.CD_SPEC: MigSpec
    }
    ATTRS_UNIQUE: Final[list[tuple[Db]]] = [
        (Db.ID_MIGRATION, Db.CD_SPEC)
    ]
    LOGGER: Final[Logger] = PYPOMES_LOGGER

    def __init__(self,
                 __id: int = None,
                 /,
                 id_migration: int = None,
                 cd_spec: MigSpec = None,
                 db_engine: DbEngine | str = PYDB_DB_ENGINE,
                 db_conn: Any = None,
                 committable: bool = None,
                 errors: list[str] = None) -> None:

        # non-nullables in DB
        self.id_migration: int | None = None
        self.cd_spec: MigSpec | None = None

        # nullables in DB
        self.vl_spec: str | None = None

        where_data: dict[str, Any] | None = None
        if __id:
            where_data = {MigrationSpec.Db.ID: __id}
        elif id_migration and cd_spec:
            where_data = {MigrationSpec.Db.ID_MIGRATION: id_migration,
                          MigrationSpec.Db.CD_SPEC: cd_spec}

        super().__init__(where_data=where_data,
                         db_engine=db_engine,
                         db_conn=db_conn,
                         committable=committable,
                         errors=errors)


MigrationSpec.initialize(db_specs=(MigrationSpec.Db, int),
                         attrs_enum=MigrationSpec.ATTRS_ENUM,
                         attrs_unique=MigrationSpec.ATTRS_UNIQUE,
                         logger=MigrationSpec.LOGGER)
