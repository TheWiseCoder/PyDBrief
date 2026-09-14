from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import StrEnumAny
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_constants import PYDB_DB_ENGINE, InputParam


class MigSpecType(StrEnum):
    """
    Spec types for migration
    """
    BOOL = auto()
    INT = auto()
    STR = auto()
    LIST_INT = auto()
    LIST_STR = auto()


class MigSpec(StrEnumAny):
    """
    Spec keys for migration.
    """
    EXCLUDE_COLUMNS = (InputParam.EXCLUDE_COLUMNS.value, MigSpecType.LIST_STR)
    EXCLUDE_CONSTRAINTS = (InputParam.EXCLUDE_CONSTRAINTS.value, MigSpecType.LIST_STR)
    EXCLUDE_RELATIONS = (InputParam.INCLUDE_RELATIONS.value, MigSpecType.LIST_STR)
    FLATTEN_STORAGE = (InputParam.FLATTEN_STORAGE.value, MigSpecType.BOOL)
    INCLUDE_RELATIONS = (InputParam.INCLUDE_RELATIONS.value, MigSpecType.LIST_STR)
    INCREMENTAL_MIGRATIONS = (InputParam.INCREMENTAL_MIGRATIONS.value, MigSpecType.LIST_STR)
    NAMED_LOBDATA = (InputParam.NAMED_LOBDATA.value, MigSpecType.LIST_STR)
    OMIT_DEFAULTS = (InputParam.OMIT_DEFAULTS.value, MigSpecType.LIST_STR)
    OPTIMIZE_PKS = (InputParam.OPTIMIZE_PKS.value, MigSpecType.BOOL)
    OVERRIDE_COLUMNS = (InputParam.OVERRIDE_COLUMNS.value, MigSpecType.LIST_STR)
    PROCESS_INDEXES = (InputParam.PROCESS_INDEXES.value, MigSpecType.BOOL)
    PROCESS_VIEWS = (InputParam.PROCESS_VIEWS.value, MigSpecType.BOOL)
    REFLECT_FILETYPE = (InputParam.REFLECT_FILETYPE.value, MigSpecType.BOOL)
    RELAX_REFLECTION = (InputParam.RELAX_REFLECTION.value, MigSpecType.BOOL)
    REMOVE_CTRLCHARS = (InputParam.REMOVE_CTRLCHARS.value, MigSpecType.LIST_STR)
    SKIP_NONEMPTY = (InputParam.SKIP_NONEMPTY.value, MigSpecType.BOOL)


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
