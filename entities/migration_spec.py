from __future__ import annotations  # allow forward references
from enum import StrEnum, auto
from logging import Logger
from pypomes_core import StrEnumAny
from pypomes_db import DbEngine
from pypomes_logging import PYPOMES_LOGGER
from pypomes_sob import PySob
from typing import Any, Final

from app_consts import PYDB_DB_ENGINE


class MigSpec(StrEnumAny):
    """
    Spec keys for migration.
    """
    EXCLUDE_COLUMNS = ("exclude-columns", list[str])
    EXCLUDE_CONSTRAINTS = ("exclude-constraints", list[str])
    EXCLUDE_RELATIONS = ("exclude-relations", list[str])
    FLATTEN_STORAGE = ("flatten-storage", bool)
    INCLUDE_RELATIONS = ("include-relations", list[str])
    INCREMENTAL_MIGRATIONS = ("incremental-migrations", list[str])
    NAMED_LOBDATA = ("named-lobdata", list[str])
    OMIT_DEFAULTS = ("omit-defaults", list[str])
    OPTIMIZE_PKS = ("optimize-pks", bool)
    OVERRIDE_COLUMNS = ("override-columns", list[str])
    PROCESS_INDEXES = ("process-indexes", bool),
    PROCESS_VIEWS = ("process-views", bool)
    REFLECT_FILETYPE = ("reflect-filetype", bool)
    RELAX_REFLECTION = ("relax-reflection", bool)
    REMOVE_CTRLCHARS = ("remove-ctrlchars", list[str])
    SKIP_NONEMPTY = ("skip-nonempty", bool)


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
