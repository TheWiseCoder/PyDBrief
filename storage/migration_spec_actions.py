from pypomes_core import (
    validate_bool, validate_int, validate_ints,
    validate_str, validate_strs, validate_format_error
)
from pypomes_db import db_connect, db_commit, db_rollback, db_close
from typing import Any

from app_constants import PYDB_DB_ENGINE, InputParam, OpType
from entities.migration import Migration
from entities.migration_spec import MigrationSpec, MigSpec, MigSpecType


def update_migration_spec(input_params: dict[str, Any],
                          errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_spec_params: dict[str, Any] = \
            __validate_input(input_params=input_params,
                             valid_params=[i.value for i in MigSpec] + [InputParam.MIGRATION_BADGE],
                             op=OpType.UPDATE,
                             db_conn=db_conn,
                             errors=errors)
        if not errors:
            # create and persist the migration specs
            migration_spec: MigrationSpec
            id_migration: int = migration_spec_params.pop(MigrationSpec.Db.ID_MIGRATION)
            for mig_spec, spec_value in migration_spec_params.items():
                vl_spec: str = str(spec_value)
                if vl_spec.startswith("[") and vl_spec.endswith("]"):
                    vl_spec = vl_spec[1:len(vl_spec)]
                if MigrationSpec.exists(where_data={MigrationSpec.Db.ID_MIGRATION: id_migration,
                                                    MigrationSpec.Db.CD_SPEC: mig_spec},
                                        db_engine=PYDB_DB_ENGINE,
                                        db_conn=db_conn,
                                        errors=errors):
                    migration_spec = MigrationSpec(id_migration=id_migration,
                                                   cd_spec=MigSpec.from_value(mig_spec),
                                                   db_engine=PYDB_DB_ENGINE,
                                                   db_conn=db_conn,
                                                   errors=errors)
                    if vl_spec:
                        migration_spec.vl_spec = vl_spec
                        migration_spec.update(db_engine=PYDB_DB_ENGINE,
                                              db_conn=db_conn,
                                              errors=errors)
                    else:
                        migration_spec.delete(db_engine=PYDB_DB_ENGINE,
                                              db_conn=db_conn,
                                              errors=errors)
                elif not errors and vl_spec:
                    migration_spec = MigrationSpec()
                    migration_spec.id_migration = id_migration
                    # noinspection PyTypeChecker
                    migration_spec.cd_spec = mig_spec
                    migration_spec.vl_spec = vl_spec
                    migration_spec.insert(db_engine=PYDB_DB_ENGINE,
                                          db_conn=db_conn,
                                          errors=errors)
                if errors:
                    break

            # conclude the operation
            if errors:
                db_rollback(connection=db_conn,
                            engine=PYDB_DB_ENGINE)
            else:
                db_commit(connection=db_conn,
                          engine=PYDB_DB_ENGINE,
                          errors=errors)
            db_close(connection=db_conn,
                     engine=PYDB_DB_ENGINE)


def delete_migration_spec(input_params: dict[str, Any],
                          errors: list[str]) -> None:

    # obtain DB connection
    db_conn: Any = db_connect(engine=PYDB_DB_ENGINE,
                              errors=errors)
    if db_conn:
        # validate the input data
        migration_spec_params: dict[str, Any] = \
            __validate_input(input_params=input_params,
                             valid_params=[InputParam.MIGRATION_BADGE, InputParam.MIGRATION_SPECS],
                             op=OpType.DELETE,
                             db_conn=db_conn,
                             errors=errors)
        if not errors:
            # delete the migration specs
            migration_spec: MigrationSpec
            id_migration: int = migration_spec_params.get(MigrationSpec.Db.ID_MIGRATION)
            mig_specs: list[str] = migration_spec_params.get(InputParam.MIGRATION_SPECS)
            MigrationSpec.erase(where_data={MigrationSpec.Db.ID_MIGRATION: id_migration,
                                            MigrationSpec.Db.CD_SPEC: mig_specs},
                                db_engine=PYDB_DB_ENGINE,
                                db_conn=db_conn)
            # conclude the operation
            if errors:
                db_rollback(connection=db_conn,
                            engine=PYDB_DB_ENGINE)
            else:
                db_commit(connection=db_conn,
                          engine=PYDB_DB_ENGINE,
                          errors=errors)
            db_close(connection=db_conn,
                     engine=PYDB_DB_ENGINE)


def __validate_input(input_params: dict[str, Any],
                     valid_params: list[str],
                     op: OpType,
                     db_conn: Any,
                     errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {}

    # verify the input attributes
    errors.extend([validate_format_error(122,
                                         f"@{key}")
                   for key in input_params if key not in valid_params])

    nm_badge: str = validate_str(source=input_params,
                                 attr=InputParam.MIGRATION_BADGE,
                                 max_length=64,
                                 required=True,
                                 errors=errors)
    if nm_badge:
        values: list[int] = Migration.get_values(attrs=Migration.Db.ID,
                                                 where_data={Migration.Db.NM_BADGE: nm_badge},
                                                 min_count=1,
                                                 max_count=1,
                                                 db_engine=PYDB_DB_ENGINE,
                                                 db_conn=db_conn,
                                                 errors=errors)
        if values:
            result[MigrationSpec.Db.ID_MIGRATION] = values[0]

    mig_specs: list[str] = validate_strs(source=input_params,
                                         attr=InputParam.MIGRATION_SPECS,
                                         required=op == OpType.DELETE,
                                         min_length=1,
                                         errors=errors)
    if mig_specs:
        for mig_spec in mig_specs:
            if MigSpec.from_value(mig_spec) is None:
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(142,
                                                    mig_spec,
                                                    "not a valid parameter for migration",
                                                    f"@{InputParam.MIGRATION_SPECS}"))
        if not errors:
            result[InputParam.MIGRATION_SPECS] = mig_specs

    if not errors and op == OpType.UPDATE:
        for mig_spec in MigSpec:
            curr_errors: list[str] = []
            value: Any = None
            match mig_spec.anyval:
                case MigSpecType.BOOL:
                    value = validate_bool(source=input_params,
                                          attr=mig_spec,
                                          errors=curr_errors)
                case MigSpecType.INT:
                    value = validate_int(source=input_params,
                                         attr=mig_spec,
                                         errors=curr_errors)
                case MigSpecType.STR:
                    value = validate_str(source=input_params,
                                         attr=mig_spec,
                                         errors=curr_errors)
                case MigSpecType.LIST_STR:
                    value = validate_strs(source=input_params,
                                          attr=mig_spec,
                                          errors=curr_errors)
                case MigSpecType.LIST_INT:
                    value = validate_ints(source=input_params,
                                          attr=mig_spec,
                                          errors=curr_errors)
            if curr_errors:
                errors.extend(curr_errors)
            elif value is not None:
                result[mig_spec] = value

    return result
