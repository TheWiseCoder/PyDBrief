import sys
from enum import StrEnum
from logging import Logger
from pypomes_core import (
    str_sanitize, exc_format, validate_format_error
)
from pypomes_db import db_execute
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, inspect
)
from sqlalchemy.exc import SAWarning
from typing import Any

from app_constants import (
    MigConfig, MigStep, MigSpec, MigSpot
)
from migration.pydb_sessions import get_session_registry
from migration.pydb_types import is_lob
from migration.steps.pydb_database import column_set_nullable, view_get_ddl
from migration.steps.pydb_migration import (
    prune_metadata, setup_schema, setup_tables
)
from migration.steps.pydb_engine import build_engine


def migrate_metadata(errors: list[str],
                     session_id: str,
                     # migration_warnings: list[str],
                     logger: Logger) -> dict[str, Any]:

    # iinitialize the return variable
    result: dict[str, Any] | None = None

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]
    session_steps: dict[MigSpot, Any] = session_registry[MigConfig.STEPS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]

    # create engines
    source_engine: Engine = build_engine(errors=errors,
                                         rdbms=session_spots[MigSpot.FROM_RDBMS],
                                         logger=logger)
    target_engine: Engine = build_engine(errors=errors,
                                         rdbms=session_spots[MigSpot.TO_RDBMS],
                                         logger=logger)
    # were both engines created ?
    if source_engine and target_engine:
        # yes, proceed
        from_schema: str | None = None

        # obtain the source schema's internal name
        source_inspector: Inspector = inspect(subject=source_engine,
                                              raiseerr=True)
        for schema_name in source_inspector.get_schema_names():
            # is this the source schema ?
            if session_specs[MigSpec.FROM_SCHEMA] == schema_name.lower():
                # yes, use the actual name with its case imprint
                from_schema = schema_name
                break

        # proceed, if the source schema exists
        if from_schema:
            # obtain the list of plain and materialized views in source schema
            mat_views: list[str] = source_inspector.get_materialized_view_names()
            schema_views: list[str] = source_inspector.get_view_names()
            schema_views.extend(mat_views)

            # determine if relation 'rel' is to be reflected in 'source_metadata'
            def assert_relation(rel: str, _md: MetaData) -> bool:
                result: bool = ((not session_specs[MigSpec.EXCLUDE_RELATIONS] or
                                 rel not in session_specs[MigSpec.EXCLUDE_RELATIONS]) and
                                rel not in schema_views and
                                (not session_specs[MigSpec.INCLUDE_RELATIONS] or
                                 rel in session_specs[MigSpec.INCLUDE_RELATIONS]))
                logger.debug(msg=f"Relation '{rel}' asserted '{result}' on reflection")
                return result

            # obtain the source schema metadata
            source_metadata: MetaData = MetaData(schema=from_schema)
            try:
                # HAZARD:
                # - if the parameter 'resolve_fks' is set to 'True' (the default value),
                #   then relations referenced in FK columns of included tables
                #   will also be included, regardless of parameters 'only' or 'views'
                #   (this is remedied at 'prune_metadata()')
                # - if 'resolve_fks' is ommited, not finding referenced tables will not
                #   prevent migration to continue, although SQLAlchemy will nonetheless raise
                #   a 'NoReferencedTableError' exception upon 'source_metadata.sorted_tables'
                #   retrieval, if a FK-referenced table is missing from the source schema
                # - the parameter 'views' should not be set to 'True', as no reflection is
                #   necessary for views - a view is migrated by retrieving its DDL script
                #   and executing it at the target schema
                source_metadata.reflect(bind=source_engine,
                                        schema=from_schema,
                                        only=assert_relation,
                                        resolve_fks=not session_specs[MigSpec.RELAX_REFLECTION])
            except (Exception, SAWarning) as e:
                # - unable to fully reflect the source schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                exc_err: str = str_sanitize(source=exc_format(exc=e,
                                                              exc_info=sys.exc_info()))
                logger.error(msg=exc_err)
                # 104: The operation {} returned the error {}
                errors.append(validate_format_error(104,
                                                    "schema-reflection",
                                                    exc_err))
            if not errors:
                # build list of views to migrate
                target_views: list[str] = []
                if session_specs[MigSpec.PROCESS_VIEWS]:
                    if session_specs[MigSpec.INCLUDE_RELATIONS]:
                        target_views.extend([view for view in (session_specs[MigSpec.INCLUDE_RELATIONS] or [])
                                             if view in schema_views])
                    else:
                        target_views = schema_views

                # prepare the source metadata for migration
                prune_metadata(source_schema=session_specs[MigSpec.FROM_SCHEMA],
                               source_metadata=source_metadata,
                               process_indexes=session_specs[MigSpec.PROCESS_INDEXES],
                               schema_views=schema_views,
                               include_relations=session_specs[MigSpec.INCLUDE_RELATIONS] or [],
                               exclude_relations=session_specs[MigSpec.EXCLUDE_RELATIONS] or [],
                               exclude_columns=session_specs[MigSpec.EXCLUDE_COLUMNS] or [],
                               exclude_constraints=session_specs[MigSpec.EXCLUDE_CONSTRAINTS] or [],
                               logger=logger)

                # proceed with the appropriate tables
                target_tables: list[Table] = []
                try:
                    # 'target_tables' will contain no views (as per 'prune_metadata()')
                    target_tables: list[Table] = source_metadata.sorted_tables
                except (Exception, SAWarning) as e:
                    # - unable to organize the tables in the proper sequence, probably caused by:
                    #   - cross-dependencies between tables, resulted from mutually dependent FKs, or
                    #   - a table or view referenced by a FK column was not found in the schema
                    # - this error will cause the migration to be aborted,
                    #   as SQLAlchemy would not be able to compile the migrated schema
                    exc_err: str = str_sanitize(source=exc_format(exc=e,
                                                                  exc_info=sys.exc_info()))
                    logger.error(msg=exc_err)
                    # 104: The operation {} returned the error {}
                    errors.append(validate_format_error(104,
                                                        "schema-migration",
                                                        exc_err))
                # errors ?
                if not errors:
                    # no, proceed
                    if session_steps[MigStep.MIGRATE_METADATA]:
                        # migrate the schema
                        to_schema: str = setup_schema(errors=errors,
                                                      target_rdbms=session_spots[MigSpot.TO_RDBMS],
                                                      target_schema=session_specs[MigSpec.TO_SCHEMA],
                                                      target_engine=target_engine,
                                                      target_tables=target_tables,
                                                      target_views=target_views,
                                                      mat_views=mat_views,
                                                      logger=logger)
                        if not to_schema:
                            err_msg: str = f"Unable to migrate schema to RDBMS {session_spots[MigSpot.TO_RDBMS]}"
                            logger.error(msg=err_msg)
                            # 102: Unexpected error: {}
                            errors.append(validate_format_error(102,
                                                                err_msg))
                    else:
                        to_schema = session_specs[MigSpec.TO_SCHEMA]

                    # errors ?
                    if not errors:
                        # no, migrate tables' metadata (not applicable for views)
                        result = setup_tables(errors=errors,
                                              source_rdbms=session_spots[MigSpot.FROM_RDBMS],
                                              target_rdbms=session_spots[MigSpot.TO_RDBMS],
                                              source_schema=from_schema,
                                              target_schema=to_schema,
                                              target_s3=session_spots[MigSpot.TO_S3],
                                              target_tables=target_tables,
                                              override_columns=session_specs[MigSpec.OVERRIDE_COLUMNS] or {},
                                              step_metadata=session_steps[MigStep.MIGRATE_METADATA],
                                              logger=logger)

                        # proceed, if migrating the metadata was indicated
                        if not errors and session_steps[MigStep.MIGRATE_METADATA]:
                            # migrate the tables, one at a time
                            for target_table in target_tables:
                                try:
                                    source_metadata.create_all(bind=target_engine,
                                                               tables=[target_table],
                                                               checkfirst=False)
                                    if not session_spots[MigSpot.TO_S3]:
                                        # make sure LOB columns are nullable
                                        # (SQLAlchemy fails at that, in certain sitations)
                                        columns_props: dict = result.get(target_table.name).get("columns")
                                        for name, props in columns_props.items():
                                            if is_lob(col_type=props.get("source-type")) and \
                                               "nullable" not in props.get("features", []):
                                                props["features"] = props.get("features", [])
                                                props["features"].append("nullable")
                                                column_set_nullable(errors=errors,
                                                                    rdbms=session_spots[MigSpot.TO_RDBMS],
                                                                    table=f"{session_specs[MigSpec.TO_SCHEMA]}."
                                                                          f"{target_table.name}",
                                                                    column=name,
                                                                    logger=logger)
                                except (Exception, SAWarning) as e:
                                    # unable to fully compile the schema with a single table
                                    exc_err = str_sanitize(source=exc_format(exc=e,
                                                                             exc_info=sys.exc_info()))
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104,
                                                                        "schema-construction",
                                                                        exc_err))
                            # migrate the views, one at a time
                            for target_view in target_views:
                                op_errors: list[str] = []
                                view_ddl: str = view_get_ddl(errors=errors,
                                                             view_name=target_view,
                                                             view_type="M" if target_view in mat_views else "P",
                                                             source_rdbms=session_spots[MigSpot.FROM_RDBMS],
                                                             source_schema=from_schema,
                                                             target_schema=to_schema,
                                                             logger=logger)
                                if view_ddl:
                                    db_execute(errors=op_errors,
                                               exc_stmt=view_ddl,
                                               engine=session_spots[MigSpot.TO_RDBMS])
                                # errors ?
                                if op_errors:
                                    # yes, report them
                                    errors.extend(op_errors)
                                    err_msg: str = ("Unable to create view "
                                                    f"{session_specs[MigSpec.TO_SCHEMA]}.{target_view}")
                                    logger.error(msg=err_msg)
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104,
                                                                        "schema-construction",
                                                                        err_msg))
        else:
            err_msg: str = f"schema not found in RDBMS {session_spots[MigSpot.FROM_RDBMS]}"
            logger.error(msg=err_msg)
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                session_specs[MigSpec.FROM_SCHEMA],
                                                "@from-schema",
                                                err_msg))
    return result
