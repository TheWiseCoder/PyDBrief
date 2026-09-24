import sys
from logging import Logger
from pypomes_core import str_sanitize, exc_format, validate_format_error
from pypomes_db import db_execute
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, inspect
)
from sqlalchemy.exc import SAWarning
from typing import Any

from entities.migration import Migration, MigStep
from entities.migration_issue import MigrationIssue, IssueType
from entities.session import Session

from app_constants import InputParam
from migration.pydb_database import column_set_nullable, view_get_ddl
from migration.pydb_engine import build_engine
from migration.pydb_types import is_lob_column
from migration.steps.pydb_migration import (
    assert_relation, prune_metadata, setup_schema, setup_tables
)


def migrate_metadata(migration: Migration,
                     session: Session,
                     migration_warnings: list[str],
                     errors: list[str],
                     logger: Logger) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    # create engines
    source_engine: Engine = build_engine(db_engine=session.get_source_db().cd_engine,
                                         errors=errors,
                                         logger=logger)
    target_engine: Engine = build_engine(db_engine=session.get_target_db().cd_engine,
                                         errors=errors,
                                         logger=logger)
    if source_engine and target_engine:
        from_schema: str | None = None

        # obtain the source schema's internal name
        source_inspector: Inspector = inspect(subject=source_engine,
                                              raiseerr=True)
        for schema_name in source_inspector.get_schema_names():
            if session.nm_source_schema == schema_name.lower():
                # use the actual name with its case imprint
                from_schema = schema_name
                break

        if from_schema:
            # obtain the list of plain and materialized views in source schema
            plain_views: list[str] = [v.lower() for v in
                                      source_inspector.get_view_names(schema=from_schema)]
            mat_views: list[str] = [v.lower() for v in
                                    source_inspector.get_materialized_view_names(schema=from_schema)]
            schema_views: list[str] = plain_views + mat_views

            # determine if relation 'rel' is to be reflected in 'source_metadata'
            def assert_reflection(rel: str,
                                  _md: MetaData) -> bool:
                rel = rel.lower()
                result = (rel not in schema_views and
                          assert_relation(migration=migration,
                                          relation=rel))
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
                                        views=False,
                                        only=assert_reflection,
                                        resolve_fks=not migration.is_relax_reflection)
            except (Exception, SAWarning) as e:
                # - unable to fully reflect the source schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                exc_err: str = str_sanitize(exc_format(exc=e,
                                                       exc_info=sys.exc_info()))
                logger.error(msg=exc_err)
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_type=IssueType.ERROR,
                                         ds_issue=exc_err)
                # 104: The operation {} returned the error {}
                errors.append(validate_format_error(104,
                                                    "schema-reflection",
                                                    exc_err))
            if not errors:
                # build list of views to migrate
                target_views: list[str] = []
                if migration.is_process_views:
                    if migration.ds_include_relations or migration.ds_exclude_relations:
                        target_views.extend([v for v in schema_views if assert_relation(migration=migration,
                                                                                        relation=v)])
                    else:
                        target_views = schema_views

                # prepare the source metadata for migration
                prune_metadata(migration=migration,
                               session=session,
                               source_metadata=source_metadata,
                               schema_views=schema_views,
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
                    exc_err: str = str_sanitize(exc_format(exc=e,
                                                           exc_info=sys.exc_info()))
                    logger.error(msg=exc_err)
                    # 104: The operation {} returned the error {}
                    MigrationIssue.new_issue(id_migration=migration.id,
                                             cd_type=IssueType.ERROR,
                                             ds_issue=exc_err)
                    errors.append(validate_format_error(104,
                                                        "schema-migration",
                                                        exc_err))
                if not errors:
                    if migration.cd_step == MigStep.MIGRATE_METADATA:
                        # migrate the schema
                        to_schema: str = setup_schema(target_db=session.get_target_db().cd_engine,
                                                      target_schema=session.nm_target_schema,
                                                      target_engine=target_engine,
                                                      target_tables=target_tables,
                                                      target_views=target_views,
                                                      mat_views=mat_views,
                                                      errors=errors,
                                                      logger=logger)
                        if not to_schema:
                            err_msg: str = f"Unable to migrate schema to RDBMS '{session.get_source_db().cd_engine}'"
                            logger.error(msg=err_msg)
                            MigrationIssue.new_issue(id_migration=migration.id,
                                                     cd_type=IssueType.ERROR,
                                                     ds_issue=err_msg)
                            # 102: Unexpected error: {}
                            errors.append(validate_format_error(102,
                                                                err_msg))
                    else:
                        to_schema = session.nm_target_schema

                    if not errors:
                        # migrate tables' metadata (not applicable for views)
                        result = setup_tables(migration=migration,
                                              session=session,
                                              target_tables=target_tables,
                                              migration_warnings=migration_warnings,
                                              errors=errors,
                                              logger=logger)

                        # proceed, if migrating the metadata was indicated
                        if not errors and migration.cd_step == MigStep.MIGRATE_METADATA:
                            # migrate the tables, one at a time
                            for target_table in target_tables:
                                try:
                                    source_metadata.create_all(bind=target_engine,
                                                               tables=[target_table],
                                                               checkfirst=False)
                                    if not session.id_target_s3:
                                        # make sure LOB columns are nullable
                                        # (SQLAlchemy fails at that, in certain sitations)
                                        columns_props: dict = result.get(target_table.name).get("columns")
                                        for name, props in columns_props.items():
                                            if is_lob_column(col_type=props.get("source-type")) and \
                                               "nullable" not in props.get("features", []):
                                                props["features"] = props.get("features", [])
                                                props["features"].append("nullable")
                                                column_set_nullable(db_type=session.get_target_db().cd_type,
                                                                    table=f"{session.nm_target_schema}."
                                                                          f"{target_table.name}",
                                                                    column=name,
                                                                    errors=errors)
                                except (Exception, SAWarning) as e:
                                    # unable to fully compile the schema with a single table
                                    exc_err: str = str_sanitize(exc_format(exc=e,
                                                                           exc_info=sys.exc_info()))
                                    logger.error(msg=exc_err)
                                    MigrationIssue.new_issue(id_migration=migration.id,
                                                             cd_type=IssueType.ERROR,
                                                             ds_issue=exc_err)
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104,
                                                                        "schema-construction",
                                                                        exc_err))
                            # migrate the views, one at a time
                            for target_view in target_views:
                                curr_errors: list[str] = []
                                view_ddl: str = view_get_ddl(view_name=target_view,
                                                             view_type="M" if target_view in mat_views else "P",
                                                             source_db=session.get_source_db().cd_engine,
                                                             source_schema=from_schema,
                                                             target_schema=to_schema,
                                                             errors=errors,
                                                             logger=logger)
                                if view_ddl:
                                    db_execute(exc_stmt=view_ddl,
                                               engine=session.get_source_db().cd_engine,
                                               errors=curr_errors)
                                # errors ?
                                if curr_errors:
                                    # yes, report them
                                    errors.extend(curr_errors)
                                    MigrationIssue.new_issues(id_migration=migration.id,
                                                              cd_type=IssueType.ERROR,
                                                              ds_issues=curr_errors)
                                    err_msg: str = ("Unable to create view "
                                                    f"{session.nm_target_schema}.{target_view}")
                                    logger.error(msg=err_msg)
                                    MigrationIssue.new_issue(id_migration=migration.id,
                                                             cd_type=IssueType.ERROR,
                                                             ds_issue=err_msg)
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104,
                                                                        "schema-construction",
                                                                        err_msg))
        else:
            err_msg: str = f"schema not found in RDBMS '{session.get_source_db().cd_engine}'"
            logger.error(msg=err_msg)
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                session.nm_source_schema,
                                                f"@{InputParam.SOURCE_SCHEMA}",
                                                err_msg))
    return result
