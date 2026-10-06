import sys
from datetime import datetime
from logging import Logger
from pypomes_core import TZ_LOCAL, str_as_list, str_sanitize, exc_format, validate_format_error
from pypomes_db import (
    db_create_table, db_table_exists, db_execute,
    db_get_columns_metadata, db_get_table_pk, db_build_column_clause
)
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, inspect
)
from sqlalchemy.exc import SAWarning
from typing import Any

from entities.database import Database
from entities.migration import Migration
from entities.migration_issue import MigrationIssue, IssueType
from entities.migration_table import MigrationTable
from entities.migration_work import MigrationWork
from entities.session import Session

from app_constants import PYDB_DB_ENGINE, InputParam, MigStep
from migration.pydb_common import get_migration_work, execute_sql
from migration.pydb_database import column_set_nullable, view_get_ddl, build_engine
from migration.pydb_types import convert_column_type, is_lob_column
from migration.steps.pydb_migration import (
    assert_relation, prune_metadata, setup_schema, setup_tables
)


def migrate_metadata(migration: Migration,
                     session: Session,
                     mig_step: MigStep,
                     migration_warnings: list[str],
                     errors: list[str],
                     logger: Logger) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    source_db: Database = session.get_source_db()
    target_db: Database = session.get_target_db()
    migration_tables: list[MigrationTable] | None = migration.get_migration_tables() or []
    if migration.ds_pre_sql:
        execute_sql(migration=migration,
                    mig_step=mig_step,
                    db_engine=source_db.cd_engine,
                    sql_text=migration.ds_pre_sql)

    # create engines
    sa_source_engine: Engine | None = None
    sa_target_engine: Engine | None = None
    if not errors:
        sa_source_engine = build_engine(db_engine=source_db.cd_engine,
                                        errors=errors,
                                        logger=logger)
        sa_target_engine = build_engine(db_engine=target_db.cd_engine,
                                        errors=errors,
                                        logger=logger)

    if sa_source_engine and sa_target_engine:
        from_schema: str | None = None

        # obtain the source schema's internal name
        source_inspector: Inspector = inspect(subject=sa_source_engine,
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

            # determine the relations to be processed
            only_tables: list[str] = []
            all_tables: list[str] = source_inspector.get_table_names(schema=from_schema)
            for table_name in all_tables:
                ok: bool = table_name.lower() not in schema_views and \
                           assert_relation(migration=migration,
                                           relation=table_name.lower())
                if ok:
                    only_tables.append(table_name)
                    migration_table: MigrationTable = MigrationTable.for_table(
                        table=table_name.lower(),
                        migration_tables=migration_tables
                    )
                    if migration_table and migration_table.ds_pre_sql:
                        execute_sql(migration=migration,
                                    mig_step=mig_step,
                                    db_engine=source_db.cd_engine,
                                    sql_text=migration_table.ds_pre_sql)
                logger.debug(msg=f"Relation '{table_name}' asserted '{ok}' on inspection")

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
                source_metadata.reflect(bind=sa_source_engine,
                                        schema=from_schema,
                                        views=False,
                                        only=only_tables,
                                        resolve_fks=not migration.is_relax_reflection)
            except (Exception, SAWarning) as e:
                # - unable to fully reflect the source schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                exc_err: str = str_sanitize(exc_format(exc=e,
                                                       exc_info=sys.exc_info()))
                logger.error(msg=exc_err)
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_step=mig_step,
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
                target_tables: list[Table] = []
                prune_metadata(migration=migration,
                               session=session,
                               mig_step=mig_step,
                               migration_tables=migration_tables,
                               source_metadata=source_metadata,
                               logger=logger)

                # proceed with the appropriate tables
                try:
                    # 'target_tables' will contain no views (as per 'prune_metadata()')
                    target_tables = source_metadata.sorted_tables
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
                                             cd_step=mig_step,
                                             cd_type=IssueType.ERROR,
                                             ds_issue=exc_err)
                    errors.append(validate_format_error(104,
                                                        "schema-migration",
                                                        exc_err))
                to_schema: str | None = None
                if not errors:
                    if mig_step == MigStep.MIGRATE_METADATA:
                        # migrate the schema
                        to_schema = setup_schema(migration=migration,
                                                 target_db=target_db.cd_engine,
                                                 target_schema=session.nm_target_schema,
                                                 target_engine=sa_target_engine,
                                                 target_tables=target_tables,
                                                 target_views=target_views,
                                                 mat_views=mat_views,
                                                 errors=errors,
                                                 logger=logger)
                        if not to_schema:
                            err_msg: str = f"Unable to migrate schema to RDBMS '{source_db.cd_engine}'"
                            logger.error(msg=err_msg)
                            MigrationIssue.new_issue(id_migration=migration.id,
                                                     cd_step=mig_step,
                                                     cd_type=IssueType.ERROR,
                                                     ds_issue=err_msg)
                            # 102: Unexpected error: {}
                            errors.append(validate_format_error(102,
                                                                err_msg))
                    else:
                        to_schema = session.nm_target_schema

                if not errors:
                    # migrated tables' metadata (not applicable for views)
                    result = setup_tables(migration=migration,
                                          session=session,
                                          mig_step=mig_step,
                                          migration_tables=migration_tables,
                                          target_tables=target_tables,
                                          migration_warnings=migration_warnings,
                                          errors=errors,
                                          logger=logger)
                    # initialize the list of tables created in the current migration run
                    result["effected-tables"] = []

                    # reify materialized views
                    if not errors and mig_step == MigStep.MIGRATE_METADATA:
                        reify_mviews: list[str] = str_as_list(migration.ds_reify_mviews)
                        for reify_mview in reify_mviews:
                            table_name: str = f"{from_schema}.{reify_mview}"
                            source_cols_metadata: list[tuple] = db_get_columns_metadata(table_name=table_name,
                                                                                        engine=source_db.cd_engine,
                                                                                        errors=errors)
                            if not errors:
                                table_pk: tuple[str, str] = db_get_table_pk(table_name=table_name,
                                                                            engine=source_db.cd_engine,
                                                                            errors=errors)
                                if not errors:
                                    pk_constraint: list[str] = [f"{table_pk[0]} PRIMARY KEY ({table_pk[1]})"] \
                                        if table_pk else None
                                    target_cols_metadata: list[tuple] = []
                                    for col_metadata in source_cols_metadata:
                                        type_equivalent: str = \
                                            convert_column_type(col_type=col_metadata[1].lower(),
                                                                db_source_type=source_db.cd_type,
                                                                db_target_type=target_db.cd_type)
                                        # col_metadata[6] has the default value
                                        target_cols_metadata.append(
                                            (col_metadata[0].lower(), type_equivalent,
                                             col_metadata[2], col_metadata[3],
                                             col_metadata[4], col_metadata[5], None))
                                    create_table: bool = not db_table_exists(table_name=table_name,
                                                                             engine=target_db.cd_engine,
                                                                             errors=errors) and not errors
                                    if create_table:
                                        try:
                                            # noinspection PyTypeChecker
                                            db_create_table(table_name=table_name,
                                                            column_data=target_cols_metadata,
                                                            constraints=pk_constraint,
                                                            engine=target_db.cd_engine,
                                                            errors=errors)
                                        except (Exception, SAWarning) as e:
                                            # unable to create table
                                            exc_err: str = str_sanitize(exc_format(exc=e,
                                                                                   exc_info=sys.exc_info()))
                                            logger.error(msg=exc_err)
                                            MigrationIssue.new_issue(id_migration=migration.id,
                                                                     cd_step=mig_step,
                                                                     cd_type=IssueType.ERROR,
                                                                     ds_issue=exc_err)
                                            # 104: The operation {} returned the error {}
                                            errors.append(validate_format_error(104,
                                                                                "schema-construction",
                                                                                exc_err))
                                    if not errors:
                                        columns: dict[str, Any] = {}
                                        for i in range(0, len(target_cols_metadata)):
                                            source_clause: list[str] = db_build_column_clause(
                                                col_name=target_cols_metadata[i][0],
                                                col_metadata=source_cols_metadata[i][1:]).split(maxsplit=1)
                                            target_clause: list[str] = db_build_column_clause(
                                                col_name=target_cols_metadata[i][0],
                                                col_metadata=target_cols_metadata[i][1:]).split(maxsplit=1)

                                            columns[target_clause[0]] = {
                                                "source-type": source_clause[1],
                                                "target-type": target_clause[1]
                                            }
                                            if table_pk and target_clause[0] in str_as_list(table_pk[1].lower()):
                                                columns[target_clause[0]]["features"] = "primary-key"
                                        result[reify_mview] = {"columns": columns}
                                        if create_table:
                                            result["effected-tables"].append(reify_mview)

                            if errors:
                                MigrationIssue.new_issues(id_migration=migration.id,
                                                          cd_step=mig_step,
                                                          cd_type=IssueType.ERROR,
                                                          ds_issues=errors)
                                break

                    # migrate the tables
                    if not errors and mig_step == MigStep.MIGRATE_METADATA:
                        for target_table in target_tables:
                            migration_work: MigrationWork = get_migration_work(migration=migration,
                                                                               table=target_table.name,
                                                                               errors=errors)
                            if not errors and not migration_work.is_created:
                                try:
                                    source_metadata.create_all(bind=sa_target_engine,
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
                                    # table was successfully created
                                    result["effected-tables"].append(target_table.name)
                                    migration_work.is_created = True
                                    migration_work.ts_finish = datetime.now(tz=TZ_LOCAL)
                                    migration_work.update(db_engine=PYDB_DB_ENGINE,
                                                          errors=errors)
                                except (Exception, SAWarning) as e:
                                    # unable to fully compile the schema with a single table
                                    exc_err: str = str_sanitize(exc_format(exc=e,
                                                                           exc_info=sys.exc_info()))
                                    logger.error(msg=exc_err)
                                    MigrationIssue.new_issue(id_migration=migration.id,
                                                             cd_step=mig_step,
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
                                                         source_db=source_db.cd_engine,
                                                         source_schema=from_schema,
                                                         target_schema=to_schema,
                                                         errors=errors,
                                                         logger=logger)
                            if view_ddl:
                                db_execute(exc_stmt=view_ddl,
                                           engine=source_db.cd_engine,
                                           errors=curr_errors)
                            # errors ?
                            if curr_errors:
                                # yes, report them
                                errors.extend(curr_errors)
                                MigrationIssue.new_issues(id_migration=migration.id,
                                                          cd_step=mig_step,
                                                          cd_type=IssueType.ERROR,
                                                          ds_issues=curr_errors)
                                err_msg: str = ("Unable to create view "
                                                f"{session.nm_target_schema}.{target_view}")
                                logger.error(msg=err_msg)
                                MigrationIssue.new_issue(id_migration=migration.id,
                                                         cd_step=mig_step,
                                                         cd_type=IssueType.ERROR,
                                                         ds_issue=err_msg)
                                # 104: The operation {} returned the error {}
                                errors.append(validate_format_error(104,
                                                                    "schema-construction",
                                                                    err_msg))
        else:
            err_msg: str = f"schema not found in RDBMS '{source_db.cd_engine}'"
            logger.error(msg=err_msg)
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                session.nm_source_schema,
                                                f"@{InputParam.SOURCE_SCHEMA}",
                                                err_msg))
    return result
