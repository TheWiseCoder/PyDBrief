import sys
from logging import Logger
from pypomes_core import (
    str_sanitize, exc_format, validate_format_error
)
from pypomes_db import DbEngine, db_execute
from pypomes_s3 import S3Engine
from sqlalchemy import (
    Engine, Inspector, MetaData, Table, inspect
)
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql.elements import Type
from typing import Any

from .pydb_database import column_set_nullable, view_get_ddl
from .pydb_migration import (
    prune_metadata, setup_schema, setup_tables
)
from .pydb_engine import build_engine
from migration.pydb_types import is_lob


# structure of the migration data returned:
# [
#   <table-name>: {
#      "columns": [
#        <column-name>: {
#          "name": <column-name>,
#          "source-type": <column-type>,
#          "target-type": <column-type>,
#          "features": [
#            "identity",
#            "nullable",
#            "unique",
#            "foreign-key",
#            "primary-key"
#          ]
#        },
#        ...
#      ],
#      "plain-count": <number-of-tuples-migrated>,
#      "plain-status": "none" | "full" | "partial"
#      "lob-count":  <number-of-lobs-migrated>,
#      "lob-status": "none" | "full" | "partial"
#   }
# ]
def migrate_metadata(errors: list[str],
                     source_rdbms: DbEngine,
                     target_rdbms: DbEngine,
                     source_schema: str,
                     target_schema: str,
                     target_s3: S3Engine,
                     step_metadata: bool,
                     process_indexes: bool,
                     process_views: bool,
                     relax_reflection: bool,
                     include_relations: list[str],
                     exclude_relations: list[str],
                     exclude_columns: list[str],
                     exclude_constraints: list[str],
                     override_columns: dict[str, Type],
                     logger: Logger) -> dict[str, Any]:

    # iinitialize the return variable
    result: dict[str, Any] | None = None

    # create engines
    source_engine: Engine = build_engine(errors=errors,
                                         rdbms=source_rdbms,
                                         logger=logger)
    target_engine: Engine = build_engine(errors=errors,
                                         rdbms=target_rdbms,
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
            if source_schema == schema_name.lower():
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
                result: bool = (rel not in exclude_relations and
                                rel not in schema_views and
                                (not include_relations or rel in include_relations))
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
                                        resolve_fks=not relax_reflection)
            except (Exception, SAWarning) as e:
                # - unable to fully reflect the source schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                exc_err: str = str_sanitize(exc_format(exc=e,
                                                       exc_info=sys.exc_info()))
                logger.error(msg=exc_err)
                # 104: The operation {} returned the error {}
                errors.append(validate_format_error(104,
                                                    "schema-reflection",
                                                    exc_err))
            if not errors:
                # build list of views to migrate
                target_views: list[str] = []
                if process_views:
                    if include_relations:
                        target_views.extend([view for view in include_relations
                                             if view in schema_views])
                    else:
                        target_views = schema_views

                # prepare the source metadata for migration
                prune_metadata(source_schema=source_schema,
                               source_metadata=source_metadata,
                               process_indexes=process_indexes,
                               schema_views=schema_views,
                               include_relations=include_relations,
                               exclude_relations=exclude_relations,
                               exclude_columns=exclude_columns,
                               exclude_constraints=exclude_constraints,
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
                    errors.append(validate_format_error(104,
                                                        "schema-migration",
                                                        exc_err))
                # errors ?
                if not errors:
                    # no, proceed
                    if step_metadata:
                        # migrate the schema
                        to_schema: str = setup_schema(errors=errors,
                                                      target_rdbms=target_rdbms,
                                                      target_schema=target_schema,
                                                      target_engine=target_engine,
                                                      target_tables=target_tables,
                                                      target_views=target_views,
                                                      mat_views=mat_views,
                                                      logger=logger)
                        if not to_schema:
                            err_msg: str = f"Unable to migrate schema to RDBMS {target_rdbms}"
                            logger.error(msg=err_msg)
                            # 102: Unexpected error: {}
                            errors.append(validate_format_error(102,
                                                                err_msg))
                    else:
                        to_schema = target_schema

                    # errors ?
                    if not errors:
                        # no, migrate tables' metadata (not applicable for views)
                        result = setup_tables(errors=errors,
                                              source_rdbms=source_rdbms,
                                              target_rdbms=target_rdbms,
                                              source_schema=from_schema,
                                              target_schema=to_schema,
                                              target_s3=target_s3,
                                              target_tables=target_tables,
                                              override_columns=override_columns,
                                              logger=logger)

                        # proceed, if migrating the metadata was indicated
                        if not errors and step_metadata:
                            # migrate the tables, one at a time
                            for target_table in target_tables:
                                try:
                                    source_metadata.create_all(bind=target_engine,
                                                               tables=[target_table],
                                                               checkfirst=False)
                                    if not target_s3:
                                        # make sure LOB columns are nullable
                                        # (SQLAlchemy fails at that, in certain sitations)
                                        columns_props: dict = result.get(target_table.name).get("columns")
                                        for name, props in columns_props.items():
                                            if is_lob(col_type=props.get("source-type")) and \
                                               "nullable" not in props.get("features", []):
                                                props["features"] = props.get("features", [])
                                                props["features"].append("nullable")
                                                column_set_nullable(errors=errors,
                                                                    rdbms=target_rdbms,
                                                                    table=f"{target_schema}.{target_table.name}",
                                                                    column=name,
                                                                    logger=logger)
                                except (Exception, SAWarning) as e:
                                    # unable to fully compile the schema with a single table
                                    exc_err = str_sanitize(exc_format(exc=e,
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
                                                             source_rdbms=source_rdbms,
                                                             source_schema=from_schema,
                                                             target_schema=to_schema,
                                                             logger=logger)
                                if view_ddl:
                                    db_execute(errors=op_errors,
                                               exc_stmt=view_ddl,
                                               engine=target_rdbms)
                                # errors ?
                                if op_errors:
                                    # yes, report them
                                    errors.extend(op_errors)
                                    err_msg: str = f"Unable to create view {target_schema}.{target_view}"
                                    logger.error(msg=err_msg)
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104,
                                                                        "schema-construction",
                                                                        err_msg))
        else:
            err_msg: str = f"schema not found in RDBMS {source_rdbms} @from-schema"
            logger.error(msg=err_msg)
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142,
                                                source_schema,
                                                err_msg))
    return result
