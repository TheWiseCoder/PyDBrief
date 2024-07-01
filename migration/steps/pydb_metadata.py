import sys
from logging import Logger
from pypomes_core import exc_format, str_sanitize, validate_format_error
from sqlalchemy import Engine, Inspector, MetaData, Table, inspect
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql.elements import Type

from .pydb_database import set_nullable
from .pydb_migration import (
    prune_metadata, migrate_schema, migrate_tables, migrate_view
)
from .pydb_engine import build_engine
from ..pydb_types import is_lob


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
                     source_rdbms: str,
                     target_rdbms: str,
                     source_schema: str,
                     target_schema: str,
                     step_metadata: bool,
                     process_indexes: bool,
                     include_tables: list[str],
                     exclude_tables: list[str],
                     include_views: list[str],
                     skip_columns: list[str],
                     skip_ck_constraints: list[str],
                     skip_fk_constraints: list[str],
                     skip_named_constraints: list[str],
                     external_columns: dict[str, Type],
                     logger: Logger | None) -> dict:

    # iinitialize the return variable
    result: dict | None = None

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
            # clear parameters not needed if not creating/transforming the target schema
            if not step_metadata:
                process_indexes = False
                include_views.clear()
                skip_ck_constraints.clear()
                skip_fk_constraints.clear()
                skip_named_constraints.clear()

            # obtain the list of plain and materialized views in source schema
            plain_views: list[str] = source_inspector.get_view_names()
            mat_views: list[str] = source_inspector.get_materialized_view_names()
            all_views: list[str] = plain_views + mat_views
            if include_views == ["*"]:
                include_views = all_views

            # determine if table 'tb' is to be included in 'source_metadata'
            def sel_tb(tb: str, _md: MetaData) -> bool:
                return (tb not in exclude_tables and
                        ((not include_tables or tb in include_tables) or
                         (tb in all_views and tb in include_views)))

            # obtain the source schema metadata
            source_metadata: MetaData = MetaData(schema=from_schema)
            try:
                # HAZARD:
                # - if the parameter 'resolve_fks' is set to 'True' (the default value),
                #   then tables and views referenced in FK columns of included tables
                #   will also be included, regardless of parameters 'only' or 'views'
                #   (this is remedied at 'prune_metadata()')
                # - SQLAlchemy will raise a 'NoReferencedTableError' exception upon
                #   'source_metadata.sorted_tables' retrieval, if a FK-referenced table
                #   is prevented from loading (requires 'resolve_fks' to be set to 'False')
                source_metadata.reflect(bind=source_engine,
                                        schema=from_schema,
                                        only=sel_tb,
                                        views=len(include_views) > 0)
            except (Exception, SAWarning) as e:
                # - unable to fully reflect the source schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                exc_err = str_sanitize(exc_format(exc=e,
                                                  exc_info=sys.exc_info()))
                # 104: The operation {} returned the error {}
                errors.append(validate_format_error(104, "schema-reflection", exc_err))

            if not errors:
                # prepare the source metadata for migration
                prune_metadata(source_schema=source_schema,
                               source_metadata=source_metadata,
                               plain_views=plain_views,
                               mat_views=mat_views,
                               include_tables=include_tables,
                               exclude_tables=exclude_tables,
                               include_views=include_views,
                               skip_columns=skip_columns,
                               skip_ck_constraints=skip_ck_constraints,
                               skip_fk_constraints=skip_fk_constraints,
                               skip_named_constraints=skip_named_constraints,
                               process_indexes=process_indexes,
                               logger=logger)

                # proceed with the appropriate tables
                sorted_tables: list[Table] = []
                try:
                    sorted_tables: list[Table] = source_metadata.sorted_tables
                except (Exception, SAWarning) as e:
                    # - unable to organize the tables in the proper sequence, probably caused by:
                    #   - cross-dependencies between tables, caused by mutually dependent FKs, or
                    #   - a table or view referenced by a FK column was not found in the schema
                    # - this error will cause the migration to be aborted,
                    #   as SQLAlchemy would not be able to compile the migrated schema
                    exc_err = str_sanitize(exc_format(exc=e,
                                                      exc_info=sys.exc_info()))
                    # 104: The operation {} returned the error {}
                    errors.append(validate_format_error(104, "schema-migration", exc_err))

                # errors ?
                if not errors:
                    # no, proceed
                    if step_metadata:
                        # migrate the schema
                        to_schema: str = migrate_schema(errors=errors,
                                                        target_rdbms=target_rdbms,
                                                        target_schema=target_schema,
                                                        target_engine=target_engine,
                                                        target_tables=sorted_tables,
                                                        plain_views=plain_views,
                                                        mat_views=mat_views,
                                                        logger=logger)
                        if not to_schema:
                            # 102: Unexpected error: {}
                            errors.append(validate_format_error(
                                102, f"unable to migrate schema to RDBMS {target_rdbms}"))
                    else:
                        to_schema = target_schema

                    # errors ?
                    if not errors:
                        # no, migrate tables' metadata (not applicable for views)
                        real_tables: list[Table] = [table for table in sorted_tables
                                                    if table.name not in plain_views + mat_views]
                        result = migrate_tables(errors=errors,
                                                source_rdbms=source_rdbms,
                                                target_rdbms=target_rdbms,
                                                source_schema=from_schema,
                                                target_schema=to_schema,
                                                target_tables=real_tables,
                                                external_columns=external_columns,
                                                logger=logger)

                        # proceed, if migrating the metadata was indicated
                        if step_metadata:
                            # migrate the schema, one table at a time
                            for real_table in real_tables:
                                try:
                                    source_metadata.create_all(bind=target_engine,
                                                               tables=[real_table],
                                                               checkfirst=False)
                                    # make sure LOB columns are nullable
                                    # (SQLAlchemy fails at that, in certain sitations)
                                    columns_props: dict = result.get(real_table.name).get("columns")
                                    for name, props in columns_props.items():
                                        if is_lob(col_type=props.get("source-type")) and \
                                           "nullable" not in props.get("features", []):
                                            props["features"] = props.get("features", [])
                                            props["features"].append("nullable")
                                            set_nullable(errors=errors,
                                                         rdbms=target_rdbms,
                                                         table=f"{target_schema}.{real_table.name}",
                                                         column=name,
                                                         logger=logger)
                                except (Exception, SAWarning) as e:
                                    # unable to fully compile the schema with a single table
                                    exc_err = str_sanitize(exc_format(exc=e,
                                                                      exc_info=sys.exc_info()))
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104, "schema-construction", exc_err))

                            # migrate the schema, one view at a time
                            real_views: list[Table] = [table for table in sorted_tables
                                                       if table.name in plain_views + mat_views]
                            for real_view in real_views:
                                migrate_view(errors=errors,
                                             view_name=real_view.name,
                                             view_type="M" if real_view.name in mat_views else "P",
                                             source_rdbms=source_rdbms,
                                             target_rdbms=target_rdbms,
                                             source_schema=from_schema,
                                             target_schema=to_schema,
                                             logger=logger)
        else:
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142, source_schema,
                                                f"schema not found in RDBMS {source_rdbms}",
                                                "@from-schema"))
    return result
