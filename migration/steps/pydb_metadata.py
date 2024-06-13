import sys
from logging import Logger
from pypomes_core import exc_format, str_sanitize, validate_format_error
from sqlalchemy import Engine, Inspector, MetaData, Table, inspect
from sqlalchemy.exc import SAWarning
from sqlalchemy.sql.elements import Type

from .pydb_migration import migrate_schema, migrate_tables, migrate_schema_views
from .pydb_engine import build_engine


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
                     process_views: bool,
                     process_mviews: bool,
                     include_tables: list[str],
                     exclude_tables: list[str],
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
            if source_schema.lower() == schema_name.lower():
                # yes, use the actual name with its case imprint
                from_schema = schema_name
                break

        # proceed, if the source schema exists
        if from_schema:
            # obtain the source schema metadata
            source_metadata: MetaData = MetaData(schema=from_schema)
            try:
                source_metadata.reflect(bind=source_engine,
                                        schema=from_schema,
                                        views=False)
            except SAWarning as e:
                # - unable to fully reflect the schema
                # - this error will cause the migration to be aborted,
                #   as SQLAlchemy will not be able to find the schema tables
                exc_err = str_sanitize(exc_format(exc=e,
                                                  exc_info=sys.exc_info()))
                # 104: The operation {} returned the error {}
                errors.append(validate_format_error(104, "schema-reflection", exc_err))

            if not errors:
                # build list of migration candidates
                source_tables: list[Table] = list(source_metadata.tables.values())
                target_tables: list[Table] = []
                include: bool = not include_tables
                for source_table in source_tables:
                    table_name: str = source_table.name.lower()
                    if table_name in include_tables:
                        target_tables.append(source_table)
                        include_tables.remove(table_name)
                    elif table_name in exclude_tables:
                        exclude_tables.remove(table_name)
                    elif include and source_table.schema == from_schema:
                        target_tables.append(source_table)

                # proceed, if all tables in include and exclude lists were accounted for
                if include_tables or exclude_tables:
                    # some tables not found, report them
                    bad_tables: str = ",".join(include_tables + exclude_tables)
                    # 142: Invalid value {}: {}
                    errors.append(validate_format_error(142, bad_tables,
                                                        f"not found in {source_rdbms}.{source_schema}"))
                else:
                    # purge the source metadata from tables not selected, and from indexes if applicable
                    for source_table in source_tables:
                        if source_table not in target_tables:
                            source_metadata.remove(table=source_table)
                        elif step_metadata and not process_indexes:
                            source_table.indexes.clear()

                    # proceed with the appropriate tables
                    sorted_tables: list[Table] = []
                    try:
                        sorted_tables: list[Table] = source_metadata.sorted_tables
                    except SAWarning as e:
                        # - unable to organize the tables in the proper sequence:
                        #   probably, cross-dependencies between tables, caused by mutually dependent FKs
                        # - this error will cause the migration to be aborted,
                        #   as SQLAlchemy would not be able to compile the migrated schema
                        exc_err = str_sanitize(exc_format(exc=e,
                                                          exc_info=sys.exc_info()))
                        # 104: The operation {} returned the error {}
                        errors.append(validate_format_error(104, "schema-migration", exc_err))

                    # any errors ?
                    if not errors:
                        # no, proceed
                        if step_metadata:

                            # migrate the schema
                            to_schema: str = migrate_schema(errors=errors,
                                                            target_rdbms=target_rdbms,
                                                            target_schema=target_schema,
                                                            target_engine=target_engine,
                                                            target_tables=sorted_tables,
                                                            logger=logger)
                        else:
                            to_schema = target_schema

                        # proceed, if there is a schema to work with
                        if to_schema:
                            # yes, migrate the tables
                            result = migrate_tables(errors=errors,
                                                    source_rdbms=source_rdbms,
                                                    target_rdbms=target_rdbms,
                                                    source_schema=source_schema,
                                                    target_tables=sorted_tables,
                                                    external_columns=external_columns,
                                                    logger=logger)

                            # proceed, if migrating the metadata was indicated
                            if step_metadata:
                                # assign the new schema for the migration candidate tables
                                for sorted_table in sorted_tables:
                                    sorted_table.schema = to_schema

                                try:
                                    # migrate the schema
                                    source_metadata.create_all(bind=target_engine,
                                                               checkfirst=False)
                                    if process_mviews:
                                        # migrate the materialized views in the schema
                                        migrate_schema_views(errors=errors,
                                                             source_rdbms=source_rdbms,
                                                             source_schema=source_schema,
                                                             target_rdbms=target_rdbms,
                                                             target_schema=target_schema,
                                                             view_type="M",
                                                             logger=logger)
                                    if process_views:
                                        # migrate the views in the schema
                                        migrate_schema_views(errors=errors,
                                                             source_rdbms=source_rdbms,
                                                             source_schema=source_schema,
                                                             target_rdbms=target_rdbms,
                                                             target_schema=target_schema,
                                                             view_type="P",
                                                             logger=logger)
                                except Exception as e:
                                    # unable to fully compile the schema
                                    exc_err = str_sanitize(exc_format(exc=e,
                                                                      exc_info=sys.exc_info()))
                                    # 104: The operation {} returned the error {}
                                    errors.append(validate_format_error(104, "schema-construction", exc_err))
                        else:
                            # 102: Unexpected error: {}
                            errors.append(validate_format_error(102,
                                                                f"unable to create schema in RDBMS {target_rdbms}",
                                                                "@to-schema"))
        else:
            # 142: Invalid value {}: {}
            errors.append(validate_format_error(142, source_schema,
                                                f"schema not found in RDBMS {source_rdbms}",
                                                "@from-schema"))
    return result
