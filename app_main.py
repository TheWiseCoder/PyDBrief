import json
import sys
from flask import (
    Blueprint, Flask, Request, Response, jsonify, request, send_file
)
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from pathlib import Path
from sqlalchemy.sql.elements import Type
from typing import Any, Final

from app_ident import APP_NAME, APP_VERSION  # must be imported before PyPomes and local packages
from pypomes_core import (
    Mimetype, pypomes_versions, exc_format,
    validate_bool, validate_strs, validate_enum,
    validate_format_error, validate_format_errors
)
from pypomes_db import DbEngine
from pypomes_http import (
    http_get_parameter, http_get_parameters
)
from pypomes_logging import PYPOMES_LOGGER, logging_service
from pypomes_s3 import S3Engine

from app_constants import DbConfig, S3Config, MigrationConfig
from migration.pydb_common import (
    MigrationMetrics, OngoingMigrations,
    get_s3_params, set_s3_params,
    get_rdbms_params, set_rdbms_params, set_migration_metrics
)
from migration.pydb_migrator import migrate
from migration.pydb_validator import (
    assert_override_columns, assert_incremental_migrations,
    assert_params, assert_migration, assert_migration_badge,
    get_migration_context
)

# create the Flask application
flask_app: Final[Flask] = Flask(__name__)

# support cross-origin resource sharing
CORS(flask_app)

# set the logging endpoint
flask_app.add_url_rule(rule="/logging",
                       endpoint="logging",
                       view_func=logging_service,
                       methods=["GET", "POST"])

# make PyDBrief's REST API available as a Swagger app
swagger_blueprint: Blueprint = get_swaggerui_blueprint(
    base_url="/apidocs",
    api_url="/swagger",
    config={"defaultModelsExpandDepth": -1}
)
flask_app.register_blueprint(blueprint=swagger_blueprint)

# configure 'jsonify()' with 'ensure_ascii=False'
flask_app.config["JSON_AS_ASCII"] = False


@flask_app.route("/swagger")
def swagger() -> Response:
    """
    Entry point for the microservice providing OpenAPI specifications in the Swagger standard.

    The optional *filename* parameter specifies the name of the file to be written to by the browser.
    If omitted, the browser is asked to only display the returned content.

    :return: the requested OpenAPI specifications
    """
    filename: str = http_get_parameter(request=request,
                                       param="filename")

    return send_file(path_or_file=Path(Path.cwd(), "swagger/pydbrief.json"),
                     mimetype=Mimetype.JSON,
                     as_attachment=filename is not None,
                     download_name=filename)


@flask_app.route(rule="/version",
                 methods=["GET"])
def version() -> Response:
    """
    Obtain the current version of *PyDBrief*, along with the foundation modules in use.

    :return: the versions in execution
    """
    # register the request
    PYPOMES_LOGGER.info(msg=f"URL {request.url}")

    # retrieve the versions
    versions: dict[str, Any] = {
        APP_NAME: APP_VERSION,
        "Foundations": pypomes_versions()
    }

    # assign to the return variable
    result: Response = jsonify(versions)

    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {request.path}: {result}")

    return result


@flask_app.route(rule="/rdbms",
                 methods=["POST"])
@flask_app.route(rule="/rdbms/<engine>",
                 methods=["GET"])
def handle_rdbms(engine: str = None) -> Response:
    """
    Entry point for configuring the RDBMS engine to use.

    The parameters are as follows:
      - *db-engine*: the reference RDBMS engine (*mysql*, *oracle*, *postgres*, or *sqlserver*)
      - *db-name*: name of database
      - *db-user*: the logon user
      - *db-pwd*: the logon password
      - *db-host*: the host URL
      - *db-port*: the connection port
      - *db-client*: the client package (Oracle, only)
      - *db-driver*: the database access driver (SQLServer, only)

    :param engine: the reference RDBMS engine (*mysql*, *oracle*, *postgres*, or *sqlserver*)
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    db_engine: DbEngine = DbEngine(engine) if engine in DbEngine else None
    input_params: dict[str, Any] = http_get_parameters(request=request)
    assert_params(errors=errors,
                  service="/rdbms",
                  method=request.method,
                  input_params=input_params)

    reply: dict[str, Any] | None = None
    if not errors:
        if request.method == "GET":
            # get RDBMS connection params
            reply = get_rdbms_params(errors=errors,
                                     db_engine=db_engine)
        else:
            # configure the RDBMS
            set_rdbms_params(errors=errors,
                             input_params=input_params)
            if not errors:
                engine = input_params.get(DbConfig.ENGINE)
                reply = {"status": f"RDBMS '{engine}' configuration updated"}

    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    input_params.pop(DbConfig.PWD, None)
    PYPOMES_LOGGER.info(f"Response {request.path}?{input_params}: {result}")

    return result


@flask_app.route(rule="/s3",
                 methods=["POST"])
@flask_app.route(rule="/s3/<engine>",
                 methods=["GET"])
def handle_s3(engine: str = None) -> Response:
    """
    Entry point for configuring the S3 service to use.

    The parameters are as follows:
        - *s3-engine*: the reference S3 engine (*aws* or *minio*)
        - *s3-endpoint-url*: the access URL for the service
        - *s3-bucket-name*: the name of the default bucket
        - *s3-access-key*: the access key for the service
        - *s3-secret-key*: the access secret code
        - *s3-region-name*: the name of the region where the engine is located (AWS only)
        - *s3-secure-access*: whether to use Transport Security Layer (MinIO only)

    :param engine: the reference S3 engine (*aws* or *minio*)
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    assert_params(errors=errors,
                  service="/s3",
                  method=request.method,
                  input_params=input_params)

    reply: dict[str, Any] | None = None
    if not errors:
        # obtain the S3 engine
        engine = engine or input_params.get(S3Config.ENGINE)
        s3_engine: S3Engine = S3Engine(engine) if engine in S3Engine else None
        if s3_engine:
            if request.method == "GET":
                # get S3 access params
                reply = get_s3_params(errors=errors,
                                      s3_engine=s3_engine)
            else:
                # configure the S3 service
                set_s3_params(errors=errors,
                              input_params=input_params)
                if not errors:
                    reply = {"status": f"S3 '{engine}' configuration updated"}
        else:
            # 141: Invalid value {}
            errors.append(validate_format_error(141,
                                                engine,
                                                f"@{S3Config.ENGINE}"))
    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    input_params.pop(S3Config.SECRET_KEY, None)
    PYPOMES_LOGGER.info(msg=f"Response {request.path}?{input_params}: {result}")

    return result


@flask_app.route(rule="/migration:metrics",
                 methods=["GET", "PATCH"])
@flask_app.route(rule="/migration:verify",
                 methods=["POST"])
def handle_migration() -> Response:
    """
    Entry point for configuring the RDBMS-independent parameters, and for assessing migration readiness.

    Assessing the server's migration readiness means to verify whether its state and data
    are valid and consistent, thus allowing for a migration to be attempted.

    For metrics, these are the expected parameters:
        - *batch-size-in*: maximum number of rows to retrieve per batch (defaults to no maximum)
        - *batch-size-out*: maximum number of rows to output per batch (defaults to no maximum)
        - *chunk-size*: maximum size, in bytes, of data chunks in LOB data copying (defaults to 1048576)
        - *incremental-size*: maximum number of rows to migrate, for tables flagged for incremental migration

    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    scheme: dict[str, Any] = http_get_parameters(request=request)
    assert_params(errors=errors,
                  service=request.path,
                  method=request.method,
                  input_params=scheme)

    reply: dict[str, Any] | None = None
    if not errors:
        match request.method:
            case "GET":
                # retrieve the migration parameters
                reply = MigrationMetrics
            case "PATCH":
                # establish the migration parameters
                set_migration_metrics(errors=errors,
                                      input_params=scheme,
                                      logger=PYPOMES_LOGGER)
                if not errors:
                    reply = {"status": "Migration metrics updated"}
            case "POST":
                # assert whether migration is warranted
                assert_migration(errors=errors,
                                 inpt_params=scheme,
                                 run_mode=False)
                # errors ?
                if errors:
                    # yes, report the problem
                    reply = {"status": "Migration cannot be launched"}
                else:
                    # no, display the migration context
                    reply = get_migration_context(errors=errors,
                                                  input_params=scheme)
                    if reply:
                        reply["status"] = "Migration can be launched"

    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(f"Response {request.path}?{scheme}: {result}")

    return result


@flask_app.route(rule="/migrate",
                 methods=["DELETE", "POST"])
def handle_migrate() -> Response:
    """
    Initiate or abort a migration operation.

    :return: *Response* with the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    reply: dict[str, Any]
    if request.method == "POST":
        reply = migrate_data(errors=errors,
                             request=request)
    else:
        reply = abort_migration(errors=errors,
                                request=request)
    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(f"Response: {result}")

    return result


def migrate_data(errors: list[str],
                 request: Request) -> dict[str, Any]:
    """
    Migrate the specified schema/tables/views/indexes from the source to the target RDBMS.

    Sources and targets for the migration:
      - *from-rdbms*: the source RDBMS for the migration
      - *from-schema*: the source schema for the migration
      - *to-rdbms*: the destination RDBMS for the migration
      - *to-schema*: the destination schema for the migration
      - *to-s3*: optionally, the destination cloud storage for the LOBs

    Types of migration to be carried out:
      - *migrate-metadata*: migrate the schema's metadata (this creates or transforms the destination schema)
      - *migrate-plaindata*: migrate non-LOB data
      - *migrate-lobdata*: migrate LOBs (large binary objects)
      - *syncronize-plaindata*: make sure tables in target database have the same content as tables in source database

    Migration parameters:
      - *process-indexes*: whether to migrate indexes (defaults to *False*)
      - *process-views*: whether to migrate views (defaults to *False*)
      - *relax-reflection*: relaxes finding referenced tables at reflection (defaults to *False*)
      - *skip-nonempty*: prevents data migration for nonempty tables in the destination schema
      - *reflect-filetype*: attempts to reflect extensions for LOBs, on migration to S3 storage
      - *flatten-storage*: whether to omit path on LOB migration to S3 storage
      - *include-relations*: optional list of relations (tables, views, and indexes) to migrate
      - *exclude-relations*: optional list of relations (tables, views, and indexes) not to migrate
      - *exclude-constraints*: optional list of constraints not to migrate
      - *incremental-migration*: optional list of tables for which migration is to be carried out incrementally
      - *remove-nulls*: optional list of tables having columns with embedded NULLs in string data
      - *exclude-columns*: optional list of table columns not to migrate
      - *override-columns*: optional list of columns with forced migration types
      - *named-lobdata*: optional list of LOB columns and their associated names and extensions
      - *migration-badge*: optional name for session (used on JSON and log file creation)

    These are noteworthy:
      - the parameters *include-relations* and *exclude-relations* are mutually exclusive
      - if *migrate-plaindata* is set, it is assumed that metadata is also being migrated,
        or that all targeted tables in destination schema exist
      - if *migrate-lobdata* is set, and *to-s3* is not, it is assumed that plain data are also being,
        or have already been, migrated.

    :return: *Response* with the operation outcome
    """
    # initialize the return variable
    result: dict[str, Any] | None = None

    # retrieve and validate the input parameters
    input_params: dict[str, str] = http_get_parameters(request=request)
    assert_params(errors=errors,
                  service="/migrate",
                  method=request.method,
                  input_params=input_params)
    if not errors:
        # assert whether migration is warranted
        assert_migration(errors=errors,
                         inpt_params=input_params,
                         run_mode=True)

        # assert and retrieve the override columns parameter
        override_columns: dict[str, Type] = assert_override_columns(errors=errors,
                                                                    input_params=input_params)
        # assert and retrieve the incremental migrations parameter
        incremental_migrations: dict[str, tuple[int, int]] = assert_incremental_migrations(errors=errors,
                                                                                           input_params=input_params)
        # is migration possible ?
        if not errors:
            # yes, obtain the remaining migration parameters
            source_rdbms: DbEngine = validate_enum(errors=None,
                                                   source=input_params,
                                                   attr=MigrationConfig.FROM_RDBMS,
                                                   enum_class=DbEngine)
            target_rdbms: DbEngine = validate_enum(errors=None,
                                                   source=input_params,
                                                   attr=MigrationConfig.TO_RDBMS,
                                                   enum_class=DbEngine)
            target_s3: S3Engine = validate_enum(errors=None,
                                                source=input_params,
                                                attr=MigrationConfig.TO_S3,
                                                enum_class=S3Engine)

            source_schema: str = input_params.get(MigrationConfig.FROM_SCHEMA).lower()
            target_schema: str = input_params.get(MigrationConfig.TO_SCHEMA).lower()
            migration_badge: str = input_params.get(MigrationConfig.MIGRATION_BADGE)

            step_metadata: bool = validate_bool(errors=None,
                                                source=input_params,
                                                attr=MigrationConfig.MIGRATE_METADATA)
            step_plaindata: bool = validate_bool(errors=None,
                                                 source=input_params,
                                                 attr=MigrationConfig.MIGRATE_PLAINDATA)
            step_lobdata: bool = validate_bool(errors=None,
                                               source=input_params,
                                               attr=MigrationConfig.MIGRATE_LOBDATA)
            step_synchronize: bool = validate_bool(errors=None,
                                                   source=input_params,
                                                   attr=MigrationConfig.SYNCHRONIZE_PLAINDATA)
            process_indexes: bool = validate_bool(errors=None,
                                                  source=input_params,
                                                  attr=MigrationConfig.PROCESS_INDEXES)
            process_views: bool = validate_bool(errors=None,
                                                source=input_params,
                                                attr=MigrationConfig.PROCESS_VIEWS)
            relax_reflection: bool = validate_bool(errors=None,
                                                   source=input_params,
                                                   attr=MigrationConfig.RELAX_REFLECTION)
            skip_nonempty: bool = validate_bool(errors=None,
                                                source=input_params,
                                                attr=MigrationConfig.SKIP_NONEMPTY)
            reflect_filetype: bool = validate_bool(errors=None,
                                                   source=input_params,
                                                   attr=MigrationConfig.REFLECT_FILETYPE)
            flatten_storage: bool = validate_bool(errors=None,
                                                  source=input_params,
                                                  attr=MigrationConfig.FLATTEN_STORAGE)
            remove_nulls: list[str] = [s.lower()
                                       for s in validate_strs(errors=None,
                                                              source=input_params,
                                                              attr=MigrationConfig.REMOVE_NULLS)]
            include_relations: list[str] = [s.lower()
                                            for s in validate_strs(errors=None,
                                                                   source=input_params,
                                                                   attr=MigrationConfig.INCLUDE_RELATIONS)]
            exclude_relations: list[str] = [s.lower()
                                            for s in validate_strs(errors=None,
                                                                   source=input_params,
                                                                   attr=MigrationConfig.EXCLUDE_RELATIONS)]
            exclude_columns: list[str] = [s.lower()
                                          for s in validate_strs(errors=None,
                                                                 source=input_params,
                                                                 attr=MigrationConfig.EXCLUDE_COLUMNS)]
            exclude_constraints: list[str] = [s.lower()
                                              for s in validate_strs(errors=None,
                                                                     source=input_params,
                                                                     attr=MigrationConfig.EXCLUDE_CONSTRAINTS)]
            named_lobdata: list[str] = [s.lower()
                                        for s in validate_strs(errors=None,
                                                               source=input_params,
                                                               attr=MigrationConfig.NAMED_LOBDATA)]
            # migrate the data
            result = migrate(errors=errors,
                             source_rdbms=source_rdbms,
                             target_rdbms=target_rdbms,
                             source_schema=source_schema,
                             target_schema=target_schema,
                             target_s3=target_s3,
                             step_metadata=step_metadata,
                             step_plaindata=step_plaindata,
                             step_lobdata=step_lobdata,
                             step_synchronize=step_synchronize,
                             process_indexes=process_indexes,
                             process_views=process_views,
                             relax_reflection=relax_reflection,
                             skip_nonempty=skip_nonempty,
                             reflect_filetype=reflect_filetype,
                             flatten_storage=flatten_storage,
                             incremental_migrations=incremental_migrations,
                             remove_nulls=remove_nulls,
                             include_relations=include_relations,
                             exclude_relations=exclude_relations,
                             exclude_columns=exclude_columns,
                             exclude_constraints=exclude_constraints,
                             named_lobdata=named_lobdata,
                             override_columns=override_columns,
                             migration_badge=migration_badge,
                             app_name=APP_NAME,
                             app_version=APP_VERSION,
                             logger=PYPOMES_LOGGER)
    return result


def abort_migration(errors: list[str],
                    request: Request) -> dict[str, Any]:
    """
    Abort the ongoing migration specified by the *migration-badge* input parameter.

    :return: the operation outcome
    """
    # initialize the return variable
    result: dict[str, Any] | None = None

    # retrieve and validate the input parameters
    input_params: dict[str, str] = http_get_parameters(request=request)
    assert_params(errors=errors,
                  service="/migrate",
                  method=request.method,
                  input_params=input_params)
    if not errors:
        migration_badge = assert_migration_badge(errors=errors,
                                                 input_params=input_params)
        if migration_badge:
            OngoingMigrations.remove(migration_badge)
            result = {
                "status": f"Migration '{migration_badge}' marked for abortion"
            }

    return result


@flask_app.errorhandler(code_or_exception=Exception)
def handle_exception(exc: Exception) -> Response:
    """
    Handle exceptions raised when responding to requests, but not handled.

    :return: status 500, with JSON containing the errors.
    """
    # import the needed exception
    from werkzeug.exceptions import NotFound

    # declare the return variable
    result: Response

    # is the exception an instance of werkzeug.exceptions.NotFound ?
    if isinstance(exc, NotFound):
        # yes, disregard it (with status 'No content')
        # (handles a bug causing the re-submission of a GET request from a browser)
        result = Response(status=204)
    else:
        # no, report the problem
        err_msg: str = exc_format(exc=exc,
                                  exc_info=sys.exc_info())
        PYPOMES_LOGGER.error(msg=f"{err_msg}")
        reply: dict = {
            "errors": [err_msg]
        }
        json_str: str = json.dumps(obj=reply,
                                   ensure_ascii=False)
        result = Response(response=json_str,
                          status=500,
                          mimetype="application/json")
    return result


def _build_response(errors: list[str],
                    reply: dict) -> Response:

    # declare the return variable
    result: Response

    if errors:
        reply_err: dict = {"errors": validate_format_errors(errors=errors)}
        if isinstance(reply, dict):
            reply_err.update(reply)
        result = jsonify(reply_err)
        result.status_code = 400
    else:
        # 'reply' might be 'None'
        result = jsonify(reply)

    return result


if __name__ == "__main__":

    flask_app.run(host="0.0.0.0",
                  port=5000,
                  debug=False)
