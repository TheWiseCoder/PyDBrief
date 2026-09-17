import json
import logging
import os
import sys
from enum import StrEnum
from flask import (
    Blueprint, Flask, Request, Response,
    request, jsonify, send_file
)
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from pathlib import Path
from typing import Any, Final

# must be imported before PyPomes and local packages
from app_ident import APP_NAME, APP_VERSION, get_env_keys

from pypomes_core import (
    Mimetype, pypomes_versions,
    dict_clone, dict_jsonify,
    exc_format, validate_format_errors
)
from pypomes_http import (
    HttpMethod, HttpStatus, http_get_parameters
)
from pypomes_logging import (
    PYPOMES_LOGGER,
    logging_get_params, logging_log_forward, service_logging
)

from app_constants import InputParam
from migration import pydb_migration
from storage.database_actions import (
    create_database, update_database, delete_database, retrieve_databases
)
from storage.migration_actions import (
    create_migration, update_migration, delete_migration,
    retrieve_migrations, verify_migration
)
from storage.s3_actions import (
    create_s3, update_s3, delete_s3, retrieve_s3s
)
from storage.session_actions import (
    create_session, update_session, delete_session, retrieve_sessions
)

# -------------------------------------------------------------------- #
from app_constants_old import MigConfig, MigSpec
from migration.pydb_sessions import get_session_params, get_session_registry, abort_session_migration
from migration.pydb_migrator import migrate
from migration.pydb_validator import (
    assert_expected_params,
    validate_metrics, validate_spots, validate_steps, validate_specs
)
# -------------------------------------------------------------------- #


# create the Flask application
flask_app: Final[Flask] = Flask(__name__)

# support cross-origin resource sharing
CORS(flask_app)

# set the logging endpoint
flask_app.add_url_rule(rule="/logging",
                       endpoint="logging",
                       view_func=service_logging,
                       methods=[HttpMethod.GET, HttpMethod.POST])

# make PyDBrief's REST API available as a Swagger app
swagger_blueprint: Blueprint = get_swaggerui_blueprint(
    base_url="/apidocs",
    api_url="/swagger",
    config={"defaultModelsExpandDepth": -1}
)
flask_app.register_blueprint(blueprint=swagger_blueprint)

# configure 'jsonify()' with 'ensure_ascii=False'
flask_app.config["JSON_AS_ASCII"] = False

# forward SQLAlchemy's logging activity to PYPOMES_LOGGER
if os.getenv("PYDB_LOG_SQLALCHEMY") == "1":
    logger: logging.Logger = logging.getLogger("sqlalchemy.engine")
    logging_log_forward(source_logger=logger,
                        target_logger=PYPOMES_LOGGER)
    logger = logging.getLogger("sqlalchemy.dialects")
    logging_log_forward(source_logger=logger,
                        target_logger=PYPOMES_LOGGER)


@flask_app.route(rule="/swagger",
                 methods=[HttpMethod.GET])
def service_swagger() -> Response:
    """
    Entry point for the microservice providing OpenAPI specifications in the Swagger standard.

    The optional *filename* parameter specifies the name of the file to be written to by the browser.
    If omitted, the browser is asked to only display the returned content.

    :return: the requested OpenAPI specifications
    """
    # retrieve the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)

    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    filename: str = input_params.get("filename")
    filepath: Path = Path(Path.cwd(), "swagger/pydbrief.json")
    result: Response = send_file(path_or_file=filepath,
                                 mimetype=Mimetype.JSON,
                                 as_attachment=filename is not None,
                                 download_name=filename)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/version",
                 methods=[HttpMethod.GET])
def service_version() -> Response:
    """
    Obtain the current version of *PyDBrief*, along with the foundation modules in use.

    :return: the versions in execution
    """
    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    # retrieve the versions
    env_keys: list[str] = get_env_keys()
    versions: dict[str, Any] = {
        APP_NAME: {
            "version": APP_VERSION,
            "base-url": f"{request.scheme}://{request.host}",
            "requester": request.headers.get("X-Forwarded-For",
                                             request.remote_addr)
        },
        "foundations": pypomes_versions(),
        "environment": {key: value for key, value in os.environ.items()
                        if key in env_keys and not ("_PWD" in key or "_SECRET" in key)},
        "logging": dict_jsonify(source=logging_get_params())
    }
    # assign to the return variable
    result: Response = jsonify(versions)

    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/favicon.ico",
                 methods=[HttpMethod.GET])
def service_ignore() -> Response:
    """
    Handle irrelevant browser requests.

    :return: *Response* with status *NO CONTENT*
    """
    return Response(status=HttpStatus.NO_CONTENT)


@flask_app.route(rule="/database",
                 methods=[HttpMethod.GET, HttpMethod.POST])
@flask_app.route(rule="/database/<db_engine>",
                 methods=[HttpMethod.DELETE, HttpMethod.GET, HttpMethod.PATCH])
def service_database(db_engine: str = None) -> Response:
    """
    Entry point for handling database engines to use.

    The parameters are as follows:
      - *db-engine*: identifies the database engine instance
      - *db-type*: the type of the database engine (*mysql*, *oracle*, *postgres*, or *sqlserver*)
      - *db-name*: name of database
      - *db-user*: the logon user
      - *db-pwd*: the logon password
      - *db-host*: the host URL
      - *db-port*: the connection port
      - *db-client*: the client package (Oracle, only)
      - *db-driver*: the database access driver (SQLServer, only)

    :param db_engine: the identification of the database engine instance
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    if db_engine:
        input_params[InputParam.DB_ENGINE] = db_engine

    # log the request
    msg: str = __log_init(request=request,
                          input_params=dict_clone(source=input_params,
                                                  from_to_keys=[key for key in input_params
                                                                if key != InputParam.DB_PWD]))
    PYPOMES_LOGGER.info(msg=msg)

    reply: dict[StrEnum | str, Any] | None = None
    match request.method:
        case HttpMethod.GET:
            reply = retrieve_databases(input_params=input_params,
                                       errors=errors)
        case HttpMethod.POST:
            create_database(input_params=input_params,
                            errors=errors)
        case HttpMethod.PATCH:
            update_database(input_params=input_params,
                            errors=errors)
        case HttpMethod.DELETE:
            delete_database(input_params=input_params,
                            errors=errors)

    # build the response
    result: Response = _build_response(reply=reply,
                                       errors=errors)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/s3",
                 methods=[HttpMethod.GET, HttpMethod.POST])
@flask_app.route(rule="/s3/<s3_engine>",
                 methods=[HttpMethod.DELETE, HttpMethod.GET, HttpMethod.PATCH])
def service_s3(s3_engine: str = None) -> Response:
    """
    Entry point for handling S3 engines to use.

    The parameters are as follows:
      - *s3-engine*: identifies the S3 engine instance
      - *s3-type*: the type of the S3 engine (*aws* or *minio*)
      - *s3-endpoint-url*: the access URL for the service
      - *s3-bucket-name*: the name of the default bucket
      - *s3-access-key*: the access key for the service
      - *s3-secret-key*: the access secret code
      - *s3-region-name*: the name of the region where the engine is located (AWS only)
      - *s3-secure-access*: whether to use Transport Security Layer (MinIO only)

    :param s3_engine: the identification of the database engine instance
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    if s3_engine:
        input_params[InputParam.S3_ENGINE] = s3_engine

    # log the request
    msg: str = __log_init(request=request,
                          input_params=dict_clone(source=input_params,
                                                  from_to_keys=[key for key in input_params
                                                                if key != InputParam.S3_SECRET_KEY]))
    PYPOMES_LOGGER.info(msg=msg)

    reply: dict[StrEnum | str, Any] | None = None
    match request.method:
        case HttpMethod.GET:
            reply = retrieve_s3s(input_params=input_params,
                                 errors=errors)
        case HttpMethod.POST:
            create_s3(input_params=input_params,
                      errors=errors)
        case HttpMethod.PATCH:
            update_s3(input_params=input_params,
                      errors=errors)
        case HttpMethod.DELETE:
            delete_s3(input_params=input_params,
                      errors=errors)

    # build the response
    result: Response = _build_response(reply=reply,
                                       errors=errors)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/session",
                 methods=[HttpMethod.GET, HttpMethod.POST])
@flask_app.route(rule="/session/<cd_session>",
                 methods=[HttpMethod.DELETE, HttpMethod.GET, HttpMethod.PATCH])
def service_session(cd_session: str = None) -> Response:
    """
    Entry point for handling migration sessions.

    The parameters are as follows:
      - *cd-session*: identifies the migration session instance
      - *source-db*: the instance of the database engine used as source
      - *target-db*: the instance of the database engine used as target
      - *target-s3*: the instance of the S3 engine used as target
      - *source-schema*: name of schema in source database
      - *target-schema*: name of schema in target database

    :param cd_session: the identification of the migration session instance
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    if cd_session:
        input_params[InputParam.CD_SESSION] = cd_session

    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    reply: dict[StrEnum | str, Any] | None = None
    match request.method:
        case HttpMethod.GET:
            reply = retrieve_sessions(input_params=input_params,
                                      errors=errors)
        case HttpMethod.POST:
            create_session(input_params=input_params,
                           errors=errors)
        case HttpMethod.PATCH:
            update_session(input_params=input_params,
                           errors=errors)
        case HttpMethod.DELETE:
            delete_session(input_params=input_params,
                           errors=errors)

    # build the response
    result: Response = _build_response(reply=reply,
                                       errors=errors)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/migration",
                 methods=[HttpMethod.GET, HttpMethod.POST])
@flask_app.route(rule="/migration/<nm_badge>",
                 methods=[HttpMethod.DELETE, HttpMethod.GET, HttpMethod.PATCH])
@flask_app.route(rule="/migration:verify/<nm_badge>",
                 methods=[HttpMethod.GET])
def service_migration(nm_badge: str = None) -> Response:
    """
    Entry point for handling migrations.

    The parameters are as follows:
      - *badge*: identifies the migration instance
      - *session*: the session the migration belongs to
      - *step*: the migration step (one from the list below)
      - *batch-size-in*: maximum number of rows to retrieve per batch
      - *batch-size-out*: maximum number of rows to output per batch
      - *chunk-size*: maximum size, in bytes, of data chunks in LOB data copying
      - *exclude-columns*: optional list of table columns not to migrate
      - *exclude-constraints*: optional list of constraints not to migrate
      - *exclude-relations*: optional list of relations (tables, views, and indexes) not to migrate
      - *flatten-storage*: whether to omit path on LOB migration to S3 storage
      - *include-relations*: optional list of relations (tables, views, and indexes) to migrate
      - *incremental-migration*: optional list of tables for which migration is to be carried out incrementally
      - *incremental-size*: maximum number of rows to migrate, for tables flagged for incremental migration
      - *lobdata-channels*: number of simultaneous channels to use in lobdata migration
      - *lobdata-channel-size*: size of channels used in lobdata migration
      - *named-lobdata*: optional list of LOB columns and their associated names and extensions
      - *omit_defaults*: optional list of columns whose default values are to be imitted
      - *optimize-pks*: optimizes the type donversion for primary keys which are not foreign keys
      - *override-columns*: optional list of columns with forced migration types
      - *plaindata-channels*: number of simultaneous channels to use in plaindata migration
      - *plaindata-channel-size*: size of channels used in plaindata migration
      - *process-indexes*: whether to migrate indexes (defaults to *False*)
      - *process-views*: whether to migrate views (defaults to *False*)
      - *reflect-filetype*: attempts to reflect extensions for LOBs, on migration to S3 storage
      - *relax-reflection*: relaxes finding referenced tables at reflection (defaults to *False*)
      - *remove-ctrlchars*: optional list of tables having columns with embedded control characters in string data
      - *skip-nonempty*: prevents data migration for nonempty tables in the destination schema

    Steps of migration:
      - *migrate-metadata*: migrate the schema's metadata
      - *migrate-plaindata*: migrate non-LOB data
      - *migrate-lobdata*: migrate LOBs (large binary objects)
      - *correlate-plaindata*: make sure tables in target and source databases have the same PK content
      - *correlate-lobdata*: make sure folders in target S3 have the same entries as in in source database
      - *syncronize-plaindata*: make sure tables in target and source databases have the same tuple content

    :param nm_badge: the identification of the migration instance
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    if nm_badge:
        input_params[InputParam.BADGE] = nm_badge

    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    reply: dict[StrEnum | str, Any] | None = None
    if request.path.startswith("/migration:verify"):
        verify_migration(input_params=input_params,
                         errors=errors)
    else:
        match request.method:
            case HttpMethod.GET:
                reply = retrieve_migrations(input_params=input_params,
                                            errors=errors)
            case HttpMethod.POST:
                create_migration(input_params=input_params,
                                 errors=errors)
            case HttpMethod.PATCH:
                update_migration(input_params=input_params,
                                 errors=errors)
            case HttpMethod.DELETE:
                delete_migration(input_params=input_params,
                                 errors=errors)
    # build the response
    result: Response = _build_response(reply=reply,
                                       errors=errors)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/migrate/<nm_badge>",
                 methods=[HttpMethod.GET])
def service_migrate(nm_badge: str = None) -> Response:
    """
    Initiate or abort a migration operation.

    :return: *Response* with the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = http_get_parameters(request=request)
    if nm_badge:
        input_params[InputParam.BADGE] = nm_badge

    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    reply: dict[StrEnum | str, Any] = None


@flask_app.route(rule="/migrate",
                 methods=[HttpMethod.POST])
@flask_app.route(rule="/migrate/<session_id>",
                 methods=[HttpMethod.DELETE])
def service_migrate_old(session_id: str = None) -> Response:
    """
    Initiate or abort a migration operation.

    :return: *Response* with the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, str] = get_session_params(request=request,
                                                      session_id=session_id,
                                                      errors=errors)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    assert_expected_params(service="/migrate",
                           method=request.method,
                           input_params=input_params,
                           errors=errors)

    session_id: str = input_params.get(MigSpec.SESSION_ID)
    reply: dict[str, Any] | None = None
    if not errors:
        if request.method == HttpMethod.POST:
            reply = migrate_data(session_id=session_id,
                                 input_params=input_params,
                                 base_url=f"{request.scheme}://{request.host}",
                                 requester=request.headers.get("X-Forwarded-For",
                                                               request.remote_addr),
                                 errors=errors)
        elif abort_session_migration(session_id=session_id,
                                     errors=errors):
            reply = {
                "status": f"Migration in session '{session_id}' marked for abortion"
            }

    # build the response
    result: Response = _build_response_old(client_id=input_params.get(MigSpec.CLIENT_ID),
                                           reply=reply,
                                           errors=errors)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


def migrate_data(session_id: str,
                 input_params: dict[str, Any],
                 base_url: str,
                 requester: str,
                 errors: list[str]) -> dict[str, Any]:
    """
    Migrate the specified schema/tables/views/indexes from the source to the target RDBMS.

    Sources and targets for the migration:
      - *from-rdbms*: the source RDBMS for the migration
      - *from-schema*: the source schema for the migration
      - *to-rdbms*: the destination RDBMS for the migration
      - *to-schema*: the destination schema for the migration
      - *to-s3*: optionally, the destination cloud storage for the LOBs

    Steps of migration to be carried out:
      - *migrate-metadata*: migrate the schema's metadata (this creates or transforms the destination schema)
      - *migrate-plaindata*: migrate non-LOB data
      - *migrate-lobdata*: migrate LOBs (large binary objects)
      - *correlate-plaindata*: make sure tables in target and source databases have the same PK content
      - *correlate-lobdata*: make sure folders in target S3 have the same entries as in in source database
      - *syncronize-plaindata*: make sure tables in target and source databases have the same tuple content

    Migration specs:
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
      - *remove-ctrlchars*: optional list of tables having columns with embedded control characters in string data
      - *exclude-columns*: optional list of table columns not to migrate
      - *override-columns*: optional list of columns with forced migration types
      - *omit_defaults*: optional list of columns whose default values are to be imitted
      - *optimize-pks*: optimizes the type donversion for primary keys which are not foreign keys
      - *named-lobdata*: optional list of LOB columns and their associated names and extensions
      - *migration-badge*: optional name for migration (used on JSON and log file creation)
      - *session-id*: optional session identification, defaults to the client's active session

    These are noteworthy:
      - the parameters *include-relations* and *exclude-relations* are mutually exclusive
      - if *migrate-plaindata* is set, it is assumed that metadata is also being, or has already been, migrated
      - if *migrate-lobdata* is set, and *to-s3* is not, it is assumed that plain data are also being,
        or have already been, migrated.

    :param session_id: the session identification
    :param input_params: the input parameters
    :param base_url: PyDBrief's base access address
    :param requester: origin of the reuest
    :param errors: incidental errors
    :return: *Response* with the operation outcome
    """
    # initialize the return variable
    result: dict[str, Any] | None = None

    # validate the migration spots
    validate_spots(input_params=input_params,
                   session_id=session_id,
                   errors=errors)

    # validate the migration steps
    validate_steps(input_params=input_params,
                   session_id=session_id,
                   errors=errors)

    # validate the migration specs
    validate_specs(input_params=input_params,
                   session_id=session_id,
                   errors=errors)

    # validate the migration metrics
    validate_metrics(input_params=get_session_registry(session_id=session_id)[MigConfig.METRICS],
                     session_id=session_id,
                     errors=errors)

    # is migration possible ?
    if not errors:
        # yes, migrate the data
        result = migrate(session_id=session_id,
                         app_name=APP_NAME,
                         base_url=base_url,
                         requester=requester,
                         app_version=APP_VERSION,
                         errors=errors,
                         logger=PYPOMES_LOGGER)
    return result


@flask_app.errorhandler(code_or_exception=Exception)
def handle_exception(exc: Exception) -> Response:
    """
    Handle exceptions raised when responding to requests, but not handled.

    :return: status 500, with JSON containing the errors
    """
    # import the needed exception
    from werkzeug.exceptions import NotFound

    # declare the return variable
    result: Response

    # log the request
    input_params: dict[str, Any] = http_get_parameters(request=request)
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    # is the exception an instance of werkzeug.exceptions.NotFound ?
    if isinstance(exc, NotFound):
        # yes, disregard it
        result = Response(status=HttpStatus.NO_CONTENT)
    else:
        # no, report the problem
        err_msg: str = exc_format(exc=exc,
                                  exc_info=sys.exc_info())
        PYPOMES_LOGGER.error(msg=err_msg)
        reply: dict = {"errors": [err_msg]}
        result = jsonify(reply)
        result.status_code = HttpStatus.INTERNAL_SERVER_ERROR

    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


def _build_response(reply: dict,
                    errors: list[str]) -> Response:

    # declare the return variable
    result: Response

    if errors:
        reply_err: dict = {"errors": validate_format_errors(errors)}
        if isinstance(reply, dict):
            reply_err.update(reply)
        result = jsonify(reply_err)
        result.status_code = HttpStatus.BAD_REQUEST
    else:
        if reply:
            result = jsonify(reply)
        else:
            result = Response(status=HttpStatus.NO_CONTENT)
    return result


def _build_response_old(client_id: str,
                        reply: dict,
                        errors: list[str]) -> Response:

    # declare the return variable
    result: Response

    if errors:
        reply_err: dict = {"errors": validate_format_errors(errors)}
        if isinstance(reply, dict):
            reply_err.update(reply)
        result = jsonify(reply_err)
        result.status_code = HttpStatus.BAD_REQUEST
    else:
        # 'reply' might be 'None'
        result = jsonify(reply)
    result.set_cookie(key="client-id",
                      value=client_id)
    return result


def __log_init(request: Request,
               input_params: dict) -> str:

    params: str = json.dumps(obj=input_params,
                             ensure_ascii=False)
    return f"Request {request.method}:{request.path}, params {params}"


if __name__ == "__main__":

    flask_app.run(host="0.0.0.0",
                  port=5000,
                  debug=False)
