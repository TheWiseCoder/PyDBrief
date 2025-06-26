import json
import sys
from copy import deepcopy
from enum import StrEnum
from flask import (
    Blueprint, Flask, Request, Response,
    request, jsonify, send_file
)
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from pathlib import Path
from typing import Any, Final

from app_ident import APP_NAME, APP_VERSION  # must be imported before PyPomes and local packages
from pypomes_core import (
    Mimetype, pypomes_versions, dict_clone, dict_pop_all,
    exc_format, validate_enum, validate_format_errors
)
from pypomes_db import DbEngine
from pypomes_http import (
    HttpMethod, HttpStatus, http_get_parameters
)
from pypomes_logging import PYPOMES_LOGGER, service_logging
from pypomes_s3 import S3Engine

from app_constants import (
    DbConfig, S3Config, MigrationState,
    MigConfig, MigSpec, MigSpot
)
from migration.pydb_common import get_rdbms_specs, get_s3_specs
from migration.pydb_sessions import (
    create_session, delete_session, set_session_state,
    get_sessions, get_session_params, get_session_registry, abort_session_migration
)
from migration.pydb_migrator import migrate
from migration.pydb_validator import (
    assert_expected_params,
    validate_metrics, validate_rdbms, validate_s3,
    validate_spots, validate_steps, validate_specs
)

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
    result: Response = send_file(path_or_file=Path(Path.cwd(), "swagger/pydbrief.json"),
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
    versions: dict[str, Any] = {
        APP_NAME: APP_VERSION,
        "foundations": pypomes_versions()
    }
    # assign to the return variable
    result: Response = jsonify(versions)

    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/rdbms",
                 methods=[HttpMethod.POST])
@flask_app.route(rule="/rdbms/<engine>",
                 methods=[HttpMethod.GET])
def service_rdbms(engine: str = None) -> Response:
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
    input_params: dict[str, Any] = get_session_params(errors=errors,
                                                      request=request)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=dict_clone(source=input_params,
                                                  from_to_keys=[key for key in input_params
                                                                if key != DbConfig.PWD]))
    PYPOMES_LOGGER.info(msg=msg)

    assert_expected_params(errors=errors,
                           service="/rdbms",
                           method=request.method,
                           input_params=input_params)

    reply: dict[StrEnum | str, Any] | None = None
    if not errors:
        session_id: str = input_params.get(MigSpec.SESSION_ID)
        if request.method == HttpMethod.GET:
            input_params[DbConfig.ENGINE] = engine
            db_engine: DbEngine = validate_enum(errors=errors,
                                                source=input_params,
                                                attr=DbConfig.ENGINE,
                                                enum_class=DbEngine,
                                                required=True)
            if db_engine:
                # get RDBMS connection params
                reply = get_rdbms_specs(errors=errors,
                                        session_id=session_id,
                                        db_engine=db_engine)
        else:
            validate_rdbms(errors=errors,
                           input_params=input_params)
            if not errors:
                engine = input_params.get(DbConfig.ENGINE)
                reply = {"status": f"RDBMS '{engine}' configuration updated"}

        if reply:
            reply[MigSpec.SESSION_ID] = session_id

    # build the response
    result: Response = _build_response(errors=errors,
                                       client_id=input_params.get(MigSpec.CLIENT_ID),
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/s3",
                 methods=[HttpMethod.POST])
@flask_app.route(rule="/s3/<engine>",
                 methods=[HttpMethod.GET])
def service_s3(engine: str = None) -> Response:
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
    input_params: dict[str, Any] = get_session_params(errors=errors,
                                                      request=request)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=dict_clone(source=input_params,
                                                  from_to_keys=[key for key in input_params
                                                                if key != S3Config.SECRET_KEY]))
    PYPOMES_LOGGER.info(msg=msg)

    assert_expected_params(errors=errors,
                           service="/s3",
                           method=request.method,
                           input_params=input_params)

    reply: dict[S3Config | str, Any] | None = None
    if not errors:
        session_id: str = input_params.get(MigSpec.SESSION_ID)
        if request.method == HttpMethod.GET:
            input_params[S3Config.ENGINE] = engine
            s3_engine: S3Engine = validate_enum(errors=errors,
                                                source=input_params,
                                                attr=S3Config.ENGINE,
                                                enum_class=S3Engine,
                                                required=True)
            if s3_engine:
                # get S3 access params
                reply = get_s3_specs(errors=errors,
                                     session_id=session_id,
                                     s3_engine=s3_engine)
        else:
            # configure the S3 service
            validate_s3(errors=errors,
                        input_params=input_params)
            if not errors:
                engine = input_params.get(S3Config.ENGINE)
                reply = {"status": f"S3 '{engine}' configuration updated"}
        if reply:
            reply[MigSpec.SESSION_ID] = session_id

    # build the response
    result: Response = _build_response(errors=errors,
                                       client_id=input_params.get(MigSpec.CLIENT_ID),
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/sessions",
                 methods=[HttpMethod.GET])
@flask_app.route(rule="/sessions/<session_id>",
                 methods=[HttpMethod.DELETE, HttpMethod.GET,
                          HttpMethod.PATCH, HttpMethod.POST])
def service_sessions(session_id: str = None) -> Response:
    """
    Entry point for handling migration sessions.

    :param session_id: the session identification
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = get_session_params(errors=errors,
                                                      request=request,
                                                      session_id=session_id)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    assert_expected_params(errors=errors,
                           service="/sessions",
                           method=request.method,
                           input_params=input_params)

    reply: dict[str, Any] | None = None
    client_id: str = input_params.get(MigSpec.CLIENT_ID)
    if not errors:
        if session_id:
            # session_id is in path
            match request.method:
                case HttpMethod.DELETE:
                    if delete_session(errors=errors,
                                      session_id=session_id):
                        reply = {"status": f"Session '{session_id}' deleted"}
                case HttpMethod.GET:
                    reply = deepcopy(x=get_session_registry(session_id=session_id))
                    dict_pop_all(target=reply,
                                 key=DbConfig.PWD)
                    dict_pop_all(target=reply,
                                 key=S3Config.SECRET_KEY)
                case HttpMethod.PATCH:
                    state: MigrationState = set_session_state(errors=errors,
                                                              input_params=input_params)
                    if state:
                        reply = {"status": f"Session '{session_id}' set to '{state}'"}
                case HttpMethod.POST:
                    if create_session(errors=errors,
                                      client_id=client_id,
                                      session_id=session_id):
                        reply = {"status": f"Session '{session_id}' created and set to '{MigrationState.ACTIVE}'"}
        else:
            # session_id is not in path
            reply = get_sessions()
            reply["client"] = client_id

    # build the response
    result: Response = _build_response(errors=errors,
                                       client_id=client_id,
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/migration:verify",
                 methods=[HttpMethod.POST])
@flask_app.route(rule="/migration/metrics",
                 methods=[HttpMethod.GET, HttpMethod.PATCH])
def service_migration() -> Response:
    """
    Entry point for configuring migration metrics, and for assessing migration readiness.

    Assessing the server's migration readiness means to verify whether its state and data
    are valid and consistent, thus allowing for a migration to be attempted.

    For metrics, these are the expected parameters:
        - *batch-size-in*: maximum number of rows to retrieve per batch (defaults to no maximum)
        - *batch-size-out*: maximum number of rows to output per batch (defaults to no maximum)
        - *chunk-size*: maximum size, in bytes, of data chunks in LOB data copying (defaults to 1048576)
        - *incremental-size*: maximum number of rows to migrate, for tables flagged for incremental migration
        - *plaindata-channels*: number of simultaneous channels to use in plaindata migrations
        - *lobdata-channels*: number of simultaneous channels to use in lobdata migrations

    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, Any] = get_session_params(errors=errors,
                                                      request=request)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    client_id: str = input_params.get(MigSpec.CLIENT_ID)
    assert_expected_params(errors=errors,
                           service=request.path,
                           method=request.method,
                           input_params=input_params)

    reply: dict[str, Any] | None = None
    if not errors:
        session_id = input_params.get(MigSpec.SESSION_ID)
        match request.path:
            case "/migration:verify":
                # assert whether migration is warranted
                validate_spots(errors=errors,
                               input_params=input_params)
                # errors ?
                if errors:
                    # yes, report the problem
                    reply = {
                        "status": "Migration cannot be launched",
                    }
                else:
                    # no, build the migration context
                    reply = {
                        MigSpot.FROM_RDBMS: get_rdbms_specs(errors=errors,
                                                            session_id=session_id,
                                                            db_engine=input_params[MigSpot.FROM_RDBMS]),
                        MigSpot.TO_RDBMS: get_rdbms_specs(errors=errors,
                                                          session_id=session_id,
                                                          db_engine=input_params[MigSpot.TO_RDBMS]),
                        "status": "Migration can be launched"
                    }
                    to_s3: S3Engine = input_params.get(MigSpot.TO_S3)
                    if to_s3:
                        reply[MigSpot.TO_S3] = get_s3_specs(errors=errors,
                                                            session_id=session_id,
                                                            s3_engine=to_s3)
            case "/migration/metrics":
                match request.method:
                    case HttpMethod.GET:
                        # obtain a copy of the metrics parameters
                        reply = get_session_registry(session_id=session_id)[MigConfig.METRICS].copy()
                    case HttpMethod.PATCH:
                        # establish the metrics parameters
                        validate_metrics(errors=errors,
                                         input_params=input_params)
                        if not errors:
                            reply = {"status": f"Migration metrics updated"}
        if reply:
            reply[MigSpec.SESSION_ID] = session_id

    # build the response
    result: Response = _build_response(errors=errors,
                                       client_id=client_id,
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


@flask_app.route(rule="/migrate",
                 methods=[HttpMethod.POST])
@flask_app.route(rule="/migrate/<session_id>",
                 methods=[HttpMethod.DELETE])
def service_migrate(session_id: str = None) -> Response:
    """
    Initiate or abort a migration operation.

    :return: *Response* with the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve and validate the input parameters
    input_params: dict[str, str] = get_session_params(errors=errors,
                                                      request=request,
                                                      session_id=session_id)
    # log the request
    msg: str = __log_init(request=request,
                          input_params=input_params)
    PYPOMES_LOGGER.info(msg=msg)

    assert_expected_params(errors=errors,
                           service="/migrate",
                           method=request.method,
                           input_params=input_params)

    session_id: str = input_params.get(MigSpec.SESSION_ID)
    reply: dict[str, Any] | None = None
    if not errors:
        if request.method == HttpMethod.POST:
            reply = migrate_data(errors=errors,
                                 input_params=input_params)
        else:
            if abort_session_migration(errors=errors,
                                       session_id=session_id):
                reply = {
                    "status": f"Migration in session '{session_id}' marked for abortion"
                }

    # build the response
    result: Response = _build_response(errors=errors,
                                       client_id=input_params.get(MigSpec.CLIENT_ID),
                                       reply=reply)
    # log the response
    PYPOMES_LOGGER.info(msg=f"Response {result}")

    return result


def migrate_data(errors: list[str],
                 input_params: dict[str, Any]) -> dict[str, Any]:
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
      - *migration-badge*: optional name for migration (used on JSON and log file creation)
      - *session-id*: optional session identification, defaults to the client's active session

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

    # validate the migration spots
    validate_spots(errors=errors,
                   input_params=input_params)

    # validate the migration steps
    validate_steps(errors=errors,
                   input_params=input_params)

    # validate the migration specs
    validate_specs(errors=errors,
                   input_params=input_params)

    # is migration possible ?
    if not errors:
        # yes, proceed

        # obtain the remaining migration parameters
        session_id: str = input_params.get(MigSpec.SESSION_ID)

        # migrate the data
        result = migrate(errors=errors,
                         session_id=session_id,
                         app_name=APP_NAME,
                         app_version=APP_VERSION,
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
        #   - handle a bug causing the re-submission of a GET request from a browser
        #   - ignore a 'GET:/favicon.ico' request from a browser
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


def _build_response(errors: list[str],
                    client_id: str,
                    reply: dict) -> Response:

    # declare the return variable
    result: Response

    if errors:
        reply_err: dict = {"errors": validate_format_errors(errors=errors)}
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
