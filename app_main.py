import json
import os
import sys
from flask import (
    Blueprint, Flask, Response, jsonify, request, send_from_directory
)
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from pathlib import Path
from sqlalchemy.sql.elements import Type
from typing import Any, Final

os.environ["PYPOMES_APP_PREFIX"] = "PYDB"
os.environ["PYDB_VALIDATION_MSG_PREFIX"] = ""

# ruff: noqa: E402
from pypomes_core import (
    get_versions, dict_jsonify, exc_format,
    str_lower, str_as_list, validate_format_errors
)  # noqa: PyPep8
from pypomes_http import (
    http_get_parameter, http_get_parameters
)  # noqa: PyPep8
from pypomes_logging import (
    PYPOMES_LOGGER,
    logging_service, logging_log_info, logging_log_error)  # noqa: PyPep8

from migration.pydb_common import (
    get_s3_params, set_s3_params,
    get_rdbms_params, set_rdbms_params,
    get_migration_metrics, set_migration_metrics
)  # noqa: PyPep8
from migration.pydb_migrator import migrate  # noqa: PyPep8
from migration.pydb_validator import (
    assert_column_types, assert_migration, get_migration_context
)  # noqa: PyPep8

# create the Flask application
app: Flask = Flask(__name__)
# support cross-origin resource sharing
CORS(app)
# set the logging endpoint
app.add_url_rule(rule="/logging",
                 endpoint="logging",
                 view_func=logging_service,
                 methods=["GET", "POST"])

# make PyDBrief's API available as a Swagger app
swagger_blueprint: Blueprint = get_swaggerui_blueprint(
    base_url="/apidocs",
    api_url="/swagger/pydbrief.json",
    config={"defaultModelsExpandDepth": -1}
)
app.register_blueprint(blueprint=swagger_blueprint)

# establish the current version
APP_VERSION: Final[str] = "1.3.9"
# configure jsonify() with 'ensure_ascii=False'
app.config["JSON_AS_ASCII"] = False


@app.route("/swagger/pydbrief.json")
def swagger() -> Response:
    """
    Entry point for the microservice providing OpenAPI specifications in the Swagger standard.

    By default, the browser is instructed to save the file, instead of displaying its contents.

    This parameter can optionally be provided to indicate otherwise:
        - attach=<1|t|true|0|f|false>: 'true' if not specified

    :return: the requested OpenAPI specifications
    """
    # define the treatment to be given to the file by the client
    param: str = http_get_parameter(request, "attach")
    attach: bool = (isinstance(param, str) and
                    param.lower() in ["1", "t", "true"])

    return send_from_directory(directory=Path(Path.cwd(), "swagger"),
                               path="pydbrief.json",
                               as_attachment=attach)


@app.route(rule="/version",
           methods=["GET"])
def version() -> Response:
    """
    Obtain the current version of *PyDBrief*, along with the *PyPomes* modules in use.

    :return: the versions in execution
    """
    # register the request
    logging_log_info(msg=f"Request {request.path}")

    versions: dict = get_versions()
    versions["PyDBrief"] = APP_VERSION

    # assign to the return variable
    result: Response = jsonify(versions)

    # log the response
    logging_log_info(msg=f"Response {request.path}: {result}")

    return result


@app.route(rule="/rdbms",
           methods=["POST"])
@app.route(rule="/rdbms/<rdbms>",
           methods=["GET"])
def handle_rdbms(rdbms: str = None) -> Response:
    """
    Entry point for configuring the RDBMS to use.

    The parameters are as follows:
        - *db-engine*: the reference RDBMS engine (*mysql*, *oracle*, *postgres*, or *sqlserver*)
        - *db-name*: name of database
        - *db-user*: the logon user
        - *db-pwd*: the logon password
        - *db-host*: the host URL
        - *db-port*: the connection port
        - *db-client*: the client package (Oracle, only)
        - *db-driver*: the database access driver (SQLServer, only)

    :param rdbms: the reference RDBMS engine (*mysql*, *oracle*, *postgres*, or *sqlserver*)
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request=request)

    reply: dict | None = None
    if request.method == "GET":
        # get RDBMS connection params
        reply = get_rdbms_params(errors=errors,
                                 rdbms=rdbms)
        dict_jsonify(source=reply)
    else:
        # configure the RDBMS
        set_rdbms_params(errors=errors,
                         scheme=scheme)
        if not errors:
            rdbms = scheme.get("db-engine")
            reply = {"status": f"RDBMS '{rdbms}' configuration updated"}

    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    logging_log_info(f"Response {request.path}?{scheme}: {result}")

    return result


@app.route(rule="/s3",
           methods=["POST"])
@app.route(rule="/s3/<s3_engine>",
           methods=["GET"])
def handle_s3(s3_engine: str = None) -> Response:
    """
    Entry point for configuring the S3 service to use.

    The parameters are as follows:
        - *s3-engine*: the reference S3 engine (*aws*, *ecs*, or *minio*)
        - *s3-endpoint-url*: the access URL for the service
        - *s3-bucket-name*: the name of the default bucket
        - *s3-access-key*: the access key for the service
        - *s3-secret-key*: the access secret code
        - *s3-region-name*: the name of the region where the engine is located (AWS only)
        - *s3-secure-access*: whether or not to use Transport Security Layer (MinIO only)

    :param s3_engine: the reference S3 engine (*aws*, *ecs*, or *minio*)
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request=request)

    reply: dict | None = None
    if request.method == "GET":
        # get S3 access params
        reply = get_s3_params(errors=errors,
                              s3_engine=s3_engine)
    else:
        # configure the S3 service
        set_s3_params(errors=errors,
                      scheme=scheme)
        if not errors:
            s3_engine = scheme.get("s3-engine")
            reply = {"status": f"S3 '{s3_engine}' configuration updated"}

    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    logging_log_info(f"Response {request.path}?{scheme}: {result}")

    return result


@app.route(rule="/migration:metrics",
           methods=["GET", "PATCH"])
@app.route(rule="/migration:verify",
           methods=["POST"])
def handle_migration() -> Response:
    """
    Entry point for configuring the RDBMS-independent parameters, and for assessing migration readiness.

    Assessing the server's migration readiness means to verify whether its state and data
    are valid and consistent, thus allowing for a migration to be attempted.

    For metrics, these are the expected parameters:
        - *batch-size*: maximum number of rows to migrate per batch (defaults to 1000000)
        - *chunk-size*: maximum size, in bytes, of data chunks in LOB data copying (defaults to 1048576)

    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request=request)

    reply: dict[str, Any] | None = None
    match request.method:
        case "GET":
            # retrieve the migration parameters
            reply = get_migration_metrics()
        case "PATCH":
            # establish the migration parameters
            set_migration_metrics(errors=errors,
                                  scheme=scheme,
                                  logger=PYPOMES_LOGGER)
            if not errors:
                reply = {"status": "Migration metrics updated"}
        case "POST":
            # assert whether migration is warranted
            assert_migration(errors=errors,
                             scheme=scheme,
                             run_mode=False)
            # errors ?
            if errors:
                # yes, report the problem
                reply = {"status": "Migration cannot be launched"}
            else:
                # no, display the migration context
                reply = get_migration_context(errors=errors,
                                              scheme=scheme)
                if reply:
                    reply["status"] = "Migration can be launched"

    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    logging_log_info(f"Response {request.path}?{scheme}: {result}")

    return result


@app.route(rule="/migrate",
           methods=["POST"])
def migrate_data() -> Response:
    """
    Migrate the specified schema/tables/views/indexes from the source to the target RDBMS.

    These are the expected parameters:
        - *from-rdbms*: the source RDBMS for the migration
        - *from-schema*: the source schema for the migration
        - *to-rdbms*: the destination RDBMS for the migration
        - *to-schema*: the destination schema for the migration
        - *migrate-metadata*: migrate metadata (this creates or transforms the destination schema)
        - *migrate-plaindata*: migrate non-LOB data
        - *migrate-lobdata*: migrate LOBs (large binary objects)
        - *process-indexes*: whether to migrate indexes (defaults to *False*)
        - *process-views*: whether to migrate views (defaults to *False*)
        - *relax-reflection*: relaxes finding referenced tables at reflection (defaults to *False*)
        - *skip-nonempty*: prevents data migration for nonempty tables in the destination schema
        - *include-relations*: optional list of relations (tables, views, and indexes) to migrate
        - *exclude-relations*: optional list of relations (tables, views, and indexes) not to migrate
        - *exclude-constraints*: optional list of constraints not to migrate
        - *remove-nulls*: optional list of tables having columns with embedded NULLs in string data
        - *exclude-columns*: optional list of table columns not to migrate
        - *override-columns*: optional list of columns with forced migration types

    These are noteworthy:
        - the parameters *include-relations* and *exclude-relations* are mutually exclusive;
        - if *migrate-plaindata* is set, it is assumed that metadata is also being migrated,
          or that all targeted tables in destination schema exist and are empty;
        - if *migrate-lobdata* is set, it is assumed that plain data are also being,
          or have already been, migrated.

    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request=request)

    # assert whether migration is warranted
    assert_migration(errors=errors,
                     scheme=scheme,
                     run_mode=True)

    # assert and obtain the external columns parameter
    override_columns: dict[str, Type] = assert_column_types(errors=errors,
                                                            scheme=scheme)
    reply: dict[str, Any] | None = None
    # is migration possible ?
    if not errors:
        # yes, obtain the migration parameters
        source_rdbms: str = scheme.get("from-rdbms").lower()
        target_rdbms: str = scheme.get("to-rdbms").lower()
        source_schema: str = scheme.get("from-schema").lower()
        target_schema: str = scheme.get("to-schema").lower()
        target_s3: str = scheme.get("to-s3").lower() if scheme.get("to-s3") else None
        step_metadata: bool = str_lower(scheme.get("migrate-metadata")) in ["1", "t", "true"]
        step_plaindata: bool = str_lower(scheme.get("migrate-plaindata")) in ["1", "t", "true"]
        step_lobdata: bool = str_lower(scheme.get("migrate-lobdata")) in ["1", "t", "true"]
        process_indexes: bool = str_lower(scheme.get("process-indexes")) in ["1", "t", "true"]
        process_views: bool = str_lower(scheme.get("process-views")) in ["1", "t", "true"]
        relax_reflection: bool = str_lower(scheme.get("relax-reflection")) in ["1", "t", "true"]
        skip_nonempty: bool = str_lower(scheme.get("skip-nonempty")) in ["1", "t", "true"]
        add_extensions: bool = str_lower(scheme.get("add-extensions")) in ["1", "t", "true"]
        remove_nulls: list[str] = str_as_list(str_lower(scheme.get("remove-nulls"))) or []
        include_relations: list[str] = str_as_list(str_lower(scheme.get("include-relations"))) or []
        exclude_relations: list[str] = str_as_list(str_lower(scheme.get("exclude-relations"))) or []
        exclude_columns: list[str] = str_as_list(str_lower(scheme.get("exclude-columns"))) or []
        exclude_constraints: list[str] = str_as_list(str_lower(scheme.get("exclude-constraints"))) or []

        # migrate the data
        reply = migrate(errors=errors,
                        source_rdbms=source_rdbms,
                        target_rdbms=target_rdbms,
                        source_schema=source_schema,
                        target_schema=target_schema,
                        target_s3=target_s3,
                        step_metadata=step_metadata,
                        step_plaindata=step_plaindata,
                        step_lobdata=step_lobdata,
                        process_indexes=process_indexes,
                        process_views=process_views,
                        relax_reflection=relax_reflection,
                        skip_nonempty=skip_nonempty,
                        add_extensions=add_extensions,
                        remove_nulls=remove_nulls,
                        include_relations=include_relations,
                        exclude_relations=exclude_relations,
                        exclude_columns=exclude_columns,
                        exclude_constraints=exclude_constraints,
                        override_columns=override_columns,
                        version=APP_VERSION,
                        logger=PYPOMES_LOGGER)
    # build the response
    result: Response = _build_response(errors=errors,
                                       reply=reply)
    # log the response
    logging_log_info(f"Response: {result}")

    return result


@app.errorhandler(code_or_exception=Exception)
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
        logging_log_error(msg=f"{err_msg}")
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

    if len(errors) == 0:
        # 'reply' might be None
        result = jsonify(reply)
    else:
        reply_err: dict = {"errors": validate_format_errors(errors=errors)}
        if isinstance(reply, dict):
            reply_err.update(reply)
        result = jsonify(reply_err)
        result.status_code = 400

    return result


if __name__ == "__main__":

    app.run(host="0.0.0.0",
            port=5000,
            debug=True)
