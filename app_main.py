import json
import os
import sys
from flask import (
    Blueprint, Flask, Response, jsonify, request, send_from_directory
)
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from pathlib import Path
from typing import Final
from werkzeug.exceptions import NotFound

os.environ["PYPOMES_APP_PREFIX"] = "PYDB"

# ruff: noqa: E402
from pypomes_core import (
    exc_format, str_as_list, validate_bool, validate_format_errors, validate_str
)  # noqa: PyPep8
from pypomes_http import (
    http_get_parameter, http_get_parameters
)  # noqa: PyPep8
from pypomes_logging import (
    logging_send_entries, logging_log_info, logging_log_error
)  # noqa: PyPep8

from migrator import (
    pydb_common, pydb_migrator, pydb_validator
)  # noqa: PyPep8

# establish the current version
APP_VERSION: Final[str] = "1.0.0_RC04"

# create the Flask application
app = Flask(__name__)

# support cross origin resource sharing
CORS(app)

# configure jsonify() with 'ensure_ascii=False'
app.config["JSON_AS_ASCII"] = False

# make PyDBrief's API available as a Swagger app
swagger_blueprint: Blueprint = get_swaggerui_blueprint(
    base_url="/swagger",
    api_url="/swagger/pydbrief.json",
    config={"defaultModelsExpandDepth": -1}
)
app.register_blueprint(swagger_blueprint)


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
    attach: bool = isinstance(param, str) and param.lower() in ["1", "t", "true"]

    return send_from_directory(directory=Path(Path.cwd(), "swagger"),
                               path="pydbrief.json",
                               as_attachment=attach)


@app.route(rule="/version",
           methods=["GET"])
def version() -> Response:
    """
    Obtain the current version of *PYDBrief*.

    :return: the version in execution
    """
    # register the request
    logging_log_info(f"Request {request.path}")

    # assign to the return variable
    result: Response = jsonify({"version": APP_VERSION})

    # register the response
    logging_log_info(f"Response {request.path}: {result}")

    return result


@app.route(rule="/get-log",
           methods=["GET"])
def get_log() -> Response:
    """
    Entry pointy for obtaining the execution log of the system.

    The query parameters are optional, and are used to filter the records to be returned:
        - *level*=<log-level>
        - *from*=<YYYYMMDDhhmmss>
        - *to*=<YYYYMMDDhhmmss>
        - *last-days*=<n>
        - *last-hours*=<n>

    By default, the browser is instructed to save the file, instead of displaying its contents.

    This parameter can optionally be provided to indicate otherwise:
        - *attach*=<1|t|true|0|f|false> - 'true' if not specified

    :return: the requested log data
    """
    # register the request
    req_query: str = request.query_string.decode()
    logging_log_info(f"Request {request.path}?{req_query}")

    # run the request
    result: Response = logging_send_entries(request)

    # register the response
    logging_log_info(f"Response {request.path}?{req_query}: {result}")

    return result


@app.route(rule="/rdbms/<rdbms>",
           methods=["GET", "PATCH"])
def handle_rdbms(rdbms: str) -> Response:
    """
    Entry point for configuring the *RDMS* to use.

    The parameters are as follows:
        - *rdbms*: specifies the type of RDBMS (*oracle*, *postgres*, or *sqlserver*)
        - *db-name*: name of database
        - *db-user*: the logon user
        - *db-pwd*: the logon password
        - *db-host*: the host URL
        - *db-client*: the client package (Oracle, only)
        - *db-driver*: the database access driver (SQLServer, only)

    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request)
    scheme["rdbms"] = rdbms

    reply: dict
    if request.method == "GET":
        # get RDBMS connection params
        reply = pydb_validator.get_connection_params(errors, scheme)
    else:
        # configure the RDBMS
        pydb_validator.set_connection_params(errors, scheme)
        if len(errors) == 0:
            reply = {"status": "Configuration updated"}

    # build the response
    result: Response = _build_response(errors, reply)

    # register the response
    logging_log_info(f"Response {request.path}?{scheme}: {result}")

    return result


@app.route(rule="/migration:configure",
           methods=["GET", "PATCH"])
@app.route(rule="/migration:verify",
           methods=["POST"])
def handle_migration() -> Response:
    """
    Entry point for configuring the RDBMS-independent parameters, and for assessing migration readiness.

    Assessing the server's migration readiness means to verify whether its state and data
    are valid and consistent, thus allowing for a migration to be attempted.

    For configuring, these are the expected parameters:
        - *batch-size*: maximum number of rows to migrate per batch (defaults to 100000)
        - *processes*: the number of processes to speed-up the migration with (defaults to 1)

    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request)

    reply: dict | None = None
    match request.method:
        case "GET":
            # retrieve the migration parameters
            reply = pydb_common.get_migration_params()
        case "PATCH":
            # establish the migration parameters
            pydb_common.set_migration_parameters(errors, scheme)
        case "POST":
            # validate the source and target RDBMS engines
            pydb_common.validate_rdbms_dual(errors, scheme)
            # errors ?
            if len(errors) == 0:
                # no, assert the migration parameters
                pydb_validator.assert_migration(errors, scheme)
                # errors ?
                if len(errors) == 0:
                    # no, display parameters
                    reply = pydb_validator.get_migration_context(scheme)
                    reply.update({"status": "Migration can be launched"})
                else:
                    # yes, report the problems
                    reply = {"status": "Migration cannot be launched"}

    # build the response
    result: Response = _build_response(errors, reply)

    # register the response
    logging_log_info(f"Response {request.path}?{scheme}: {result}")

    return result


@app.route(rule="/migrate/schema/<schema>",
           methods=["POST"])
@app.route(rule="/migrate/schema/<schema>/tables",
           methods=["POST"])
def migrate_data(schema: str) -> Response:
    """
    Migrate the specified table from the source to the target *RDBMS*.

    These are the expected parameters:
        - *tables*: optional list tables to migrate (defaults to all tables in *schema*)
        - *from*: the source RDBMS for the migration
        - *to*: the destination RDBMS for the migration
        - #drop-tables*: whether to drop the destination tables before the migration

    :param schema: the database schema to work with
    :return: the operation outcome
    """
    # initialize the errors list
    errors: list[str] = []

    # retrieve the input parameters
    scheme: dict = http_get_parameters(request)
    scheme["schema"] = schema

    # validate the source and target RDBMS engines
    (source_rdbms, target_rdbms) = pydb_common.validate_rdbms_dual(errors, scheme)

    # define whether to drop the destination table before migrating the data
    drop_tables: bool = validate_bool(errors=errors,
                                      scheme=scheme,
                                      attr="drop-tables",
                                      mandatory=True)

    # assert whether migration is warranted
    if len(errors) == 0:
        pydb_validator.assert_migration(errors, scheme)

    reply: dict | None = None
    # is migration possible ?
    if len(errors) == 0:
        # yes, retrieve the schema
        schema: str = validate_str(errors=errors,
                                   scheme=scheme,
                                   attr="schema",
                                   default=True)
        # was the schema obtained ?
        if schema:
            # yes, retrieve the list of tables and migrate the data
            tables: list[str] = str_as_list(scheme.get("tables"))
            reply = pydb_migrator.migrate_data(errors, source_rdbms, target_rdbms,
                                               schema, tables, drop_tables)

    # build the response
    result: Response = _build_response(errors, reply)

    # register the response
    logging_log_info(f"Response: {result.get_json()}")

    return result


@app.errorhandler(code_or_exception=Exception)
def handle_exception(exc: Exception) -> Response:
    """
    Handle exceptions raised when responding to requests, but not handled.

    :return: status 500, with JSON containing the errors.
    """
    # declare the return variable
    result: Response

    # is the exception an instance of werkzeug.exceptions.NotFound ?
    if isinstance(exc, NotFound):
        # yes, disregard it
        result = Response()
        result.status_code = 204
    else:
        # no, report the problem
        err_msg: str = exc_format(exc, sys.exc_info())
        logging_log_error(f"{err_msg}")
        reply: dict = {
            "errors": [err_msg]
        }
        result = Response(response=json.dumps(reply),
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
        reply_err: dict = {"errors": validate_format_errors(errors)}
        if isinstance(reply, dict):
            reply_err.update(reply)
        result = jsonify(reply_err)
        result.status_code = 400

    return result


if __name__ == "__main__":

    app.run(host="0.0.0.0",
            port=5000,
            debug=True)
