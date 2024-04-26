import json
import os
import sys
from pathlib import Path

from flask import (
    Blueprint, Flask, Response, jsonify, request, send_from_directory
)
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
from typing import Final
from werkzeug.exceptions import NotFound

os.environ["PYPOMES_APP_PREFIX"] = "PYDB"

# ruff: noqa: E402
from pypomes_core import (
    exc_format, validate_str, validate_format_error, validate_format_errors
)  # noqa: PyPep8
from pypomes_http import (
    http_get_parameter, http_get_parameters
)  # noqa: PyPep8
from pypomes_logging import (
    logging_send_entries, logging_log_info, logging_log_error
)  # noqa: PyPep8

from converter import pydb_oracle, pydb_postgres, pydb_sqlserver  # noqa: PyPep8
from converter.pydb_converter import convert  # noqa: PyPep8

# establish the current version
APP_VERSION: Final[str] = "1.0.0_RC02"

# list supported DB engines
PYDB_SUPPORTED_ENGINES: list[str] = ["oracle", "postgres", "sqlserver"]

# create the Flask application
app = Flask(__name__)

# support cross origin resource sharing
CORS(app)

# configure jsonify() with 'ensure_ascii=False'
app.config["JSON_AS_ASCII"] = False

# make PyDBrave's API available as a Swagger app
swagger_blueprint: Blueprint = get_swaggerui_blueprint(
    base_url="/api-docs",
    api_url="/swagger/pydbrave.json",
    config={"defaultModelsExpandDepth": -1}
)
app.register_blueprint(swagger_blueprint)


@app.route("/swagger/ijud.json")
# ?attach=<1|t|true|0|f|false|> - optional, 'False' if not specifies
def swagger() -> Response:
    """
    Entry point for the microservice providing OpenAPI specifications in the Swagger standard.

    By default, the browser is instructed to save the file, instead of displaying its contents.

    This parameter can optionally be provided to indicate otherwise:
        - attach=<1|t|true|0|f|false> - 'true' if not specified

    :return: the requested OpenAPI specifications
    """
    # define the treatment to be given to the file by the client
    param: str = http_get_parameter(request, "attach")
    attach: bool = isinstance(param, str) and param.lower() in ["1", "t", "true"]

    return send_from_directory(directory=Path(Path.cwd(), "swagger"),
                               path="pydbrave.json",
                               as_attachment=attach)


@app.route(rule="/version",
           methods=["GET"])
def version() -> Response:
    """
    Obtain the current version of *PYDBrave*.

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
        - level=<log-level>
        - from=<YYYYMMDDhhmmss>
        - to=<YYYYMMDDhhmmss>
        - last-days=<n>
        - last-hours=<n>

    By default, the browser is instructed to save the file, instead of displaying its contents.

    This parameter can optionally be provided to indicate otherwise:
        - attach=<1|t|true|0|f|false> - 'true' if not specified

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


# ? engine=<oracle|postgres|sqlserver>
@app.route(rule="/configure_engine",
           methods=["PATCH", "POST"])
def configure_engine() -> Response:
    """
    Entry point for configuring the database engines.

    The parameters are as follows:
        - direction: defines the migration direction for the engine (*source* or *target*)
        - engine: specifies the type of engine (*oracle*, *postgres*, or *sqlserver*)
        - db_name: name of database
        - db_user: the logon user
        - db_pwd: the logon password
        - db_host: the host URL
    """
    # declare the return variable
    result: Response

    # initialize the errors list
    errors: list[str] = []

    # retrieve the  input parameters
    scheme: dict = http_get_parameters(request)
    engine: str = validate_str(errors=errors,
                               scheme=scheme,
                               attr="engine",
                               default=PYDB_SUPPORTED_ENGINES)
    # configure the engine
    if len(errors) == 0:
        match engine:
            case "oracle":
                pydb_oracle.set_connection_params(errors, scheme, request.method == "POST")
            case "postgres":
                pydb_postgres.set_connection_params(errors, scheme, request.method == "POST")
            case "sqlserver":
                pydb_sqlserver.set_connection_params(errors, scheme, request.method == "POST")

    if len(errors) == 0:
        result = jsonify({"status": "Operação bem sucedida"})
    else:
        result = jsonify({"errors": validate_format_errors(errors)})
        result.status_code = 200

    return result


@app.route(rule="/convert",
           methods=["POST"])
def launch_migration() -> Response:

    # initialize the errors list
    errors: list[str] = []

    # retrieve the  input parameters
    scheme: dict = http_get_parameters(request)
    source_engine: str = validate_str(errors=errors,
                                      scheme=scheme,
                                      attr="source-engine",
                                      default=PYDB_SUPPORTED_ENGINES)
    target_engine: str = validate_str(errors=errors,
                                      scheme=scheme,
                                      attr="source-engine",
                                      default=PYDB_SUPPORTED_ENGINES)
    if len(errors) == 0 and source_engine == target_engine:
        # 116: Value {} cannot be assigned for attributes {} at the same time
        errors.append(validate_format_error(116, source_engine,
                                            "source-engine, target-engine"))
    if len(errors) == 0:


    if len(errors) == 0:
        status: dict = convert(errors, source_engine, target_engine)
    else:
        status: dict = {"errors": errors}

    result: Response = jsonify(status)
    if len(errors) > 0:
        result.status_code = 400

    # registe the response
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


if __name__ == "__main__":

    app.run(host="0.0.0.0",
            port=5000,
            debug=True)
