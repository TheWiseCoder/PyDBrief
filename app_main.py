import json
import os
import sys
from flask import Flask, Response, jsonify, request
from flask_cors import CORS
from typing import Final
from werkzeug.exceptions import NotFound

os.environ["PYPOMES_APP_PREFIX"] = "PYDB"

# ruff: noqa: E402
from pypomes_core import exc_format  # noqa: PyPep8
from pypomes_http import http_get_parameter  # noqa: PyPep8
from pypomes_logging import (
    logging_send_entries, logging_log_info, logging_log_error
)  # noqa: PyPep8

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


# ?attach=<1|t|true|0|f|false|> - opcional, 'True' se não especificado
# ?level=<log-level>
# ?from=YYYYMMDDhhmmss&to=YYYYMMDDhhmmss>
# ?last-days=<n>&last-hours=<n>
@app.route(rule="/get-log",
           methods=["GET"])
def get_log() -> Response:
    """
    Entry pointy for obtaining the execution log of the system.

    The query parameters are optional, and are used to filter the records to be returned.

    By default, the browser is instructed to save the file, instead of displaying its contents.
    The parameter *attach* can optionally be provided to indicate otherwise,
     with the value *0*, *f* or *false*.

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


@app.route(rule="/convert",
           methods=["POST"])
# ?source=<source-engine>&target=<target-engine>
def launch() -> Response:

    # retrieve the  input parameters
    source_engine: str = http_get_parameter(request, "source")
    target_engine: str = http_get_parameter(request, "target")

    errors: list[str] = []
    if source_engine not in PYDB_SUPPORTED_ENGINES:
        errors.append(f"Invalid source engine: {source_engine}")
    if target_engine not in PYDB_SUPPORTED_ENGINES:
        errors.append(f"Invalid target engine: {target_engine}")

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
