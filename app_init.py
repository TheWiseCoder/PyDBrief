import os
import sys
from logging import Logger
from pypomes_core import APP_PREFIX, pypomes_versions, dict_jsonify
from pypomes_cs import cs_get_params, cs_startup
from pypomes_db import db_get_params, db_startup
from pypomes_logging import logging_get_params
from typing import Any

from app_constants import PYDB_DBCTRL_ENGINE, PYDB_CSCTRL_ENGINE


def init_app(logger: Logger) -> bool:

    errors: list[str] = []

    db_startup(engine=PYDB_DBCTRL_ENGINE,
               errors=errors)
    if PYDB_CSCTRL_ENGINE:
        cs_startup(engine=PYDB_CSCTRL_ENGINE,
                   errors=errors)

    for error in errors:
        logger.critical(msg=error)
        sys.stderr.write(error)

    return not errors


def build_version_info(app_name: str,
                       app_version: str) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = {
        app_name: {
            "version": app_version
        },
        "foundation": pypomes_versions(),
        "environment": {key: value for key, value in os.environ.items()
                        if (key.startswith(APP_PREFIX) or
                            key.startswith("FLASK") or
                            key.startswith("PYPOMES") or
                            key.startswith("TZ")) and not ("_PWD" in key or "_SECRET" in key)},
        "logging": dict_jsonify(logging_get_params()),
        "stateful": [
            {k: v for k, v in db_get_params(engine=PYDB_DBCTRL_ENGINE).items() if "pwd" not in k}
        ]
    }
    result["stateful"][0]["db-engine"] = PYDB_DBCTRL_ENGINE
    if PYDB_CSCTRL_ENGINE:
        result["stateful"].append({k: v for k, v in cs_get_params(engine=PYDB_CSCTRL_ENGINE).items()
                                   if "secret" not in k})
        result["stateful"][1]["s3-engine"] = PYDB_CSCTRL_ENGINE

    return result
