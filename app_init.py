import sys
from logging import Logger
from pypomes_cs import cs_startup
from pypomes_db import db_startup

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
        logger.error(msg=error)

    return not errors
