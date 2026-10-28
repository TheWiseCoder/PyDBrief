import sys
from logging import Logger


def init_app(logger: Logger) -> bool:

    errors: list[str] = []
    __initialize_state_storage(errors=errors)
    for error in errors:
        logger.critical(msg=error)
        sys.stderr.write(error)
        logger.error(msg=error)

    return not errors


def __initialize_state_storage(errors: list[str]):

    from pypomes_db import db_startup
    from app_constants import PYDB_DB_ENGINE, PYDB_CS_ENGINE

    db_startup(engine=PYDB_DB_ENGINE,
               errors=errors)
    if not errors and PYDB_CS_ENGINE:
        from pypomes_cs import cs_startup
        cs_startup(engine=PYDB_CS_ENGINE,
                   errors=errors)
