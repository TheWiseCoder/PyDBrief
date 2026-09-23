import sys
from logging import Logger


def init_app(logger: Logger) -> bool:

    errors: list[str] = []
    __initialize_state_storage(errors=errors,
                               logger=logger)
    for error in errors:
        logger.critical(msg=error)
        sys.stderr.write(error)
        logger.error(msg=error)

    return not errors


def __initialize_state_storage(errors: list[str],
                               logger: Logger):

    from pypomes_db import db_startup, db_set_logger
    from app_constants import PYDB_DB_ENGINE, PYDB_S3_ENGINE

    db_startup(engine=PYDB_DB_ENGINE,
               errors=errors)
    if not errors:
        db_set_logger(engine=PYDB_S3_ENGINE,
                      logger=logger)

        if PYDB_S3_ENGINE:
            from pypomes_s3 import s3_startup, s3_set_logger
            s3_startup(engine=PYDB_S3_ENGINE,
                       errors=errors)
            if not errors:
                s3_set_logger(engine=PYDB_S3_ENGINE,
                              logger=logger)
