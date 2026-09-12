from logging import Logger


def init_app(app_version: str,
             logger: Logger) -> bool:
    """
    Load state data from database.
    """

