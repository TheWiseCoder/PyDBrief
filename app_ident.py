import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Final


# establish the app's name and current version
APP_NAME: str = "PyDBrief"
APP_VERSION: Final[str] = "2.3.8"

# load environment variables for local execution
# (this must precede importing 'pypomes-core')
if os.getenv("PYDB_DEPLOY_LOCAL") == "1":
    load_dotenv(dotenv_path=Path.cwd() / ".env_local",
                override=True)


def get_env_keys() -> list[str]:
    """
    Return the keys defined in the file *.env_local*.

    :return: keys defined in file *.env_local*
    """
    # initialize the retun variable
    result: list[str] = []

    env_path: Path = Path.cwd() / ".env_local"
    with env_path.open(mode="r") as file:
        for line in file:
            line = line.strip()
            if len(line) > 1 and not line.startswith("#"):
                result.append(line.split("=", 1)[0])

    return result


def __set_logging_file_path():

    from pypomes_core import APP_PREFIX, env_get_str, env_is_docker
    from app_constants import REGISTRY_DOCKER, REGISTRY_HOST

    # retrieve the logging file path from the environment
    env_key: str = f"{APP_PREFIX}_LOGGING_FILEPATH"
    log_path: Path = Path(env_get_str(key=env_key,
                                      def_value="/tmp/pydbrief.log"))
    # rebuild it
    base_path: str = REGISTRY_DOCKER if REGISTRY_DOCKER and env_is_docker() else REGISTRY_HOST
    if base_path:
        log_path = Path(base_path,
                        log_path.name)

    # create intermediate missing folders
    log_path.parent.mkdir(parents=True,
                          exist_ok=True)

    # set environment variable to be used by 'pypomes-logging'
    os.environ[env_key] = log_path.as_posix()


# set the logging file's path
# (this must precede importing 'pypomes-logging')
__set_logging_file_path()
