import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Final

# establish the app's name and current version
APP_NAME: str = "PyDBrief"
APP_VERSION: Final[str] = "2.0.8"

# load environment variables for local execution
if os.getenv("APP_LOCAL_DEPLOYMENT") == "1":
    load_dotenv(dotenv_path=Path.cwd() / ".env")


def get_env_keys() -> list[str]:
    """
    Retorna todas as chaves definidas no arquivo *.env*.

    :return: chaves definidas no arquivo *.env*
    """
    # inicializa a variável de retorno
    result: list[str] = []

    env_path: Path = Path.cwd() / ".env"
    with env_path.open("r") as file:
        for line in file:
            line = line.strip()
            if len(line) > 1 and not line.startswith("#"):
                result.append(line.split("=", 1)[0])

    return result
