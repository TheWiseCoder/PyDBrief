import os
from dotenv import load_dotenv
from pathlib import Path
from typing import Final

# establish the app's name and current version
APP_NAME: str = "PyDBrief"
APP_VERSION: Final[str] = "2.0.7"

# load environment variables for local execution
if os.getenv("APP_LOCAL_DEPLOYMENT") == "1":
    load_dotenv(dotenv_path=Path.cwd() / ".env")
