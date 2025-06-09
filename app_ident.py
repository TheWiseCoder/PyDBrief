import os
from typing import Final

# establish prefixes
os.environ["PYPOMES_APP_PREFIX"] = "PYDB"
os.environ["PYDB_VALIDATION_MSG_PREFIX"] = ""

# establish the app's name and current version
APP_NAME: str = "PyDBrief"
APP_VERSION: Final[str] = "1.7.8"
