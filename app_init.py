import os
from typing import Final

# establish prefixes
os.environ["PYPOMES_APP_PREFIX"] = "PYDB"
os.environ["PYDB_VALIDATION_MSG_PREFIX"] = ""

# establish the current version
APP_VERSION: Final[str] = "1.5.2"
