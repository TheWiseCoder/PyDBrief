import sys
from pypomes_core import (
    str_is_int, str_splice,
    validate_format_error, str_sanitize, exc_format
)
from pypomes_db import DbEngine
from typing import Type

from app_constants import MigIncremental
from entities.migration_spec import MigSpec
from migration.pydb_types import name_to_type


def process_override_columns(override_columns: list[str],
                             db_engine: DbEngine | str,
                             errors: list[str]) -> dict[str, Type]:

    # initialize the return variable
    result: dict[str, Type] = {}

    # process the override columns list
    try:
        for override_column in override_columns:
            # format of 'override_column' is <table_name>.<column_name>=<column_type>
            column_name: str = override_column[:int(f"{override_column.rindex('=')}")].lower()
            type_name: str = override_column.replace(column_name, "", 1)[1:].lower()
            column_type: Type = name_to_type(type_name=type_name,
                                             db_engine=db_engine)
            if column_name and column_type:
                result[column_name] = column_type
            else:
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(142,
                                                    type_name,
                                                    f"not a valid column type for dtabase engine {db_engine}"))
    except Exception as e:
        exc_err: str = str_sanitize(exc_format(exc=e,
                                               exc_info=sys.exc_info()))
        # 101: {}
        errors.append(validate_format_error(101,
                                            f"Syntax error: {exc_err}",
                                            f"@{MigSpec.OVERRIDE_COLUMNS}"))
    return result


def process_incremental_migrations(incremental_migrations: list[str],
                                   def_size: int) -> dict[str, dict[MigIncremental, int]]:

    # initialize the return variable
    result: dict[str, dict[MigIncremental, int]] = {}

    # format of 'incremental_migrations' is [<table-name>[=<size>[:<offset>],...]
    for incremental_table in incremental_migrations:
        if ":" not in incremental_table:
            incremental_table += ":"
        # noinspection PyTypeChecker
        terms: tuple[str, str, str] = str_splice(incremental_table,
                                                 seps=["=", ":"])
        size: int = int(terms[1]) if str_is_int(terms[1]) else def_size
        offset: int = int(terms[2]) if str_is_int(terms[2]) else 0
        result[terms[0]] = {
            MigIncremental.COUNT: size,
            MigIncremental.OFFSET: offset
        }

    return result
