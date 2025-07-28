from enum import StrEnum
from pathlib import Path
from pypomes_core import (
    validate_format_error
)
from pypomes_db import DbEngine
from pypomes_s3 import S3Engine
from typing import Any
from urlobject import URLObject

from app_constants import DbConfig, S3Config
from migration.pydb_sessions import get_session_registry


def get_rdbms_specs(errors: list[str],
                    session_id: str,
                    db_engine: DbEngine) -> dict[DbConfig, Any] | None:

    # initialize the return variable
    result: dict[DbConfig, Any] | None = None

    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    rdbms_params: dict[DbConfig, Any] = session_registry.get(db_engine)
    if isinstance(rdbms_params, dict):
        result = rdbms_params.copy()
        result.pop(DbConfig.PWD)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            db_engine,
                                            "unknown or unconfigured RDBMS engine",
                                            f"@{DbConfig.ENGINE}"))
    return result


def get_s3_specs(errors: list[str],
                 session_id: str,
                 s3_engine: S3Engine) -> dict[S3Config, Any] | None:

    # initialize the return variable
    result: dict[S3Config, Any] | None = None

    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    s3_params: dict[S3Config, Any] = session_registry.get(s3_engine)
    if isinstance(s3_params, dict):
        result = s3_params.copy()
        result.pop(S3Config.SECRET_KEY)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine,
                                            "unknown or unconfigured S3 engine",
                                            f"@{S3Config.ENGINE}"))
    return result


def build_channel_data(max_channels: int,
                       channel_size: int,
                       table_count: int,
                       offset_count: int,
                       limit_count: int) -> list[(int, int)]:

    # initialize the return variable
    result: list[(int, int)] = []

    # 'limt_count' might be 0, 'table_count' is always greater than 0
    if limit_count == 0 or limit_count > table_count:
        limit_count = table_count

    # normalize 'channel_size'
    channel_size = min(channel_size, table_count)
    if channel_size > limit_count:
        channel_size = limit_count
    elif max_channels * channel_size < limit_count:
        channel_size = int(limit_count / max_channels)

    # allocate sizes and offsets for multi-thread use
    total_count: int = 0
    while total_count + channel_size <= limit_count:
        result.append((offset_count, channel_size))
        total_count += channel_size
        offset_count += channel_size

    # process remaining rows
    remainder: int = limit_count - total_count
    if remainder > 0:
        # a new channel is used, if:
        #   - at least one is still available, and
        #   - the remaining size is greater than 10% of the channel size
        if len(result) < max_channels and 10 * remainder > channel_size:
            result.append((offset_count, remainder))
        # otherwise, the remaining rows are added to the last channel
        else:
            result[-1] = (result[-1][0], result[-1][1] + remainder)

    return result


def build_lob_prefix(session_registry: dict[StrEnum, Any],
                     target_db: DbEngine,
                     target_table: str,
                     column_name) -> Path:

    url: URLObject = URLObject(session_registry[target_db][DbConfig.HOST])
    # 'url.hostname' returns 'None' for 'localhost'
    host: str = f"{target_db}@{url.hostname or str(url)}"
    target_schema, table_name = target_table.split(sep=".")
    return Path(host,
                session_registry[target_db][DbConfig.NAME],
                target_schema,
                table_name,
                column_name)
