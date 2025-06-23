from enum import StrEnum
from pypomes_core import (
    validate_format_error
)
from pypomes_db import DbEngine
from pypomes_s3 import S3Engine
from typing import Any

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

    result: list[(int, int)] = []

    max_channels = max(1, max_channels)
    channel_size = min(channel_size, table_count)
    # 'limt_count' might be 0, 'table_count' is always greater than 0
    limit_count = min(max(limit_count, table_count), table_count)
    if limit_count < channel_size:
        channel_size = limit_count
    elif max_channels * channel_size < limit_count:
        channel_size = int(limit_count / max_channels)

    total: int = 0
    while total + channel_size <= limit_count:
        result.append((channel_size, offset_count))
        total += channel_size
        offset_count += channel_size

    remainder: int = limit_count - total
    if remainder > 0:
        if len(result) < max_channels:
            result.append((remainder, offset_count))
        else:
            result[-1] = (result[-1][0] + remainder, result[1][1])

    return result
