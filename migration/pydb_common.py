from enum import StrEnum
from pypomes_core import (
    validate_format_error
)
from pypomes_db import (
    DbEngine, db_get_version
)
from pypomes_s3 import (
    S3Engine, s3_get_version
)
from typing import Any

from app_constants import (
    DbConfig, S3Config, MigConfig, MigSpec, MigSpot
)
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
        result[DbConfig.VERSION] = db_get_version(engine=db_engine)
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
        result[S3Config.VERSION] = s3_get_version(engine=s3_engine)
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            s3_engine,
                                            "unknown or unconfigured S3 engine",
                                            f"@{S3Config.ENGINE}"))
    return result


def get_migration_spots(errors: list[str],
                        input_params: dict[str, str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] | None = None

    # retrieve the session identification
    session_id: str = input_params.get(MigSpec.SESSION_ID)

    # retrieve the source RDBMS parameters
    rdbms: str = input_params.get(MigSpot.FROM_RDBMS)
    from_rdbms: DbEngine = DbEngine(rdbms) if rdbms else None
    from_params: dict[DbConfig, Any] = get_rdbms_specs(errors=errors,
                                                       session_id=session_id,
                                                       db_engine=from_rdbms)
    # retrieve the target RDBMS parameters
    rdbms = input_params.get(MigSpot.TO_RDBMS)
    to_rdbms: DbEngine = DbEngine(rdbms) if rdbms else None
    to_params: dict[DbConfig, Any] = get_rdbms_specs(errors=errors,
                                                     session_id=session_id,
                                                     db_engine=to_rdbms)
    # retrieve the target S3 parameters
    s3_params: dict[S3Config, Any] | None = None
    s3: str = input_params.get(MigSpot.TO_S3)
    to_s3: S3Engine = S3Engine(s3) if s3 in S3Engine else None
    if to_s3:
        s3_params = get_s3_specs(errors=errors,
                                 session_id=session_id,
                                 s3_engine=to_s3)
    # build the return data
    if not errors:
        result: dict[str, Any] = {
            MigConfig.METRICS: get_session_registry(session_id=session_id)[MigConfig.METRICS],
            MigSpot.FROM_RDBMS: from_params,
            MigSpot.TO_RDBMS: to_params
        }
        if s3_params:
            result[MigSpot.TO_S3] = s3_params

    return result


def build_channel_data(max_channels: int,
                       channel_size: int,
                       table_count: int,
                       offset_count: int,
                       limit_count: int) -> list[(int, int)]:

    result: list[(int, int)] = []

    max_channels = max(1, max_channels)
    channel_size = min(channel_size, table_count)
    limit_count = min(limit_count, table_count)
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
