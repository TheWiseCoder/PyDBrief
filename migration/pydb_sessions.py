import uuid
from enum import StrEnum

from flask import Request
from pypomes_core import validate_bool, validate_format_error
from pypomes_http import http_get_parameters, HttpMethod
from typing import Any

from app_constants import (
    MigrationConfig, MigrationState, MetricsConfig,
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE,
    RANGE_PLAINDATA_CHANNELS, RANGE_LOBDATA_CHANNELS
)

# migration_registry: dict[str, dict[StrEnum, Any]] =
# {
#    <session_id>: {
#      MigrationConfig.CLIENT_ID: <str>,
#      MigrationConfig.STATE: <MigrationState>,
#      MigrationConfig.METRICS: {
#         MetricsConfig.BATCH_SIZE_IN: RANGE_BATCH_SIZE_IN[2],
#         MetricsConfig.BATCH_SIZE_OUT: RANGE_BATCH_SIZE_OUT[2],
#         MetricsConfig.CHUNK_SIZE: RANGE_CHUNK_SIZE[2],
#         MetricsConfig.INCREMENTAL_SIZE: RANGE_INCREMENTAL_SIZE[2],
#         MetricsConfig.LOBDATA_CHANNELS: RANGE_LOBDATA_CHANNELS[2],
#         MetricsConfig.PLAINDATA_CHANNELS: RANGE_PLAINDATA_CHANNELS[2]
#      },
#      <DbEngine>: {
#         DbConfig.ENGINE: <DbEngine>,
#         DbConfig.HOST: <str>,
#         DbConfig.NAME: <str>,
#         DbConfig.PORT: <int>,
#         DbConfig.PWD: <str>,
#         DbConfig.USER: <str>,
#         DbConfig.VERSION: <str>,
#         DbConfig.DRIVER: <str>,       <- SQLServer, only
#         DbConfig.CLIENT: <str>        <- Oracle, only
#      },
#      ...
#      <S3Engine>: {
#        S3Config.ENGINE: <S3Engine>,
#        S3Config.ACCESS_KEY: <str>,
#        S3Config.BUCKET_NAME: <str>,
#        S3Config.ENDPOINT_URL: <str>,
#        S3Config.SECRET_KEY: <str>,
#        S3Config.SECURE_ACCESS: <str>,
#        S3Config.VERSION: <str>,
#        S3Config.REGION_NAME: <str>    <- AWS, only
#      },
#      ...
#    },
#    ...
# }
migration_registry: dict[str, dict[StrEnum, Any]] = {}


def create_session(errors: list[str],
                   client_id: str,
                   session_id: str) -> bool:

    # initialize the return variable
    result: bool = False

    # check if session already exists+
    if session_id in migration_registry:
        # 142: Invalid value {}:{}
        errors.append(validate_format_error(142,
                                            session_id,
                                            "session already exists",
                                            f"@{MigrationConfig.SESSION_ID}"))
    else:
        # set client's current active session to 'inactive'
        active_session: str = get_active_session(client_id=client_id)
        if active_session:
            migration_registry[active_session][MigrationConfig.STATE] = MigrationState.INACTIVE
        # create new session
        migration_registry[session_id] = {
            MigrationConfig.CLIENT_ID: client_id,
            MigrationConfig.STATE: MigrationState.ACTIVE,
            MigrationConfig.METRICS: {
                MetricsConfig.BATCH_SIZE_IN: RANGE_BATCH_SIZE_IN[2],
                MetricsConfig.BATCH_SIZE_OUT: RANGE_BATCH_SIZE_OUT[2],
                MetricsConfig.CHUNK_SIZE: RANGE_CHUNK_SIZE[2],
                MetricsConfig.INCREMENTAL_SIZE: RANGE_INCREMENTAL_SIZE[2],
                MetricsConfig.LOBDATA_CHANNELS: RANGE_LOBDATA_CHANNELS[2],
                MetricsConfig.PLAINDATA_CHANNELS: RANGE_PLAINDATA_CHANNELS[2]
            }
        }
        result = True

    return result


def delete_session(errors: list[str],
                   session_id: str) -> bool:

    # initialize the return variable
    result: bool = False

    session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
    curr_state: MigrationState = session_registry.get(MigrationConfig.STATE)
    if curr_state in [MigrationState.ACTIVE, MigrationState.ABORTED,
                      MigrationState.ABORTED, MigrationState.INACTIVE,
                      MigrationState.ABORTED, MigrationState.FINISHED]:
        migration_registry.pop(session_id)
        result = True
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            session_id,
                                            f"session in state '{curr_state}' cannot be deleted",
                                            f"@{MigrationConfig.SESSION_ID}"))
    return result


def get_active_session(client_id: str) -> str | None:

    # initialize the return variable
    result: str | None = None

    # retrieve the id of the active session
    for key, value in migration_registry.items():
        if value.get(MigrationConfig.STATE) == MigrationState.ACTIVE and \
           value.get(MigrationConfig.CLIENT_ID) == client_id:
            result = key
            break

    return result


def get_session_state(session_id: str) -> MigrationState | None:

    # initialize the return variable
    result: MigrationState | None = None

    session_registry = migration_registry.get(session_id)
    if session_registry:
        result = session_registry.get(MigrationConfig.STATE)

    return result


def set_session_state(errors: list[str],
                      input_params: dict[str, Any]) -> MigrationState | None:

    # initialize the return variable
    result: MigrationState | None = None

    is_active: bool = validate_bool(errors=errors,
                                    source=input_params,
                                    attr=MigrationConfig.IS_ACTIVE,
                                    required=True)
    if isinstance(is_active, bool):
        new_state = MigrationState.ACTIVE if is_active else MigrationState.INACTIVE
        session_id: str = input_params.get(MigrationConfig.SESSION_ID)
        session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
        curr_state: MigrationState = session_registry.get(MigrationConfig.STATE)

        if curr_state in [MigrationState.ACTIVE, MigrationState.INACTIVE,
                          MigrationState.ABORTED, MigrationState.FINISHED]:
            if is_active:
                # set the client's current active session to 'inactive'
                client_id = input_params.get(MigrationConfig.CLIENT_ID)
                active_session: str = get_active_session(client_id=client_id)
                if active_session:
                    migration_registry[active_session][MigrationConfig.STATE] = MigrationState.INACTIVE
            session_registry[MigrationConfig.STATE] = new_state
            result = new_state
        else:
            # 142: Invalid value {}:{}
            errors.append(validate_format_error(142,
                                                session_id,
                                                f"session in state '{curr_state}' cannot be set to '{new_state}'",
                                                f"@{MigrationConfig.SESSION_ID}"))
    return result


def get_session_registry(session_id: str) -> dict[StrEnum, Any]:

    return migration_registry.get(session_id)


def get_session_params(errors: list[str],
                       request: Request,
                       session_id: str = None) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = http_get_parameters(request=request)

    # obtain the client id
    client_id: str = request.cookies.get(MigrationConfig.CLIENT_ID)
    if not client_id:
        client_id = str(uuid.uuid4())
    # 'client_id' must be returned, even if error
    result[MigrationConfig.CLIENT_ID] = client_id

    # obtain the session id
    session_id = (session_id or
                  result.get(MigrationConfig.SESSION_ID) or
                  get_active_session(client_id=client_id))
    if session_id:
        result[MigrationConfig.SESSION_ID] = session_id
        if not (request.path.startswith("/sessions") and request.method == HttpMethod.POST):
            # if it is not being created, session must exist and belong to client
            session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
            if client_id != (session_registry or {}).get(MigrationConfig.CLIENT_ID):
                # 141: Invalid value {}
                errors.append(validate_format_error(141,
                                                    session_id,
                                                    f"@{MigrationConfig.SESSION_ID}"))
    # GET on '/sessions' is the only operation not requiring 'session_id'
    elif not (request.path == "/sessions" and request.method == HttpMethod.GET):
        # 121: Required attribute
        errors.append(validate_format_error(121,
                                            f"@{MigrationConfig.SESSION_ID}"))
    return result


def get_sessions() -> dict[str, list[dict[str, str]]]:

    sessions: list[dict[str, str]] = []
    for key, value in migration_registry.items():
        sessions.append({"id": key,
                         "state": value.get(MigrationConfig.STATE),
                         "client": value.get(MigrationConfig.CLIENT_ID)})

    return {"sessions": sessions}


def abort_session_migration(errors: list[str],
                            session_id: str) -> bool:
    """
    Abort the ongoing migration for *session_id*.

    :param errors: incidental errors
    :param session_id: the session whose migration is to abort
    :return: the operation outcome
    """
    # initialize the return variable
    result: bool = False

    session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
    match session_registry.get(MigrationConfig.STATE):
        case MigrationState.MIGRATING:
            session_registry[MigrationConfig.STATE] = MigrationState.ABORTING
            result = True
        case MigrationState.ABORTING:
            # 142: Invalid value {}:{}
            errors.append(validate_format_error(142,
                                                session_id,
                                                "session already marked for abortion",
                                                f"@{MigrationConfig.SESSION_ID}"))
        case _:
            # 142: Invalid value {}:{}
            errors.append(validate_format_error(142,
                                                session_id,
                                                "session has no ongoing migration",
                                                f"@{MigrationConfig.SESSION_ID}"))
    return result
