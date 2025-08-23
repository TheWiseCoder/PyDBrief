import uuid
from enum import StrEnum
from logging import Logger

from flask import Request
from pypomes_core import validate_bool, validate_format_error
from pypomes_http import http_get_parameters, HttpMethod
from typing import Any

from app_constants import (
    RANGE_BATCH_SIZE_IN, RANGE_BATCH_SIZE_OUT,
    RANGE_CHUNK_SIZE, RANGE_INCREMENTAL_SIZE,
    RANGE_LOBDATA_CHANNELS, RANGE_LOBDATA_CHANNEL_SIZE,
    RANGE_PLAINDATA_CHANNELS, RANGE_PLAINDATA_CHANNEL_SIZE,
    MigrationState, MigConfig, MigSpec, MigMetric, MigSpot, MigStep
)

# migration_registry: dict[str, dict[StrEnum, Any]] =
# {
#    <session_id>: {
#      MigConfig.CLIENT_ID: <str>,
#      MigConfig.STATE: <MigrationState>,
#      MigConfig.SPOTS: {
#        MigSpot.FROM_RDBMS: <DbEngine>,
#        MigSpot.TO_RDBMS: <DbEngine>,
#        MigSpot.TO_S3: <S3Engine>
#      },
#      MigConfig.STEPS: {
#        MigStep.MIGRATE_METADATA: <bool>,
#        MigStep.MIGRATE_PLAINDATA: <bool>,
#        MigStep.MIGRATE_LOBDATA: <bool>,
#        MigStep.SYNCHRONIZE_PLAIDATA: <bool>
#      },
#      MigConfig.METRICS: {
#        MigMetric.BATCH_SIZE_IN: RANGE_BATCH_SIZE_IN[2],
#        MigMetric.BATCH_SIZE_OUT: RANGE_BATCH_SIZE_OUT[2],
#        MigMetric.CHUNK_SIZE: RANGE_CHUNK_SIZE[2],
#        MigMetric.INCREMENTAL_SIZE: RANGE_INCREMENTAL_SIZE[2],
#        MigMetric.LOBDATA_CHANNELS: RANGE_LOBDATA_CHANNELS[2],
#        MigMetric.LOBDATA_CHANNEL_SIZE: RANGW_LOBDATA_CHANNEL_SIZE[2]
#        MigMetric.PLAINDATA_CHANNELS: RANGE_PLAINDATA_CHANNELS[2],
#        MigMetric.PLAINDATA_CHANNEL_SIZE: RANGE_PLAINDATA_CHANNEL_SIZE[2],
#      },
#      MigConfig.SPECS: {
#        MigConfig.EXCLUDE_COLUMNS: [
#          <table-name>.<clumn-name>,
#          ...
#        ],
#        MigConfig.EXCLUDE_CONSTRAINTS: [
#          <constraint-name>,
#          ...
#        ],
#        MigConfig.EXCLUDE_RELATIONS: [
#          <table-view-index-name>,
#          ...
#        ],
#        MigConfig.FLATTEN_STORAGE: <bool>,
#        MigConfig.FROM_SCHEMA: <str>,
#        MigConfig.INCLUDE_RELATIONS: [
#          <table-view-index-name>,
#          ...
#        ],
#        MigConfig.INCREMENTAL_MIGRATIONS: {
#          "<table-name>": (<size>, <offset>),
#          ...
#        },
#        MigConfig.MIGRATION_BADGE: <str>,
#        MigConfig.NAMED_LOBDATA: [
#          <table-name>.<table-column>=<names-column>[.<extension>],
#          ...
#        ],
#        MigConfig.OVERRIDE_COLUMNS: [
#          <table-name>.<clumn-name>=<type-name>,
#          ...
#        ],
#        MigConfig.PROCESS_INDEXES: <bool>,
#        MigConfig.PROCESS_VIEWS: <bool>,
#        MigConfig.REFLECT_FILETYPE: <bool>,
#        MigConfig.RELAX_REFLECTION: <bool>,
#        MigConfig.REMOVE_NULLS:
#          <table-name>,
#          ...
#        ],
#        MigConfig.TO_SCHEMA: <str>,
#        MigConfig.SKIP_NONEMPTY: <bool>
#      },
#      <DbEngine>: {
#        DbConfig.ENGINE: <DbEngine>,
#        DbConfig.HOST: <str>,
#        DbConfig.NAME: <str>,
#        DbConfig.PORT: <int>,
#        DbConfig.PWD: <str>,
#        DbConfig.USER: <str>,
#        DbConfig.VERSION: <str>,
#        DbConfig.DRIVER: <str>,        <- SQLServer, only
#        DbConfig.CLIENT: <str>,        <- Oracle, only
#        DbConfig.VERSION: <str>
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
#        S3Config.REGION_NAME: <str>,   <- AWS, only
#        S3Config.VERSION: <str>
#      },
#      ...
#    },
#    ...
# }
migration_registry: dict[str, dict[StrEnum, Any]] = {}


def create_session(client_id: str,
                   session_id: str,
                   errors: list[str]) -> bool:

    # initialize the return variable
    result: bool = False

    # check if session already exists+
    if session_id in migration_registry:
        # 142: Invalid value {}:{}
        errors.append(validate_format_error(142,
                                            session_id,
                                            "session already exists",
                                            f"@{MigSpec.SESSION_ID}"))
    else:
        # set client's current active session to 'inactive'
        active_session: str = get_active_session(client_id=client_id)
        if active_session:
            migration_registry[active_session][MigSpec.STATE] = MigrationState.INACTIVE
        # create new session for 'client_id', and set it active
        migration_registry[session_id] = {
            MigSpec.CLIENT_ID: client_id,
            MigSpec.STATE: MigrationState.ACTIVE,
            MigConfig.SPOTS: {
                MigSpot.FROM_RDBMS: None,
                MigSpot.TO_RDBMS: None,
                MigSpot.TO_S3: None
            },
            MigConfig.STEPS: {
                MigStep.MIGRATE_METADATA: False,
                MigStep.MIGRATE_PLAINDATA: False,
                MigStep.MIGRATE_LOBDATA: False,
                MigStep.SYNCHRONIZE_PLAINDATA: False
            },
            MigConfig.METRICS: {
                MigMetric.BATCH_SIZE_IN: RANGE_BATCH_SIZE_IN[2],
                MigMetric.BATCH_SIZE_OUT: RANGE_BATCH_SIZE_OUT[2],
                MigMetric.CHUNK_SIZE: RANGE_CHUNK_SIZE[2],
                MigMetric.INCREMENTAL_SIZE: RANGE_INCREMENTAL_SIZE[2],
                MigMetric.LOBDATA_CHANNELS: RANGE_LOBDATA_CHANNELS[2],
                MigMetric.LOBDATA_CHANNEL_SIZE: RANGE_LOBDATA_CHANNEL_SIZE[2],
                MigMetric.PLAINDATA_CHANNELS: RANGE_PLAINDATA_CHANNELS[2],
                MigMetric.PLAINDATA_CHANNEL_SIZE: RANGE_PLAINDATA_CHANNEL_SIZE[2]
            },
            MigConfig.SPECS: {}
        }
        result = True

    return result


def delete_session(session_id: str,
                   errors: list[str]) -> bool:

    # initialize the return variable
    result: bool = False

    session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
    curr_state: MigrationState = session_registry.get(MigSpec.STATE)
    if curr_state in [MigrationState.ACTIVE, MigrationState.INACTIVE,
                      MigrationState.ABORTED, MigrationState.FINISHED]:
        migration_registry.pop(session_id)
        result = True
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            session_id,
                                            f"session in state '{curr_state}' cannot be deleted",
                                            f"@{MigSpec.SESSION_ID}"))
    return result


def get_active_session(client_id: str) -> str | None:

    # initialize the return variable
    result: str | None = None

    # retrieve the id of the active session
    for key, value in migration_registry.items():
        if value.get(MigSpec.STATE) == MigrationState.ACTIVE and \
           value.get(MigSpec.CLIENT_ID) == client_id:
            result = key
            break

    return result


def get_session_state(session_id: str) -> MigrationState | None:

    return (migration_registry.get(session_id) or {}).get(MigSpec.STATE)


def set_session_state(input_params: dict[str, Any],
                      errors: list[str]) -> MigrationState | None:

    # initialize the return variable
    result: MigrationState | None = None

    is_active: bool = validate_bool(source=input_params,
                                    attr=MigSpec.IS_ACTIVE,
                                    required=True,
                                    errors=errors)
    if isinstance(is_active, bool):
        new_state = MigrationState.ACTIVE if is_active else MigrationState.INACTIVE
        session_id: str = input_params.get(MigSpec.SESSION_ID)
        session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
        curr_state: MigrationState = session_registry.get(MigSpec.STATE)

        if curr_state in [MigrationState.ACTIVE, MigrationState.INACTIVE,
                          MigrationState.ABORTED, MigrationState.FINISHED]:
            if is_active:
                # set the client's current active session to 'inactive'
                client_id = input_params.get(MigSpec.CLIENT_ID)
                active_session: str = get_active_session(client_id=client_id)
                if active_session:
                    migration_registry[active_session][MigSpec.STATE] = MigrationState.INACTIVE
            session_registry[MigSpec.STATE] = new_state
            result = new_state
        else:
            # 142: Invalid value {}:{}
            errors.append(validate_format_error(142,
                                                session_id,
                                                f"session in state '{curr_state}' cannot be set to '{new_state}'",
                                                f"@{MigSpec.SESSION_ID}"))
    return result


def get_session_registry(session_id: str) -> dict[StrEnum, Any]:

    return migration_registry.get(session_id)


def get_session_params(request: Request,
                       session_id: str | None,
                       errors: list[str]) -> dict[str, Any]:

    # initialize the return variable
    result: dict[str, Any] = http_get_parameters(request=request)

    # obtain the client id
    client_id: str = request.cookies.get(MigSpec.CLIENT_ID)
    if not client_id:
        client_id = str(uuid.uuid4())
    # 'client_id' must be returned, even if error
    result[MigSpec.CLIENT_ID] = client_id

    # operations handling sessions
    del_session: bool = request.path.startswith("/sessions") and request.method == HttpMethod.DELETE
    get_session: bool = request.path.startswith("/sessions") and request.method == HttpMethod.GET
    new_session: bool = request.path.startswith("/sessions") and request.method == HttpMethod.POST

    # obtain the session id
    session_id = (session_id or
                  result.get(MigSpec.SESSION_ID) or
                  get_active_session(client_id=client_id))
    if session_id:
        # existing 'session_id' must be returned, even if error
        result[MigSpec.SESSION_ID] = session_id
        if not new_session:
            # if session is not beig created, verify that:
            #   - session exists, and
            #   - session belongs to client, or session is being aborted
            session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
            if not session_registry or not \
                    (client_id == session_registry.get(MigSpec.CLIENT_ID) or del_session):
                # 141: Invalid value {}
                errors.append(validate_format_error(141,
                                                    session_id,
                                                    f"@{MigSpec.SESSION_ID}"))
        if not errors:
            state: MigrationState = get_session_state(session_id=session_id)
            if (state == MigrationState.ABORTING and not get_session) or \
                    (state == MigrationState.MIGRATING and not (get_session or del_session)):
                # sessions with state 'ABORTING' are restricted to 'GET:/sessions'
                # sessions with state 'MIGRATING' are restricted to 'GET:/sessions' and 'DELETE:/sessions'
                # 142: Invalid value {}: {}
                errors.append(validate_format_error(141,
                                                    session_id,
                                                    f"current state is '{state}'"
                                                    f"@{MigSpec.SESSION_ID}"))
    # 'GET:/sessions' is the only operation not requiring 'session_id'
    elif not get_session:
        # 121: Required attribute
        errors.append(validate_format_error(121,
                                            f"@{MigSpec.SESSION_ID}"))
    return result


def get_sessions() -> dict[str, list[dict[str, str]]]:

    sessions: list[dict[str, str]] = []
    for key, value in migration_registry.items():
        sessions.append({"id": key,
                         "state": value.get(MigSpec.STATE),
                         "client": value.get(MigSpec.CLIENT_ID)})

    return {"sessions": sessions}


def abort_session_migration(session_id: str,
                            errors: list[str]) -> bool:
    """
    Abort the ongoing migration for *session_id*.

    :param errors: incidental errors
    :param session_id: the session whose migration is to be aborted
    :return: the operation outcome
    """
    # initialize the return variable
    result: bool = False

    session_registry: dict[StrEnum, Any] = migration_registry.get(session_id)
    if session_registry.get(MigSpec.STATE) == MigrationState.MIGRATING:
        session_registry[MigSpec.STATE] = MigrationState.ABORTING
        result = True
    else:
        # 142: Invalid value {}: {}
        errors.append(validate_format_error(142,
                                            session_id,
                                            "session has no ongoing migration",
                                            f"@{MigSpec.SESSION_ID}"))
    return result


def assert_session_abort(session_id: str,
                         errors: list[str],
                         logger: Logger) -> bool:

    # initialize the return variable
    result: bool = False

    # verify whether current migration is marked for abortion
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    if session_registry.get(MigSpec.STATE) == MigrationState.ABORTING:
        err_msg: str = f"Migration in session '{session_id}' aborted on request"
        logger.error(msg=err_msg)
        # 101: {}
        errors.append(validate_format_error(101,
                                            err_msg))
        result = True

    return result
