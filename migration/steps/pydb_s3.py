import hashlib
import mimetypes
import pickle
from enum import StrEnum
from logging import Logger
from pypomes_core import Mimetype, file_get_mimetype, file_get_extension, str_from_any
from pypomes_db import db_stream_lobs, DbEngine
from pypomes_s3 import s3_data_store, S3Engine
from pathlib import Path
from typing import Any

from app_constants import (
    MigConfig, MigMetric, MigSpot, MigSpec
)
from migration.pydb_sessions import assert_session_abort, get_session_registry


def s3_migrate_lobs(errors: list[str],
                    session_id: str,
                    s3_client: Any,
                    target_table: str,
                    source_table: str,
                    lob_prefix: Path,
                    lob_column: str,
                    pk_columns: list[str],
                    where_clause: str,
                    offset_count: int,
                    limit_count: int,
                    forced_filetype: str,
                    ret_column: str,
                    logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, Any] = session_registry[MigConfig.METRICS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]

    # retrieve the configuration for the migration
    source_db: DbEngine = session_spots[MigSpot.FROM_RDBMS]
    trget_db: DbEngine = session_spots[MigSpot.TO_RDBMS]
    target_s3: S3Engine = session_spots[MigSpot.TO_S3]
    chunk_size: int = session_metrics[MigMetric.CHUNK_SIZE]

    # initialize the properties
    forced_mimetype: str = mimetypes.types_map.get(forced_filetype)
    identifier: str | None = None
    mimetype: Mimetype | str | None = None
    extension: str | None = None
    lob_data: bytes | None = None
    metadata: dict[str, str] = {}
    first_chunk: bool = True
    lob_count: int = 0

    # get data from the LOB streamer as follows:
    #   - 'row_data' hold the streamed data (LOB identification or LOB payload)
    #   - a 'dict' identifying the LOB is sent (flagged by 'first_chunk')
    #   - if the LOB is null, one null payload follows, terminating the LOB
    #   - if the LOB is empty, one empty and one null payload follow in sequence, terminating the LOB
    #   - if the LOB has data, multiple payloads follow, until a null payload terminates the LOB
    # noinspection PyTypeChecker
    for row_data in db_stream_lobs(errors=errors,
                                   table=source_table,
                                   lob_column=lob_column,
                                   pk_columns=pk_columns,
                                   ret_column=ret_column,
                                   engine=source_db,
                                   where_clause=where_clause,
                                   offset_count=offset_count,
                                   limit_count=limit_count,
                                   chunk_size=chunk_size,
                                   logger=logger):

        # verify whether current migration is marked for abortion
        assert_session_abort(errors=errors,
                             session_id=session_id,
                             logger=logger)
        if errors:
            break

        # LOB identification
        if first_chunk:
            # the metadata is a 'dict' with the values of:
            #   - the rdbms
            #   - the table
            #   - the row's PK columns
            #   - the lobdata's filename (if 'ref_column' was specified)
            values: list[Any] = []
            metadata = {
                "rdbms": trget_db,
                "table": target_table
            }
            for key, value in sorted(row_data.items()):
                if key == ret_column:
                    identifier = value
                else:
                    values.append(value)
                    metadata[key] = str_from_any(source=value)
            if not identifier:
                # hex-formatted hash on the contents of the row's PK columns
                identifier = __build_identifier(values=values)
            mimetype = forced_mimetype
            extension = forced_filetype
            lob_data = None
            first_chunk = False

        # data chunks
        elif row_data is not None:
            # add to LOB data
            if lob_data is None:
                lob_data = b""
            if isinstance(row_data, bytes):
                lob_data += row_data
            else:
                lob_data += bytes(row_data)

        # no more data
        else:
            # send LOB data
            if lob_data is not None:
                # has filetype reflection been specified ?
                if not forced_mimetype and session_specs[MigSpec.REFLECT_FILETYPE]:
                    # yes, determine LOB's mimetype and file extension
                    mimetype = file_get_mimetype(file_data=lob_data)
                    extension = file_get_extension(mimetype=mimetype)
                # add extension
                if extension:
                    identifier += extension

                # send it to S3
                s3_data_store(errors=errors,
                              identifier=identifier,
                              data=lob_data,
                              length=len(lob_data),
                              mimetype=mimetype,
                              tags=metadata,
                              prefix=lob_prefix,
                              engine=target_s3,
                              client=s3_client)
                lob_count += 1
                result += 1
                lob_data = None

            # proceed to the next LOB
            first_chunk = True

    # log the migration
    logger.debug(msg=f"{lob_count} LOBs migrated from "
                     f"{target_table}.{lob_column} to {session_spots[MigSpot.TO_S3]}")

    return result


def __build_identifier(values: list[Any]) -> str:

    # instantiate the hasher
    hasher = hashlib.new(name="sha256")

    # compute the hash
    for value in values:
        hasher.update(pickle.dumps(obj=value))

    # return the hash in hex format
    return hasher.digest().hex()
