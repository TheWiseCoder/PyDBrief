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
                    db_conn: Any,
                    s3_client: Any,
                    source_table: str,
                    target_table: str,
                    lob_prefix: Path,
                    lob_column: str,
                    pk_columns: list[str],
                    where_clause: str,
                    offset_count: int,
                    limit_count: int,
                    forced_filetype: str,
                    reference_column: str,
                    migration_warnings: list[str],
                    logger: Logger) -> tuple[int, int]:

    # initialize the counters
    result_count: int = 0
    result_size: int = 0

    # retrieve the registry data for the session
    session_registry: dict[StrEnum, Any] = get_session_registry(session_id=session_id)
    session_metrics: dict[MigMetric, Any] = session_registry[MigConfig.METRICS]
    session_specs: dict[MigSpec, Any] = session_registry[MigConfig.SPECS]
    session_spots: dict[MigSpot, Any] = session_registry[MigConfig.SPOTS]

    # retrieve the configuration for the migration
    source_db: DbEngine = session_spots[MigSpot.FROM_RDBMS]
    target_db: DbEngine = session_spots[MigSpot.TO_RDBMS]
    target_s3: S3Engine = session_spots[MigSpot.TO_S3]
    chunk_size: int = session_metrics[MigMetric.CHUNK_SIZE]

    # initialize the file and mime types
    forced_mimetype: Mimetype | None = None
    if forced_filetype:
        filetype: str = forced_filetype[1:].upper()
        if filetype in Mimetype._member_names_:
            # noinspection PyTypeChecker
            forced_mimetype = Mimetype[filetype]
        else:
            forced_mimetype = mimetypes.guess_type(f"x{forced_filetype}")[0]
            if not forced_mimetype:
                warn_msg: str = f"Unable fo obtain a mimetype for forced filetype '{forced_filetype}'"
                migration_warnings.append(warn_msg)
                logger.warning(msg=warn_msg)

    # initialize the remaining properties
    identifier: str | None = None
    mimetype: Mimetype | str | None = None
    extension: str | None = None
    lob_data: bytes | None = None
    metadata: dict[str, str] = {}
    first_chunk: bool = True

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
                                   ret_column=reference_column,
                                   engine=source_db,
                                   connection=db_conn,
                                   where_clause=where_clause,
                                   offset_count=offset_count,
                                   limit_count=limit_count,
                                   chunk_size=chunk_size,
                                   logger=logger):

        # verify whether current migration is marked for abortion
        if errors or assert_session_abort(errors=errors,
                                          session_id=session_id,
                                          logger=logger):
            # abort the lobdata streaming
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
                "rdbms": target_db,
                "table": target_table
            }
            for key, value in sorted(row_data.items()):
                if key == reference_column:
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
            if lob_data is None:
                logger.warning(f"Attempted to migrate empty LOB '{identifier}'")
            else:
                # has filetype reflection been specified ?
                if not mimetype and session_specs[MigSpec.REFLECT_FILETYPE]:
                    # yes, determine LOB's mimetype and file extension
                    mimetype = file_get_mimetype(file_data=lob_data)
                    extension = file_get_extension(mimetype=mimetype)
                # add extension
                if extension:
                    identifier += extension

                # send it to S3
                # expected response:
                # {
                #    "object_name": <string>,
                #    "version_id": <string>,
                #    "etag": <string>,
                #    "size": <int>             (AWS only)
                # }
                reply: dict[str, Any] = s3_data_store(errors=errors,
                                                      identifier=identifier,
                                                      data=lob_data,
                                                      length=len(lob_data),
                                                      mimetype=mimetype or Mimetype.BINARY,
                                                      tags=metadata,
                                                      prefix=lob_prefix,
                                                      engine=target_s3,
                                                      client=s3_client)
                if reply:
                    result_count += 1
                    result_size += len(lob_data)
                elif not errors:
                    warn_msg: str = ("No reply received on uploading "
                                     f"'{Path(lob_prefix) / identifier}' to {target_s3}")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)
                lob_data = None

            # proceed to the next LOB
            first_chunk = True

    # log the migration
    logger.debug(msg=f"{result_count} LOBs migrated from "
                     f"{source_table}.{lob_column} to {target_s3}")

    return result_count, result_size


def __build_identifier(values: list[Any]) -> str:

    # instantiate the hasher
    hasher = hashlib.new(name="sha256")

    # compute the hash
    for value in values:
        hasher.update(pickle.dumps(obj=value))

    # return the hash in hex format
    return hasher.digest().hex()
