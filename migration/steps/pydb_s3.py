import hashlib
import mimetypes
import pickle
from logging import Logger
from pypomes_core import (
    Mimetype,
    file_get_mimetype, file_is_binary, str_from_any
)
from pypomes_db import DbEngine, db_stream_lobs
from pypomes_s3 import (
    S3Engine,
    s3_data_store, s3_startup, s3_get_client
)
from pathlib import Path
from typing import Any

from app_constants import MetricsConfig
from migration.pydb_common import assert_abort_state, get_metrics_params


def s3_migrate_lobs(errors: list[str],
                    target_s3: S3Engine,
                    target_rdbms: DbEngine,
                    target_table: str,
                    source_rdbms: DbEngine,
                    source_table: str,
                    lob_prefix: Path,
                    lob_column: str,
                    pk_columns: list[str],
                    where_clause: str,
                    limit_count: int,
                    offset_count: int,
                    reflect_filetype: bool,
                    forced_filetype: str,
                    ret_column: str,
                    source_conn: Any,
                    session_id: str,
                    logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # start the S3 module and obtain the S3 client
    client: Any = None
    if s3_startup(errors=errors,
                  engine=target_s3,
                  logger=logger):
        client = s3_get_client(errors=errors,
                               engine=target_s3,
                               logger=logger)

    # was the S3 client obtained ?
    if client:
        # yes, proceed
        chunk_size: int = get_metrics_params(session_id=session_id).get(MetricsConfig.CHUNK_SIZE)
        forced_mimetype: str = mimetypes.types_map.get(forced_filetype)

        # initialize the properties
        identifier: str | None = None
        mimetype: Mimetype | str | None = None
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
                                       engine=source_rdbms,
                                       connection=source_conn,
                                       committable=True,
                                       where_clause=where_clause,
                                       offset_count=offset_count,
                                       limit_count=limit_count,
                                       chunk_size=chunk_size,
                                       logger=logger):

            # verify whether current migration is marked for abortion
            assert_abort_state(errors=errors,
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
                    "rdbms": target_rdbms,
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
                lob_data = None
                mimetype = None
                first_chunk = False

            # data chunks
            elif row_data is not None:
                # add to LOB data
                if lob_data is None:
                    lob_data = b""
                if isinstance(row_data, bytes):
                    lob_data += row_data
                    if not mimetype:
                        mimetype = Mimetype.BINARY
                else:
                    lob_data += bytes(row_data, "utf-8")
                    if not mimetype:
                        mimetype = Mimetype.TEXT

            # no more data
            else:
                # send LOB data
                if lob_data is not None:
                    extension: str = forced_filetype
                    # has filetype reflection been specified ?
                    if reflect_filetype:
                        # yes, determine LOB's mimetype and file extension
                        mimetype = file_get_mimetype(file_data=lob_data) or \
                                   Mimetype.BINARY if file_is_binary(file_data=lob_data) else Mimetype.TEXT
                        extension = mimetypes.guess_extension(type=mimetype)
                    # add extension
                    if extension:
                        identifier += extension
                    # final consideration on mimetype
                    if not mimetype:
                        mimetype = forced_mimetype or Mimetype.BINARY

                    # send it to S3 (logging individual LOBs sent to S3 storage risks writing too many lines)
                    s3_data_store(errors=errors,
                                  identifier=identifier,
                                  data=lob_data,
                                  length=len(lob_data),
                                  mimetype=mimetype,
                                  tags=metadata,
                                  prefix=lob_prefix,
                                  engine=target_s3,
                                  client=client)
                    lob_count += 1
                    result += 1
                    lob_data = None

                # proceed to the next LOB
                first_chunk = True

        # log the migration
        logger.debug(msg=f"{lob_count} LOBs migrated from {target_table}.{lob_column} to S3 storage")

    return result


def __build_identifier(values: list[Any]) -> str:

    # instantiate the hasher
    hasher = hashlib.new(name="sha256")

    # compute the hash
    for value in values:
        hasher.update(pickle.dumps(obj=value))

    # return the hash in hex format
    return hasher.digest().hex()
