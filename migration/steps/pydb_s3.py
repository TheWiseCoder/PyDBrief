import hashlib
import mimetypes
import pickle
from logging import Logger
from pypomes_core import (
    Mimetype,
    file_get_mimetype, file_is_binary, str_from_any
)
from pypomes_db import db_stream_lobs
from pypomes_s3 import (
    S3Engine,
    s3_data_store, s3_startup, s3_get_client
)
from pathlib import Path
from typing import Any

from migration.pydb_common import MIGRATION_METRICS, Metrics


def s3_migrate_lobs(errors: list[str],
                    target_s3: S3Engine,
                    target_rdbms: str,
                    target_table: str,
                    source_rdbms: str,
                    source_table: str,
                    lob_prefix: Path,
                    lob_column: str,
                    pk_columns: list[str],
                    where_clause: str,
                    accept_empty: bool,
                    offset_count: int,
                    limit_count: int,
                    reflect_filetype: bool,
                    forced_filetype: str,
                    named_column: str,
                    source_conn: Any,
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
        forced_mimetype: str = mimetypes.types_map.get(forced_filetype)

        # initialize the properties
        identifier: str | None = None
        mimetype: Mimetype | str | None = None
        lob_data: bytes = b""
        metadata: dict[str, str] = {}
        first_chunk: bool = True

        # get data from the LOB streamer
        # noinspection PyTypeChecker
        for row_data in db_stream_lobs(errors=errors,
                                       table=source_table,
                                       lob_column=lob_column,
                                       pk_columns=pk_columns,
                                       ref_column=named_column,
                                       engine=source_rdbms,
                                       connection=source_conn,
                                       committable=True,
                                       where_clause=where_clause,
                                       offset_count=offset_count,
                                       limit_count=limit_count,
                                       accept_empty=accept_empty,
                                       chunk_size=MIGRATION_METRICS.get(Metrics.CHUNK_SIZE),
                                       logger=logger):
            # new LOB
            if first_chunk:
                # the initial data is a 'dict' with the values of:
                #   - the row's PK columns
                #   - the lobdata's filename (if 'named_column' was specified)
                values: list[Any] = []
                metadata = {
                    "rdbms": target_rdbms,
                    "table": target_table
                }
                for key, value in sorted(row_data.items()):
                    if key == named_column:
                        identifier = value
                    else:
                        values.append(value)
                        metadata[key] = str_from_any(source=value)
                if not identifier:
                    # hex-formatted hash on the contents of the row's PK columns
                    identifier = __build_identifier(values=values)
                lob_data = b""
                mimetype = None
                # noinspection PyUnusedLocal
                first_chunk = False
            # data chunks
            elif row_data:
                # add to LOB data
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
                if accept_empty or lob_data:
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

                    # send it to S3
                    s3_data_store(errors=errors,
                                  identifier=identifier,
                                  data=lob_data,
                                  length=len(lob_data),
                                  mimetype=mimetype,
                                  tags=metadata,
                                  prefix=lob_prefix,
                                  engine=target_s3,
                                  client=client,
                                  logger=logger)
                    result += 1

                # proceed to the next LOB
                first_chunk = True

    return result


def __build_identifier(values: list[Any]) -> str:

    # instantiate the hasher
    hasher = hashlib.new(name="sha256")

    # compute the hash
    for value in values:
        hasher.update(pickle.dumps(obj=value))

    # return the hash in hex format
    return hasher.digest().hex()
