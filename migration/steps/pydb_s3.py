import filetype
import hashlib
import pickle
from contextlib import suppress
from logging import Logger
from pathlib import Path
from pypomes_core import str_from_any
from pypomes_db import db_get_param, db_stream_lobs
from pypomes_http import MIMETYPE_BINARY, MIMETYPE_TEXT
from pypomes_s3 import s3_get_client, s3_data_store
from typing import Any
from urlobject import URLObject

from migration.pydb_common import MIGRATION_CHUNK_SIZE


def s3_migrate_lobs(errors: list[str],
                    target_s3: str,
                    target_rdbms: str,
                    target_table: str,
                    source_rdbms: str,
                    source_table: str,
                    lob_column: str,
                    pk_columns: list[str],
                    add_extensions: bool,
                    source_conn: Any,
                    logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # build the location of the data
    url: URLObject = URLObject(db_get_param(key="host",
                                            engine=target_rdbms))
    # 'url.hostname' returns 'None' for 'localhost'
    host: str = url.hostname or str(url)
    prefix: Path = Path(f"{target_rdbms}@{host}",
                        target_table[:target_table.index(".")],
                        target_table[target_table.index(".")+1:],
                        lob_column)

    # obtain the S3 client
    client: Any = s3_get_client(errors=errors,
                                engine=target_s3,
                                logger=logger)
    # was the S3 client obtained ?
    if client:
        # yes initialize the properties
        identifier: str | None = None
        mimetype: str | None = None
        lob_data: bytes = bytes()
        metadata: dict[str, str] = {}
        first_chunk: bool = True

        # get data from the LOB streamer
        # noinspection PyTypeChecker
        for row_data in db_stream_lobs(errors=errors,
                                       table=source_table,
                                       lob_column=lob_column,
                                       pk_columns=pk_columns,
                                       engine=source_rdbms,
                                       connection=source_conn,
                                       committable=True,
                                       chunk_size=MIGRATION_CHUNK_SIZE,
                                       logger=logger):
            # new LOB
            if first_chunk:
                # the initial data is a 'dict' with the values of the row's PK columns
                values: list[Any] = []
                metadata = {}
                for key, value in row_data.items():
                    values.append(value)
                    metadata[key] = str_from_any(source=value)
                # the LOB's identifier is a hex-formatted hash on the contents of the row's PK columns
                identifier = __build_identifier(values=values)
                metadata["rdbms"] = target_rdbms
                metadata["table"] = target_table
                lob_data = bytes()
                mimetype = None
                first_chunk = False
            # data chunks
            elif row_data:
                # add to LOB data
                if isinstance(row_data, bytes):
                    lob_data += row_data
                    if not mimetype:
                        mimetype = MIMETYPE_BINARY
                else:
                    lob_data += bytes(row_data, "utf-8")
                    if not mimetype:
                        mimetype = MIMETYPE_TEXT
            # no more data
            else:
                # determine LOB's mimetype and add a file extension to its identifier
                with suppress(TypeError):
                    kind: filetype.Type = filetype.guess(obj=lob_data)
                    if kind:
                        mimetype = kind.mime
                        if add_extensions:
                            identifier += f".{kind.extension}"

                # send it to S3
                s3_data_store(errors=errors,
                              prefix=prefix,
                              identifier=identifier,
                              data=lob_data,
                              length=len(lob_data),
                              mimetype=mimetype,
                              tags=metadata,
                              engine=target_s3,
                              client=client,
                              logger=logger)
                result += 1
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
