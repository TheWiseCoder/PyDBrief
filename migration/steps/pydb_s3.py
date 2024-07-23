import hashlib
import pickle
from logging import Logger
from pathlib import Path
from pypomes_db import db_stream_lobs
from pypomes_http import MIMETYPE_BINARY, MIMETYPE_TEXT
from pypomes_s3 import s3_get_client, s3_data_store
from typing import Any

from migration.pydb_common import MIGRATION_CHUNK_SIZE


def s3_migrate_lobs(errors: list[str],
                    target_s3: str,
                    target_schema: str,
                    source_rdbms: str,
                    source_schema: str,
                    source_table: str,
                    table_lob: str,
                    table_pks: list[str],
                    source_conn: Any,
                    logger: Logger) -> int:

    # initialize the return variable
    result: int = 0

    # build the location of the data
    basepath: Path = Path(target_schema,
                          source_table.replace(f"{source_schema}.", ""),
                          table_lob)

    # obtain the S3 client
    client: Any = s3_get_client(errors=errors,
                                engine=target_s3,
                                logger=logger)

    # was the S3 client obtained ?
    if client:
        # yes proceed
        first: bool = True
        identifier: str | None = None
        mimetype: str | None = None
        lob_data: bytes = bytes()

        # get data from the LOB streamer
        # noinspection PyTypeChecker
        for row_data in db_stream_lobs(errors=errors,
                                       table=source_table,
                                       lob_column=table_lob,
                                       pk_columns=table_pks,
                                       engine=source_rdbms,
                                       connection=source_conn,
                                       committable=True,
                                       chunk_size=MIGRATION_CHUNK_SIZE,
                                       logger=logger):
            if first:
                # the initial data is a dict with the values of the row's PK columns
                data: list[Any] = list(row_data.values())
                identifier = __build_identifier(data=data)
                first = False
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
            else:
                # end of LOB data, send it to S3
                s3_data_store(errors=errors,
                              basepath=basepath,
                              identifier=identifier,
                              data=lob_data,
                              length=len(lob_data),
                              mimetype=mimetype,
                              engine=target_s3,
                              client=client,
                              logger=logger)
                identifier = None
                mimetype = None
                lob_data = bytes()
                result += 1
                first = True

    return result


def __build_identifier(data: list[Any]) -> str:

    # instantiate the hasher
    hasher = hashlib.new(name="sha256")

    # compute the hash
    for datum in data:
        hasher.update(pickle.dumps(obj=datum))

    # return the hash in hex format
    return hasher.digest().hex()
