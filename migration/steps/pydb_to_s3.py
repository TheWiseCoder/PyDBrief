import hashlib
import mimetypes
import pickle
from logging import Logger
from pypomes_core import Mimetype, file_get_mimetype, file_get_extension, str_from_any
from pypomes_db import db_stream_lobs
from pypomes_s3 import s3_data_store
from pathlib import Path
from typing import Any

from entities.migration import Migration
from entities.migration_issue import MigrationIssue, IssueType
from entities.session import Session, sessions_aborting


def s3_migrate_lobs(migration: Migration,
                    session: Session,
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
                    chunk_size: int,
                    migration_warnings: list[str],
                    errors: list[str],
                    logger: Logger) -> tuple[int, int]:

    # initialize the counters
    result_count: int = 0
    result_size: int = 0

    # retrieve the configuration for the migration
    source_db: str = session.get_source_db().cd_engine
    target_db: str = session.get_target_db().cd_engine
    target_s3: str = session.get_target_s3().cd_engine

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
                MigrationIssue.new_issue(id_migration=migration.id,
                                         cd_type=IssueType.WARNING,
                                         ds_issue=warn_msg)

    # initialize the remaining properties
    identifier: str | None = None
    mimetype: Mimetype | str | None = None
    extension: str | None = None
    lob_data: bytes | None = None
    metadata: dict[str, str] = {}
    first_chunk: bool = True

    # get data from the LOB streamer Generator as follows:
    #   - 'row_data' hold the streamed data (LOB identification or LOB payload)
    #   - a 'dict' identifying the LOB is sent (flagged by 'first_chunk')
    #   - if the LOB is null, one null payload follows, terminating the LOB
    #   - if the LOB is empty, one empty and one null payload follow in sequence, terminating the LOB
    #   - if the LOB has data, multiple payloads follow, until a null payload terminates the LOB
    for row_data in db_stream_lobs(table=source_table,
                                   lob_column=lob_column,
                                   pk_columns=pk_columns,
                                   ret_column=reference_column,
                                   engine=source_db,
                                   connection=db_conn,
                                   where_clause=where_clause,
                                   orderby_clause=reference_column,
                                   offset_count=offset_count,
                                   limit_count=limit_count,
                                   chunk_size=chunk_size,
                                   errors=errors):

        # verify whether current migration is marked for abortion
        if session.cd_session in sessions_aborting:
            sessions_aborting.remove(session.cd_session)
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
                    metadata[key] = str_from_any(value)
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
                lob_data += bytes(row_data, "utf-8")
        else:
            # no more data
            if lob_data:
                # determine LOB's mimetype and file extension
                if not mimetype:
                    if migration.is_reflect_filetype:
                        mimetype = file_get_mimetype(file_data=lob_data)
                        if mimetype:
                            extension = file_get_extension(mimetype=mimetype)
                    else:
                        mimetype = Mimetype.BINARY
                        extension = ".bin"
                # add extension
                if extension:
                    identifier += extension

                # send lob data to S3
                # expected response:
                # {
                #    "object_name": <string>,
                #    "version_id": <string>,
                #    "etag": <string>,
                #    "size": <int>             (AWS only)
                # }
                reply: dict[str, Any] = s3_data_store(identifier=identifier,
                                                      data=lob_data,
                                                      length=len(lob_data),
                                                      mimetype=mimetype,
                                                      tags=metadata,
                                                      prefix=lob_prefix,
                                                      engine=target_s3,
                                                      client=s3_client,
                                                      errors=errors)
                if reply:
                    result_count += 1
                    result_size += len(lob_data)
                elif not errors:
                    warn_msg: str = ("No reply received on uploading "
                                     f"'{Path(lob_prefix) / identifier}' to {target_s3}")
                    migration_warnings.append(warn_msg)
                    logger.warning(msg=warn_msg)
                    MigrationIssue.new_issue(id_migration=migration.id,
                                             cd_type=IssueType.WARNING,
                                             ds_issue=warn_msg)
                lob_data = None
            else:
                logger.warning(f"Attempted to migrate empty LOB '{identifier}'")

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
