from logging import DEBUG, WARNING, Logger
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import TypeEngine
from typing import Any, Final

from . import pydb_common

# 'NullType' is used as a default type for those cases where a type cannot be determined.
# It is always converted to the target plataform's string type.
from sqlalchemy.types import NullType as Ref_NullType

# Generic types specify a column that can read, write and store a particular type of Python data.
# noinspection PyUnresolvedReferences
from sqlalchemy.types import (
    BigInteger as Ref_BigInteger,      # a type for bigger int integers
    Boolean as Ref_Boolean,            # a bool datatype
    Date as Ref_Date,                  # a type for datetime.date() objects
    DateTime as Ref_DateTime,          # a type for datetime.datetime() objects
    Double as Ref_Double,              # a type for double FLOAT floating point types
    Enum as Ref_Enum,                  # generic Enum Type
    Float as Ref_Float,                # type representing floating point types, such as FLOAT or REAL
    Integer as Ref_Integer,            # a type for int integers
    Interval as Ref_Interval,          # a type for datetime.timedelta() objects
    LargeBinary as Ref_LargeBinary,    # a type for large binary byte data
    MatchType as Ref_MatchType,        # Refers to the return type of the MATCH operator
    Numeric as Ref_Numeric,            # base for non-integer numeric types, such as NUMERIC, FLOAT, DECIMAL
    PickleType as Ref_PickleType,      # holds Python objects, which are serialized using pickle
    SchemaType as Ref_SchemaType,      # sdd capabilities to a type which allow for schema-level DDL association
    SmallInteger as Ref_SmallInteger,  # a type for smaller int integers
    String as Ref_String,              # the base for all string and character types
    Text as Ref_Text,                  # a variably sized string type
    Time as Ref_Time,                  # a type for datetime.time() objects
    Unicode as Ref_Unicode,            # a variable length Unicode string type
    UnicodeText as Ref_UnicodeText,    # an unbounded-length Unicode string type
    Uuid as Ref_Uuid                   # represents a database agnostic UUID datatype
)

# This category of types refers to types that are either part of the SQL standard,
# or are potentially found within a subset of database backends.
# Unlike the “generic” types, the SQL standard/multi-vendor types have no guarantee of working
# on all backends, and will only work on those backends that explicitly support them by name.
# noinspection PyUnresolvedReferences
from sqlalchemy.types import (
    ARRAY as REF_ARRAY,                        # represents a SQL Array type
    BIGINT as REF_BIGINT,                      # the SQL BIGINT type
    BINARY as REF_BINARY,                      # the SQL BINARY type
    BLOB as REF_BLOB,                          # the SQL BLOB type
    BOOLEAN as REF_BOOLEAN,                    # the SQL BOOLEAN type
    CHAR as REF_CHAR,                          # the SQL CHAR type
    CLOB as REF_CLOB,                          # the CLOB type
    DATE as REF_DATE,                          # the SQL DATE type
    DATETIME as REF_DATETIME,                  # the SQL DATETIME type.
    DECIMAL as REF_DECIMAL,                    # the SQL DECIMAL type
    DOUBLE as REF_DOUBLE,                      # the SQL DOUBLE type
    DOUBLE_PRECISION as REF_DOUBLE_PRECISION,  # the SQL DOUBLE PRECISION type
    FLOAT as REF_FLOAT,                        # he SQL FLOAT type
    INT as REF_,                               # alias of INTEGER
    INTEGER as REF_INT,                        # the SQL INT or INTEGER type
    JSON as REF_JSON,                          # represents a SQL JSON type
    NCHAR as REF_NCHAR,                        # the SQL NCHAR type
    NUMERIC as REF_NUMERIC,                    # the SQL NUMERIC type
    NVARCHAR as REF_NVARCHAR,                  # the SQL NVARCHAR type
    REAL as REF_REAL,                          # the SQL REAL type
    SMALLINT as REF_SMALLINT,                  # the SQL SMALLINT type
    TEXT as REF_TEXT,                          # the SQL TEXT type
    TIME as REF_TIME,                          # the SQL TIME type
    TIMESTAMP as REF_TIMESTAMP,                # the SQL TIMESTAMP type
    UUID as REF_UUID,                          # represents the SQL UUID type
    VARBINARY as REF_VARBINARY,                # the SQL VARBINARY type
    VARCHAR as REF_VARCHAR                     # the SQL VARCHAR type
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.mysql import (
    BIGINT as MSQL_BIGINT,
    BINARY as MSQL_BINARY,
    BIT as MSQL_BIT,
    BLOB as MSQL_BLOB,
    BOOLEAN as MSQL_BOOLEAN,
    CHAR as MSQL_CHAR,
    DATE as MSQL_DATE,
    DATETIME as MSQL_DATETIME,
    DECIMAL as MSQL_DECIMAL,
    DECIMAL as MSQL_DECIMAL,
    DOUBLE as MSQL_DOUBLE,
    ENUM as MSQL_ENUM,
    FLOAT as MSQL_FLOAT,
    INTEGER as MSQL_INTEGER,
    JSON as MSQL_JSON,
    LONGBLOB as MSQL_LONGBLOB,
    LONGTEXT as MSQL_LONGTEXT,
    MEDIUMBLOB as MSQL_MEDIUMBLOB,
    MEDIUMINT as MSQL_MEDIUMINT,
    MEDIUMTEXT as MSQL_MEDIUMTEXT,
    NCHAR as MSQL_NCHAR,
    NUMERIC as MSQL_NUMERIC,
    NVARCHAR as MSQL_NVARCHAR,
    REAL as MSQL_REAL,
    SET as MSQL_SET,
    SMALLINT as MSQL_SMALLINT,
    TEXT as MSQL_TEXT,
    TIME as MSQL_TIME,
    TIMESTAMP as MSQL_TIMESTAMP,
    TINYBLOB as MSQL_TINYBLOB,
    TINYINT as MSQL_TINYINT,
    TINYTEXT as MSQL_TINYTEXT,
    VARBINARY as MSQL_VARBINARY,
    VARCHAR as MSQL_VARCHAR,
    YEAR as MSQL_YEAR
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.oracle import (
    BFILE as ORCL_BFILE,
    BINARY_DOUBLE as ORCL_BINARY_DOUBLE,
    BINARY_FLOAT as ORCL_BINARY_DOUBLE,
    BLOB as ORCL_BLOB,
    CHAR as ORCL_CHAR,
    CLOB as ORCL_CLOB,
    DATE as ORCL_DATE,
    DOUBLE_PRECISION as ORCL_DOUBLE_PRECISION,
    FLOAT as ORCL_FLOAT,
    INTERVAL as ORCL_INTERVAL,
    LONG as ORCL_LONG,
    NCLOB as ORCL_NCLOB,
    NCHAR as ORCL_NCHAR,
    NUMBER as ORCL_NUMBER,
    NVARCHAR as ORCL_NVARCHAR,
    NVARCHAR2 as ORCL_NVARCHAR2,
    RAW as ORCL_RAW,
    ROWID as ORCL_ROWID,
    TIMESTAMP as ORCL_TIMESTAMP,
    VARCHAR as ORCL_VARCHAR,
    VARCHAR2 as ORCL_VARCHAR2
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.postgresql import (
    ARRAY as PG_ARRAY,
    BIGINT as PG_BIGINT,
    BIT as PG_BIT,
    BOOLEAN as PG_BOOLEAN,
    BYTEA as PG_BYTEA,
    CHAR as PG_CHAR,
    CIDR as PG_CIDR,
    CITEXT as PG_CITEXT,
    DATE as PG_DATE,
    DATEMULTIRANGE as PG_DATEMULTIRANGE,
    DATERANGE as PG_DATERANGE,
    DOMAIN as PG_DOMAIN,
    DOUBLE_PRECISION as PG_DOUBLE_PRECISION,
    ENUM as PG_ENUM,
    FLOAT as PG_FLOAT,
    HSTORE as PG_HSTORE,
    INET as PG_INET,
    INT4MULTIRANGE as PG_INT4MULTIRANGE,
    INT4RANGE as PG_INT4RANGE,
    INT8MULTIRANGE as PG_INT8MULTIRANGE,
    INT8RANGE as PG_INT8RANGE,
    INTEGER as PG_INTEGER,
    INTERVAL as INTERVAL,
    JSON as PG_JSON,
    JSONB as PG_JSONB,
    JSONPATH as PG_JSONPATH,
    MACADDR as PG_MACADDR,
    MACADDR8 as PG_MACADDR8,
    MONEY as PG_MONEY,
    NUMERIC as PG_NUMERIC,
    NUMMULTIRANGE as PG_NUMMULTIRANGE,
    NUMRANGE as PG_NUMRANGE,
    OID as PG_OID,
    REAL as PG_REAL,
    REGCLASS as PG_REGCLASS,
    REGCONFIG as PG_REGCONFIG,
    SMALLINT as PG_SMALLINT,
    TEXT as PG_TEXT,
    TIME as PG_TIME,
    TIMESTAMP as PG_TIMESTAMP,
    TSMULTIRANGE as PG_TSMULTIRANGE,
    TSQUERY as PG_TSQUERY,
    TSRANGE as PG_TSRANGE,
    TSTZMULTIRANGE as PG_TSTZMULTIRANGE,
    TSTZRANGE as PG_TSTZRANGE,
    TSVECTOR as PG_TSVECTOR,
    UUID as PG_UUID,
    VARCHAR as PG_VARCHAR
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.mssql import (
    BIGINT as SQLS_BIGINT,
    BINARY as SQLS_BINARY,
    BIT as SQLS_BIT,
    CHAR as SQLS_CHAR,
    DATE as SQLS_DATE,
    DATETIME as SQLS_DATETIME,
    DATETIME2 as SQLS_DATETIME2,
    DATETIMEOFFSET as SQLS_DATETIMEOFFSET,
    DECIMAL as SQLS_DECIMAL,
    DOUBLE_PRECISION as SQLS_DOUBLE_PRECISION,
    FLOAT as SQLS_FLOAT,
    IMAGE as SQLS_IMAGE,
    INTEGER as SQLS_INTEGER,
    JSON as SQLS_INTEGER,
    MONEY as SQLS_MONEY,
    NCHAR as SQLS_NCHAR,
    NTEXT as SQLS_NTEXT,
    NUMERIC as SQLS_NUMERIC,
    NVARCHAR as SQLS_NVARCHAR,
    REAL as SQLS_REAL,
    ROWVERSION as SQLS_ROWVERSION,
    SMALLDATETIME as SQLS_SMALLDATETIME,
    SMALLINT as SQLS_SMALLINT,
    SMALLMONEY as SQLS_SMALLMONEY,
    SQL_VARIANT as SQLS_SQL_VARIANT,
    TEXT as SQLS_TEXT,
    TIME as SQLS_TIME,
    TIMESTAMP as SQLS_TIMESTAMP,
    TINYINT as SQLS_TINYINT,
    UNIQUEIDENTIFIER as SQLS_UNIQUEIDENTIFIER,
    VARBINARY as SQLS_VARBINARY,
    VARCHAR as SQLS_VARCHAR,
    XML as SQLS_XML
)

# Reference - MySQL - Oracle - PostgreSQL - SQLServer
REF_EQUIVALENCES: Final[list[tuple]] = [
    (Ref_Date, MSQL_DATE, ORCL_DATE, PG_DATE, SQLS_DATE),
    (Ref_DateTime, MSQL_DATETIME, ORCL_DATE, PG_TIMESTAMP, SQLS_DATETIME),
    (Ref_LargeBinary, MSQL_LONGBLOB, ORCL_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (Ref_Numeric, MSQL_NUMERIC, ORCL_NUMBER, PG_NUMERIC, SQLS_NUMERIC),
    (Ref_NullType, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
    (Ref_String, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
    (Ref_Text, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
    (REF_BLOB, MSQL_LONGBLOB, ORCL_CLOB, PG_BYTEA, SQLS_VARBINARY),
    (REF_CLOB, MSQL_TEXT, ORCL_CLOB, PG_TEXT, None),
    (REF_CHAR, MSQL_CHAR, ORCL_CHAR, PG_CHAR, SQLS_CHAR),
    (REF_FLOAT, MSQL_FLOAT, ORCL_FLOAT, PG_FLOAT, SQLS_FLOAT),
    (REF_NVARCHAR, MSQL_NVARCHAR, ORCL_NVARCHAR2, PG_VARCHAR, SQLS_NVARCHAR),
]

# MySQL - Oracle - PostgreSQL - SLServer
MSQL_EQUIVALENCES: Final[list[tuple]] = [
    (MSQL_BLOB, ORCL_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (MSQL_CHAR, ORCL_CHAR, PG_CHAR, SQLS_CHAR),
    (MSQL_DATETIME, ORCL_TIMESTAMP, PG_TIMESTAMP, SQLS_DATETIME),
    (MSQL_NUMERIC, ORCL_NUMBER, PG_NUMERIC, SQLS_NUMERIC),
    (MSQL_NVARCHAR, ORCL_NVARCHAR2, PG_VARCHAR, SQLS_NVARCHAR),
    (MSQL_FLOAT, ORCL_FLOAT, PG_FLOAT, SQLS_FLOAT),
    (MSQL_LONGTEXT, ORCL_LONG, PG_TEXT, SQLS_TEXT),
    (MSQL_LONGBLOB, ORCL_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (MSQL_MEDIUMBLOB, ORCL_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (MSQL_TEXT, ORCL_CLOB, PG_TEXT, None),
    (MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP, SQLS_TIMESTAMP),
    (MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
]

# Oracle - MySQL - PostgreSQL - SQLServer
ORCL_EQUIVALENCES: Final[list[tuple]] = [
    (ORCL_BLOB, MSQL_LONGBLOB, PG_BYTEA, SQLS_VARBINARY),
    (ORCL_CHAR, MSQL_CHAR, PG_CHAR, SQLS_CHAR),
    (ORCL_CLOB, MSQL_TEXT, PG_TEXT, None),
    (ORCL_DATE, MSQL_DATETIME, PG_TIMESTAMP, SQLS_DATETIME),
    (ORCL_FLOAT, MSQL_FLOAT, PG_FLOAT, SQLS_FLOAT),
    (ORCL_LONG, MSQL_LONGTEXT, PG_TEXT, SQLS_TEXT),
    (ORCL_NUMBER, MSQL_NUMERIC, PG_NUMERIC, SQLS_NUMERIC),
    (ORCL_NVARCHAR2, MSQL_NVARCHAR, PG_VARCHAR, SQLS_NVARCHAR),
    (ORCL_RAW, MSQL_LONGBLOB, PG_BYTEA, SQLS_VARBINARY),
    (ORCL_VARCHAR2, MSQL_VARCHAR, PG_VARCHAR, SQLS_VARCHAR),
    (ORCL_TIMESTAMP, MSQL_DATETIME, PG_TIMESTAMP, SQLS_DATETIME),
]

# PostgreSQL - MySQL - Oracle - SQLServer
PG_EQUIVALENCES: Final[list[tuple]] = [
    (PG_BYTEA, MSQL_LONGBLOB, ORCL_BLOB, SQLS_VARBINARY),
    (PG_CHAR, MSQL_CHAR, ORCL_CHAR, SQLS_CHAR),
    (PG_NUMERIC, MSQL_NUMERIC, ORCL_NUMBER, SQLS_NUMERIC),
    (PG_FLOAT, MSQL_FLOAT, ORCL_FLOAT, SQLS_FLOAT),
    (PG_TEXT, MSQL_LONGTEXT, ORCL_LONG, SQLS_TEXT),
    (PG_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, SQLS_TIMESTAMP),
    (PG_VARCHAR, MSQL_VARCHAR, ORCL_VARCHAR2, SQLS_VARCHAR),
]

# SQLServer - MySQL - Oracle - PostgreSQL
SQLS_EQUIVALENCES: Final[list[tuple]] = [
    (SQLS_CHAR, MSQL_CHAR, ORCL_CHAR, PG_CHAR),
    (SQLS_DATETIME, MSQL_DATETIME, ORCL_DATE, PG_TIMESTAMP),
    (SQLS_FLOAT, MSQL_FLOAT, ORCL_FLOAT, PG_FLOAT),
    (SQLS_NUMERIC, MSQL_NUMERIC, ORCL_NUMBER, PG_NUMERIC),
    (SQLS_NVARCHAR, MSQL_NVARCHAR, ORCL_NVARCHAR2, PG_VARCHAR),
    (SQLS_TEXT, MSQL_LONGTEXT, ORCL_LONG, PG_TEXT),
    (SQLS_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP),
    (SQLS_VARBINARY, MSQL_LONGBLOB, ORCL_BLOB, PG_BYTEA),
    (SQLS_VARCHAR, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR),
]


def convert_type(source_rdbms: str,
                 target_rdbms: str,
                 source_column: Column,
                 logger: Logger) -> Any:

    target_type: TypeEngine | None = None

    col_type: TypeEngine = source_column.type
    col_name: str = f"{source_column.table.name}.{source_column.name}"
    (nat_equivalences, nat_ordinal, ref_ordinal) = _get_equivalences(source_rdbms, target_rdbms)

    # check the reference equivalences
    for ref_equivalence in REF_EQUIVALENCES:
        if col_type == ref_equivalence[0]:
            target_type = ref_equivalence[ref_ordinal]
            break

    # check the native equivalences, if necessary
    if target_type is None:
        for nat_equivalence in nat_equivalences:
            if col_type == nat_equivalence[0]:
                target_type = nat_equivalence[nat_ordinal]
                break

    # log the result
    msg: str = f"From {source_rdbms} to {target_rdbms}, type {str(col_type)} for column {col_name}"
    if target_type is None:
        target_type = col_type
        pydb_common.log(logger, WARNING,
                        f"{msg}  - unable to convert")
    else:
        pydb_common.log(logger, DEBUG,
                        f"{msg} converted to type {str(target_type)}")

    # noinspection PyCallingNonCallable
    return target_type()


def _get_equivalences(source_rdbms: str, target_rdbms: str) -> tuple[list[tuple], int, int]:

    nat_equivalences: list[tuple] | None = None
    match source_rdbms:
        case "mysql":
            nat_equivalences = MSQL_EQUIVALENCES
        case "oracle":
            nat_equivalences = ORCL_EQUIVALENCES
        case "postgres":
            nat_equivalences = PG_EQUIVALENCES
        case "sqlserver":
            nat_equivalences = SQLS_EQUIVALENCES

    nat_ordinal: int | None = None
    ref_ordinal: int | None = None
    match target_rdbms:
        case "mysql":
            ref_ordinal = 1
            match source_rdbms:
                case "oracle":
                    nat_ordinal = 1
                case "postgres":
                    nat_ordinal = 2
                case "sqlserver":
                    nat_ordinal = 3
        case "oracle":
            ref_ordinal = 2
            match source_rdbms:
                case "mysql":
                    nat_ordinal = 1
                case "postgres":
                    nat_ordinal = 2
                case "sqlserver":
                    nat_ordinal = 3
        case "postgres":
            ref_ordinal = 3
            match source_rdbms:
                case "mysql":
                    nat_ordinal = 1
                case "oracle":
                    nat_ordinal = 2
                case "sqlserver":
                    nat_ordinal = 3
        case "sqlserver":
            ref_ordinal = 4
            match source_rdbms:
                case "mysql":
                    nat_ordinal = 1
                case "oracle":
                    nat_ordinal = 2
                case "postgres":
                    nat_ordinal = 3

    return nat_equivalences, nat_ordinal, ref_ordinal
