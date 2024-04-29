from logging import DEBUG, WARNING, Logger
from sqlalchemy.sql.schema import Column
from sqlalchemy.types import TypeEngine
from typing import Final

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
    YEAR as MSQL_YEAR,
    # types which are specific to MySQL, or have MySQL-specific construction arguments:
    BIGINT as MSQL_BIGINT,
    BIT as MSQL_BIT,
    CHAR as MSQL_CHAR,
    DATETIME as MSQL_DATETIME,
    DECIMAL as MSQL_DECIMAL,
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
    TIME as MSQL_TIME,
    TIMESTAMP as MSQL_TIMESTAMP,
    TINYBLOB as MSQL_TINYBLOB,
    TINYINT as MSQL_TINYINT,
    TINYTEXT as MSQL_TINYTEXT,
    VARCHAR as MSQL_VARCHAR,
    YEAR as MSQL_YEAR
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.oracle import (
    BFILE as ORCL_BFILE,
    BLOB as ORCL_BLOB,
    CHAR as ORCL_CHAR,
    CLOB as ORCL_CLOB,
    DATE as ORCL_DATE,
    DOUBLE_PRECISION as ORCL_DOUBLE_PRECISION,
    FLOAT as ORCL_FLOAT,
    INTERVAL as ORCL_INTERVAL,
    LONG as ORCL_LONGNCLOB,
    NCLOB as ORCL_NCLOB,
    NCHAR as ORCL_NCHAR,
    NUMBER as ORCL_NUMBER,
    NVARCHAR as ORCL_NVARCHAR,
    NVARCHAR2 as ORCL_NVARCHAR2,
    RAW as ORCL_RAW,
    TIMESTAMP as ORCL_TIMESTAMP,
    VARCHAR as ORCL_VARCHAR,
    VARCHAR2 as ORCL_VARCHAR2,
    # types which are specific to Oracle, or have Oracle-specific construction arguments:
    BFILE as ORCL_BFILE,
    BINARY_DOUBLE as ORCL_,
    BINARY_FLOAT as ORCL_BINARY_DOUBLE,
    DATE as ORCL_DATE,
    FLOAT as ORCL_FLOAT,
    INTERVAL as ORCL_INTERVAL,
    LONG as ORCL_LONG,
    NCLOB as ORCL_NCLOB,
    NUMBER as ORCL_NUMBER,
    NVARCHAR2 as ORCL_NVARCHAR2,
    RAW as ORCL_RAW,
    ROWID as ORCL_ROWID,
    TIMESTAMP as ORCL_TIMESTAMP
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
    VARCHAR as PG_VARCHAR,
    # Types which are specific to PostgreSQL as PG_, or have PostgreSQL-specific construction arguments:
    ARRAY as PG_ARRAY,
    BIT as PG_BIT,
    BYTEA as PG_BYTEA,
    CIDR as PG_CIDR,
    CITEXT as PG_CITEXT,
    DATEMULTIRANGE as PG_DATEMULTIRANGE,
    DATERANGE as PG_DATERANGE,
    DOMAIN as PG_DOMAIN,
    ENUM as PG_ENUM,
    HSTORE as PG_HSTORE,
    INET as PG_INET,
    INT4MULTIRANGE as PG_INT4MULTIRANGE,
    INT4RANGE as PG_INT4RANGE,
    INT8MULTIRANGE as PG_INT8MULTIRANGE,
    INT8RANGE as PG_INT8RANGE,
    INTERVAL as PG_INTERVAL,
    JSON as PG_JSON,
    JSONB as PG_JSONB,
    JSONPATH as PG_JSONPATH,
    MACADDR as PG_MACADDR,
    MACADDR8 as PG_MACADDR8,
    MONEY as PG_MONEY,
    NUMMULTIRANGE as PG_NUMMULTIRANGE,
    NUMRANGE as PG_NUMRANGE,
    OID as PG_NUMRANGE,
    REGCLASS as PG_REGCLASS,
    REGCONFIG as PG_REGCONFIG,
    TIME as PG_TIME,
    TIMESTAMP as TIMESTAMP,
    TSMULTIRANGE as PG_TSMULTIRANGE,
    TSQUERY as PG_TSQUERY,
    TSRANGE as PG_TSRANGE,
    TSTZMULTIRANGE as PG_TSTZMULTIRANGE,
    TSTZRANGE as PG_TSTZRANGE,
    TSVECTOR as PG_TSVECTOR
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
    # types which are specific to SQL Server, or have SQL Server-specific construction arguments:
    BIT as SQLS_BIT,
    DATETIME2 as SQLS_DATETIME2,
    DATETIMEOFFSET as SQLS_DATETIMEOFFSET,
    DOUBLE_PRECISION as SQLS_DOUBLE_PRECISION,
    IMAGE as SQLS_IMAGE,
    JSON as SQLS_JSON,
    MONEY as SQLS_MONEY,
    NTEXT as SQLS_NTEXT,
    REAL as SQLS_REAL,
    ROWVERSION as SQLS_ROWVERSION,
    SMALLDATETIME as SQLS_SMALLDATETIME,
    SMALLMONEY as SQLS_SMALLMONEY,
    SQL_VARIANT as SQLS_SQL_VARIANT,
    TIME as SQLS_TIME,
    TIMESTAMP as SQLS_TIMESTAMP,
    TINYINT as SQLS_TINYINT,
    UNIQUEIDENTIFIER as SQLS_UNIQUEIDENTIFIER,
    XML as SQLS_XML
)

REF_EQUIVALENCES: Final[list[tuple]] = [
    # (reference, mysql, oracle, postgres, sqlserver)
    (Ref_NullType, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
    (Ref_Numeric, MSQL_NUMERIC, ORCL_NUMBER, PG_NUMERIC, SQLS_NUMERIC),
    (Ref_DateTime, MSQL_DATETIME, ORCL_DATE, PG_TIMESTAMP, SQLS_DATETIME),
    (Ref_String, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
    (Ref_Text, MSQL_VARCHAR, ORCL_VARCHAR2, PG_VARCHAR, SQLS_VARCHAR),
    (REF_BLOB, MSQL_BLOB, ORCL_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (REF_NVARCHAR, MSQL_NVARCHAR, ORCL_NVARCHAR2, PG_VARCHAR, SQLS_NVARCHAR),  # PG_VARCHAR ?
]

NAMED_EQUIVALENCES: Final[list[tuple]] = [
]


def convert(source_rdbms: str,
            target_rdbms: str,
            source_column: Column,
            logger: Logger) -> TypeEngine:

    # declare the return variable
    result: TypeEngine | None = None

    col_type: TypeEngine = source_column.type
    col_name: str = f"{source_column.table.name}.{source_column.name}"
    ord_source: int = _get_ordinal(source_rdbms)
    ord_target: int = _get_ordinal(target_rdbms)

    # check the reference equivalences first
    for ref_equivalence in REF_EQUIVALENCES:
        ref_type: TypeEngine = ref_equivalence[0]
        if col_type == ref_type:
            result = ref_equivalence[ord_target+1]
            break

    # has the equivalent type been found ?
    if result is None:
        # no, check the named equivalences
        for named_equivalence in NAMED_EQUIVALENCES:
            if col_type == named_equivalence[ord_source]:
                result = named_equivalence[ord_target]

    # log the result
    msg: str = f"RDBMS {source_rdbms}, type {type(col_type).__name__} for column {col_name}"
    if result is None:
        pydb_common.log(logger, WARNING,
                        f"{msg} - unable to convert to RDBMS {target_rdbms}")
        result = col_type
    else:
        pydb_common.log(logger, DEBUG,
                        f"{msg} converted to type {type(result).__name__} to RDBMS {target_rdbms}")

    return result


def _get_ordinal(rdbms: str) -> int:

    result: int
    match rdbms:
        case "mysql":
            result = 0
        case "oracle":
            result = 1
        case "postgres":
            result = 2
        case _:  # "sqlserver":
            result = 3

    return result
