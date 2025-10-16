from logging import Logger
from pypomes_core import dict_get_key, str_positional
from pypomes_db import DbEngine
from sqlalchemy.sql.elements import Type
from sqlalchemy.sql.schema import Column
from typing import Any, Final

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
    # MatchType as Ref_MatchType,      # refers to the return type of the MATCH operator
    NullType as Ref_NullType,          # default type for those cases where a type cannot be determined
    Numeric as Ref_Numeric,            # base for non-integer numeric types, such as NUMERIC, FLOAT, DECIMAL
    # PickleType as Ref_PickleType,    # holds Python objects, which are serialized using pickle
    # SchemaType as Ref_SchemaType,    # sdd capabilities to a type which allow for schema-level DDL association
    SmallInteger as Ref_SmallInteger,  # a type for smaller int integers
    String as Ref_String,              # the base for all string and character types
    Text as Ref_Text,                  # a variably sized string type
    Time as Ref_Time,                  # a type for datetime.time() objects
    # Unicode as Ref_Unicode,          # a variable length Unicode string type
    # UnicodeText as Ref_UnicodeText,  # an unbounded-length Unicode string type
    Uuid as Ref_Uuid,                  # represents a database agnostic UUID datatype
)

# This category of types refers to types that are either part of the SQL standard,
# or are potentially found within a subset of database backends.
# Unlike the “generic” types, the SQL standard/multivendor types have no guarantee of working
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
    FLOAT as REF_FLOAT,                        # the SQL FLOAT type
    INT as REF_INT,                            # alias of INTEGER
    INTEGER as REF_INTEGER,                    # the SQL INT or INTEGER type
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
    VARCHAR as REF_VARCHAR,                    # the SQL VARCHAR type
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.mysql import (
    BIGINT as MSQL_BIGINT,
    # BINARY as MSQL_BINARY,  # same as REF_BINARY
    BIT as MSQL_BIT,
    # BLOB as MSQL_BLOB,  # same as REF_BLOB
    # BOOLEAN as MSQL_BOOLEAN,  # same as REF_BOOLEAN
    CHAR as MSQL_CHAR,  # synonym of NCHAR
    # DATE as MSQL_DATE,  # same as REF_DATE
    DATETIME as MSQL_DATETIME,
    DECIMAL as MSQL_DECIMAL,  # synonym of NUMERIC
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
    NCHAR as MSQL_NCHAR,  # synonym of CHAR
    NUMERIC as MSQL_NUMERIC,  # synonym of DECIMAL
    NVARCHAR as MSQL_NVARCHAR,  # synonym of VARCHAR
    REAL as MSQL_REAL,
    SET as MSQL_SET,
    SMALLINT as MSQL_SMALLINT,
    TEXT as MSQL_TEXT,
    TIME as MSQL_TIME,
    TIMESTAMP as MSQL_TIMESTAMP,
    TINYBLOB as MSQL_TINYBLOB,
    TINYINT as MSQL_TINYINT,
    TINYTEXT as MSQL_TINYTEXT,
    # VARBINARY as MSQL_VARBINARY,  # same as REF_VARBINARY
    VARCHAR as MSQL_VARCHAR,  # synonym of NVARCHAR
    YEAR as MSQL_YEAR,
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.oracle import (
    BFILE as ORCL_BFILE,
    BINARY_DOUBLE as ORCL_BINARY_DOUBLE,
    BINARY_FLOAT as ORCL_BINARY_FLOAT,
    # BLOB as ORCL_BLOB,  # same as REF_BLOB
    # CHAR as ORCL_CHAR,  # same as REF_CHAR
    # CLOB as ORCL_CLOB,  # same as REF_CLOB
    DATE as ORCL_DATE,
    # DOUBLE_PRECISION as ORCL_DOUBLE_PRECISION,  # same as REF_DOUBLE_PRECISION
    FLOAT as ORCL_FLOAT,
    INTERVAL as ORCL_INTERVAL,
    LONG as ORCL_LONG,
    # NCHAR as ORCL_NCHAR,  # same as REF_NCHAR
    NCLOB as ORCL_NCLOB,
    NUMBER as ORCL_NUMBER,
    # NVARCHAR as ORCL_NVARCHAR,   # same as REF_NVARCHAR
    # NVARCHAR2 as REF_NVARCHAR,
    RAW as ORCL_RAW,
    # REAL as ORCL_REAL,  # same as REF_REAF
    ROWID as ORCL_ROWID,
    TIMESTAMP as ORCL_TIMESTAMP,
    # VARCHAR as ORCL_VARCHAR,  # same as REF_VARCHAR
    VARCHAR2 as ORCL_VARCHAR2,
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.postgresql import (
    ARRAY as PG_ARRAY,
    # BIGINT as PG_BIGINT,  # same as REF_BIGINT
    BIT as PG_BIT,
    # BOOLEAN as PG_BOOLEAN,  # same as REF_BOOLEAN
    BYTEA as PG_BYTEA,
    # CHAR as PG_CHAR,  # same as REF_CHAR
    CIDR as PG_CIDR,
    CITEXT as PG_CITEXT,
    # DATE as PG_DATE,  # same as REF_DATE
    DATEMULTIRANGE as REF_DATEMULTIRANGE,
    DATERANGE as REF_DATERANGE,
    DOMAIN as PG_DOMAIN,
    # DOUBLE_PRECISION as PG_DOUBLE_PRECISION,  # same as REF_DOUBLE_PRECISION
    ENUM as PG_ENUM,
    # FLOAT as PG_FLOAT,  # same as REF_FLOAT
    HSTORE as PG_HSTORE,
    INET as PG_INET,
    INT4MULTIRANGE as PG_INT4MULTIRANGE,
    INT4RANGE as PG_INT4RANGE,
    INT8MULTIRANGE as PG_INT8MULTIRANGE,
    INT8RANGE as PG_INT8RANGE,
    # INTEGER as PG_INTEGER,  # same as REF_INTEGER
    INTERVAL as PG_INTERVAL,
    JSON as PG_JSON,
    JSONB as PG_JSONB,
    JSONPATH as PG_JSONPATH,
    MACADDR as PG_MACADDR,
    MACADDR8 as PG_MACADDR8,
    MONEY as PG_MONEY,
    # NUMERIC as PG_NUMERIC,  # same as REF_NUMERIC
    NUMMULTIRANGE as PG_NUMMULTIRANGE,
    NUMRANGE as PG_NUMRANGE,
    OID as PG_OID,
    # REAL as PG_REAL,  # same as REF_REAL
    REGCLASS as PG_REGCLASS,
    REGCONFIG as PG_REGCONFIG,
    # SMALLINT as PG_SMALLINT,  # same as REF_SMALLINT
    # TEXT as PG_TEXT,  # same as REF_TEXT
    TIME as PG_TIME,
    TIMESTAMP as PG_TIMESTAMP,
    TSMULTIRANGE as PG_TSMULTIRANGE,
    TSQUERY as PG_TSQUERY,
    TSRANGE as PG_TSRANGE,
    TSTZMULTIRANGE as PG_TSTZMULTIRANGE,
    TSTZRANGE as PG_TSTZRANGE,
    TSVECTOR as PG_TSVECTOR,
    # UUID as PG_UUID,  # same as REF_UUID
    # VARCHAR as PG_VARCHAR,  # same as REF_VARCHAR
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.mssql import (
    # BIGINT as SQLS_BIGINT,  # same as REF_BIGINT
    # BINARY as SQLS_BINARY,  # same as REF_BINARY
    BIT as SQLS_BIT,
    # CHAR as SQLS_CHAR,  # same as REF_CHAR
    # DATE as SQLS_DATE,  # same as REF_DATE
    # DATETIME as SQLS_DATETIME,  # same as REF_DATETIME
    DATETIME2 as SQLS_DATETIME2,
    DATETIMEOFFSET as REF_DATETIMEOFFSET,
    # DECIMAL as SQLS_DECIMAL,  # same as REF_DECIMAL, synonym of NUMERIC
    DOUBLE_PRECISION as SQLS_DOUBLE_PRECISION,
    # FLOAT as SQLS_FLOAT,  # same as REF_FLOAT, synonym of SQLS_REAL
    IMAGE as SQLS_IMAGE,
    # INTEGER as SQLS_INTEGER,  # same as REF_INTEGER
    JSON as SQLS_JSON,
    MONEY as SQLS_MONEY,
    # NCHAR as SQLS_NCHAR,  # same as REF_NCHAR
    NTEXT as SQLS_NTEXT,
    # NUMERIC as SQLS_NUMERIC,  # same as REF_NUMERIC, synonym of SQLS_DECIMAL
    # NVARCHAR as SQLS_NVARCHAR,  # same as REF_NVARCHAR
    REAL as SQLS_REAL,  # synonym of FLOAT
    ROWVERSION as SQLS_ROWVERSION,
    SMALLDATETIME as SQLS_SMALLDATETIME,
    # SMALLINT as SQLS_SMALLINT,  # same as REF_SMALLINT
    SMALLMONEY as SQLS_SMALLMONEY,
    SQL_VARIANT as SQLS_SQL_VARIANT,
    # TEXT as SQLS_TEXT,  # same as REF_TEXT
    TIME as SQLS_TIME,
    TIMESTAMP as SQLS_TIMESTAMP,
    TINYINT as SQLS_TINYINT,
    UNIQUEIDENTIFIER as SQLS_UNIQUEIDENTIFIER,
    VARBINARY as SQLS_VARBINARY,
    # VARCHAR as SQLS_VARCHAR,  # same as REF_VARCHAR
    XML as SQLS_XML,
)

COLUMN_TYPES: dict[str, Type] = {
    # SQL types
    "ref_array": REF_ARRAY,
    "ref_bigint": REF_BIGINT,
    "ref_binary": REF_BINARY,
    "ref_blob": REF_BLOB,
    "ref_boolean": REF_BOOLEAN,
    "ref_char": REF_CHAR,
    "ref_clob": REF_CLOB,
    "ref_date": REF_DATE,
    "ref_datetime": REF_DATETIME,
    "ref_decimal": REF_DECIMAL,
    "ref_double": REF_DOUBLE,
    "ref_double_precision": REF_DOUBLE_PRECISION,
    "ref_float": REF_FLOAT,
    "ref_int": REF_INT,
    "ref_integer": REF_INTEGER,
    "ref_json": REF_JSON,
    "ref_nchar": REF_NCHAR,
    "ref_numeric": REF_NUMERIC,
    "ref_nvarchar": REF_NVARCHAR,
    "ref_real": REF_REAL,
    "ref_smallint": REF_SMALLINT,
    "ref_text": REF_TEXT,
    "ref_time": REF_TIME,
    "ref_timestamp": REF_TIMESTAMP,
    "ref_uuid": REF_UUID,
    "ref_varbinary": REF_VARBINARY,
    "ref_varchar": REF_VARCHAR,

    # MySQL types
    "msql_bigint": MSQL_BIGINT,
    "msql_binary": REF_BINARY,
    "msql_bit": MSQL_BIT,
    "msql_blob": REF_BLOB,
    "msql_boolean": REF_BOOLEAN,
    "msql_char": MSQL_CHAR,
    "msql_date": REF_DATE,
    "msql_datetime": MSQL_DATETIME,
    "msql_decimal": MSQL_DECIMAL,
    "msql_double": MSQL_DOUBLE,
    "msql_enum": MSQL_ENUM,
    "msql_float": MSQL_FLOAT,
    "msql_integer": MSQL_INTEGER,
    "msql_json": MSQL_JSON,
    "msql_longblob": MSQL_LONGBLOB,
    "msql_longtext": MSQL_LONGTEXT,
    "msql_mediumblob": MSQL_MEDIUMBLOB,
    "msql_mediumint": MSQL_MEDIUMINT,
    "msql_mediumtext": MSQL_MEDIUMTEXT,
    "msql_nchar": MSQL_NCHAR,
    "msql_numeric": MSQL_NUMERIC,
    "msql_nvarchar": MSQL_NVARCHAR,
    "msql_real": MSQL_REAL,
    "msql_set": MSQL_SET,
    "msql_smallint": MSQL_SMALLINT,
    "msql_text": MSQL_TEXT,
    "msql_time": MSQL_TIME,
    "msql_timestamp": MSQL_TIMESTAMP,
    "msql_tinyblob": MSQL_TINYBLOB,
    "msql_tinyint": MSQL_TINYINT,
    "msql_tinytext": MSQL_TINYTEXT,
    "msql_varbinary": REF_VARBINARY,
    "msql_varchar": MSQL_VARCHAR,
    "msql_year": MSQL_YEAR,

    # Oracle types
    "orcl_bfile": ORCL_BFILE,
    "orcl_binary_double": ORCL_BINARY_DOUBLE,
    "orcl_binary_float": ORCL_BINARY_FLOAT,
    "orcl_blob": REF_BLOB,
    "orcl_char": REF_CHAR,
    "orcl_clob": REF_CLOB,
    "orcl_date": ORCL_DATE,
    "orcl_double_precision": REF_DOUBLE_PRECISION,
    "orcl_float": ORCL_FLOAT,
    "orcl_interval": ORCL_INTERVAL,
    "orcl_long": ORCL_LONG,
    "orcl_nchar": REF_NCHAR,
    "orcl_nclob": ORCL_NCLOB,
    "orcl_number": ORCL_NUMBER,
    "orcl_nvarchar": REF_NVARCHAR,
    "orcl_nvarchar2": REF_NVARCHAR,
    "orcl_raw": ORCL_RAW,
    "orcl_real": REF_REAL,
    "orcl_rowid": ORCL_ROWID,
    "orcl_timestamp": ORCL_TIMESTAMP,
    "orcl_varchar": REF_VARCHAR,
    "orcl_varchar2": ORCL_VARCHAR2,

    # Postgres types
    "pg_array": PG_ARRAY,
    "pg_bigint": REF_BIGINT,
    "pg_bit": PG_BIT,
    "pg_boolean": REF_BOOLEAN,
    "pg_bytea": PG_BYTEA,
    "pg_char": REF_CHAR,
    "pg_cidr": PG_CIDR,
    "pg_citext": PG_CITEXT,
    "pg_date": REF_DATE,
    "pg_datemultirange": REF_DATEMULTIRANGE,
    "pg_daterange": REF_DATERANGE,
    "pg_domain": PG_DOMAIN,
    "pg_double_precision": REF_DOUBLE_PRECISION,
    "pg_enum": PG_ENUM,
    "pg_float": REF_FLOAT,
    "pg_hstore": PG_HSTORE,
    "pg_inet": PG_INET,
    "pg_int4multirange": PG_INT4MULTIRANGE,
    "pg_int4range": PG_INT4RANGE,
    "pg_int8multirange": PG_INT8MULTIRANGE,
    "pg_int8range": PG_INT8RANGE,
    "pg_integer": REF_INTEGER,
    "pg_interval": PG_INTERVAL,
    "pg_json": PG_JSON,
    "pg_jsonb": PG_JSONB,
    "pg_jsonpath": PG_JSONPATH,
    "pg_macaddr": PG_MACADDR,
    "pg_macaddr8": PG_MACADDR8,
    "pg_money": PG_MONEY,
    "pg_numeric": REF_NUMERIC,
    "pg_nummultirange": PG_NUMMULTIRANGE,
    "pg_numrange": PG_NUMRANGE,
    "pg_oid": PG_OID,
    "pg_real": REF_REAL,
    "pg_regclass": PG_REGCLASS,
    "pg_regconfig": PG_REGCONFIG,
    "pg_smallint": REF_SMALLINT,
    "pg_text": REF_TEXT,
    "pg_time": PG_TIME,
    "pg_timestamp": PG_TIMESTAMP,
    "pg_tsmultirange": PG_TSMULTIRANGE,
    "pg_tsquery": PG_TSQUERY,
    "pg_tsrange": PG_TSRANGE,
    "pg_tstzmultirange": PG_TSTZMULTIRANGE,
    "pg_tstzrange": PG_TSTZRANGE,
    "pg_tsvector": PG_TSVECTOR,
    "pg_uuid": REF_UUID,
    "pg_varchar": REF_VARCHAR,

    # SQLServer types
    "sqls_bigint": REF_BIGINT,
    "sqls_binary": REF_BINARY,
    "sqls_bit": SQLS_BIT,
    "sqls_char": REF_CHAR,
    "sqls_date": REF_DATE,
    "sqls_datetime": REF_DATETIME,
    "sqls_datetime2": SQLS_DATETIME2,
    "sqls_datetimeoffset": REF_DATETIMEOFFSET,
    "sqls_decimal": REF_DECIMAL,
    "sqls_double_precision": SQLS_DOUBLE_PRECISION,
    "sqls_float": REF_FLOAT,
    "sqls_image": SQLS_IMAGE,
    "sqls_integer": REF_INTEGER,
    "sqls_json": SQLS_JSON,
    "sqls_money": SQLS_MONEY,
    "sqls_nchar": REF_NCHAR,
    "sqls_ntext": SQLS_NTEXT,
    "sqls_numeric": REF_NUMERIC,
    "sqls_nvarchar": REF_NVARCHAR,
    "sqls_real": SQLS_REAL,
    "sqls_rowversion": SQLS_ROWVERSION,
    "sqls_smalldatetime": SQLS_SMALLDATETIME,
    "sqls_smallint": REF_SMALLINT,
    "sqls_smallmoney": SQLS_SMALLMONEY,
    "sqls_sql_variant": SQLS_SQL_VARIANT,
    "sqls_text": REF_TEXT,
    "sqls_time": SQLS_TIME,
    "sqls_timestamp": SQLS_TIMESTAMP,
    "sqls_tinyint": SQLS_TINYINT,
    "sqls_uniqueidentifier": SQLS_UNIQUEIDENTIFIER,
    "sqls_varbinary": SQLS_VARBINARY,
    "sqls_varchar": REF_VARCHAR,
    "sqls_xml": SQLS_XML,
}

# Reference - MySQL - Oracle - PostgreSQL - SQLServer
# (upper-case are stricter equivalences, and must be listed first)
REF_EQUIVALENCES: Final[list[tuple]] = [
    (REF_BIGINT, MSQL_BIGINT, ORCL_NUMBER, REF_BIGINT, REF_BIGINT),
    (REF_BINARY, REF_BINARY, ORCL_RAW, PG_BYTEA, REF_BINARY),
    (REF_BLOB, MSQL_LONGBLOB, REF_CLOB, PG_BYTEA, SQLS_VARBINARY),
    (REF_BOOLEAN, REF_BOOLEAN, None, REF_BOOLEAN, None),
    (REF_CHAR, MSQL_CHAR, REF_CHAR, REF_CHAR, REF_CHAR),
    (REF_CLOB, MSQL_TEXT, REF_CLOB, REF_TEXT, None),
    (REF_DATE, REF_DATE, ORCL_DATE, REF_DATE, REF_DATE),
    (REF_DATETIME, REF_DATETIME, ORCL_DATE, PG_TIMESTAMP, REF_DATETIME),
    (REF_DECIMAL, MSQL_DECIMAL, ORCL_NUMBER, REF_NUMERIC, REF_NUMERIC),
    (REF_DOUBLE, MSQL_DOUBLE, ORCL_BINARY_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (REF_DOUBLE_PRECISION, MSQL_DOUBLE, ORCL_BINARY_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (REF_FLOAT, MSQL_FLOAT, ORCL_FLOAT, REF_FLOAT, REF_FLOAT),
    (REF_INT, MSQL_INTEGER, ORCL_NUMBER, REF_INTEGER, REF_INTEGER),
    (REF_INTEGER, MSQL_INTEGER, ORCL_NUMBER, REF_INTEGER, REF_INTEGER),
    (REF_JSON, MSQL_JSON, None, PG_JSON, SQLS_JSON),
    (REF_NCHAR, MSQL_NCHAR, REF_NCHAR, REF_CHAR, REF_NCHAR),
    (REF_NUMERIC, MSQL_DECIMAL, ORCL_NUMBER, REF_NUMERIC, REF_NUMERIC),
    (REF_NVARCHAR, MSQL_NVARCHAR, REF_NVARCHAR, REF_VARCHAR, REF_NVARCHAR),
    (REF_REAL, MSQL_FLOAT, ORCL_FLOAT, REF_REAL, SQLS_REAL),
    (REF_SMALLINT, MSQL_SMALLINT, ORCL_NUMBER, REF_SMALLINT, REF_SMALLINT),
    (REF_TEXT, MSQL_TEXT, ORCL_VARCHAR2, REF_TEXT, REF_TEXT),
    (REF_TIME, MSQL_TIME, ORCL_TIMESTAMP, PG_TIME, SQLS_TIME),
    (REF_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP, REF_DATETIME),
    (REF_UUID, MSQL_VARCHAR, ORCL_VARCHAR2, REF_UUID, SQLS_UNIQUEIDENTIFIER),
    (REF_VARBINARY, REF_VARBINARY, REF_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (REF_VARCHAR, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_BigInteger, MSQL_BIGINT, ORCL_NUMBER, REF_BIGINT, REF_BIGINT),
    (Ref_Boolean, REF_BOOLEAN, None, REF_BOOLEAN, None),
    (Ref_Date, REF_DATE, ORCL_DATE, REF_DATE, REF_DATE),
    (Ref_DateTime, REF_DATETIME, ORCL_DATE, PG_TIMESTAMP, REF_DATETIME),
    (Ref_Double, MSQL_DOUBLE, ORCL_BINARY_DOUBLE, REF_DOUBLE_PRECISION, SQLS_DOUBLE_PRECISION),
    (Ref_Enum, MSQL_ENUM, None, PG_ENUM, None),
    (Ref_Float, MSQL_FLOAT, ORCL_BINARY_FLOAT, REF_FLOAT),
    (Ref_Integer, MSQL_INTEGER, ORCL_NUMBER, REF_INTEGER, REF_INTEGER),
    (Ref_Interval, None, ORCL_INTERVAL, PG_INTERVAL, None),
    (Ref_LargeBinary, MSQL_LONGBLOB, REF_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (Ref_NullType, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_Numeric, MSQL_NUMERIC, ORCL_NUMBER, REF_NUMERIC, REF_NUMERIC),
    (Ref_SmallInteger, MSQL_SMALLINT, ORCL_NUMBER, REF_SMALLINT, REF_INTEGER),
    (Ref_String, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_Text, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_Time, MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP, SQLS_TIMESTAMP),
    (Ref_Uuid, MSQL_VARCHAR, ORCL_VARCHAR2, REF_UUID, SQLS_UNIQUEIDENTIFIER),
]

# MySQL - Oracle - PostgreSQL - SLServer (TO BE COMPLETED)
MSQL_EQUIVALENCES: Final[list[tuple]] = [
    (MSQL_CHAR, REF_CHAR, REF_CHAR, REF_CHAR),
    (MSQL_NUMERIC, ORCL_NUMBER, REF_NUMERIC, REF_NUMERIC),
    (MSQL_NVARCHAR, REF_NVARCHAR, REF_VARCHAR, REF_NVARCHAR),
    (MSQL_FLOAT, ORCL_FLOAT, REF_FLOAT, REF_FLOAT),
    (MSQL_LONGTEXT, ORCL_LONG, REF_TEXT, REF_TEXT),
    (MSQL_LONGBLOB, REF_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (MSQL_MEDIUMBLOB, REF_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (MSQL_TEXT, REF_CLOB, REF_TEXT, SQLS_VARBINARY),
    (MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP, SQLS_TIMESTAMP),
    (MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
]

# Oracle - MySQL - PostgreSQL - SQLServer
ORCL_EQUIVALENCES: Final[list[tuple]] = [
    (ORCL_BFILE, MSQL_LONGBLOB, PG_BYTEA, SQLS_VARBINARY),
    (ORCL_BINARY_DOUBLE, MSQL_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (ORCL_BINARY_FLOAT, MSQL_FLOAT, REF_REAL, REF_FLOAT),
    (ORCL_DATE, REF_DATETIME, PG_TIMESTAMP, REF_DATETIME),
    (ORCL_FLOAT, MSQL_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (ORCL_INTERVAL, None, PG_INTERVAL, None),
    (ORCL_LONG, MSQL_LONGTEXT, REF_TEXT, REF_TEXT),
    (ORCL_NCLOB, MSQL_LONGTEXT, REF_TEXT, SQLS_NTEXT),
    (ORCL_NUMBER, MSQL_NUMERIC, REF_NUMERIC, REF_NUMERIC),
    (ORCL_RAW, REF_VARBINARY, PG_BYTEA, SQLS_VARBINARY),
    (ORCL_ROWID, MSQL_VARCHAR, REF_VARCHAR, REF_VARCHAR),
    (ORCL_TIMESTAMP, REF_DATETIME, PG_TIMESTAMP, REF_DATETIME),
    (ORCL_VARCHAR2, MSQL_VARCHAR, REF_VARCHAR, REF_VARCHAR),
    # SQLAlchemy reports Oracle's NUMBER(38,0) as REF_INTEGER
    (REF_INTEGER, REF_BIGINT, REF_BIGINT, MSQL_BIGINT),
]

# PostgreSQL - MySQL - Oracle - SQLServer (TO BE COMPLETED)
PG_EQUIVALENCES: Final[list[tuple]] = [
    (PG_BYTEA, MSQL_LONGBLOB, REF_BLOB, SQLS_VARBINARY),
    (PG_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, SQLS_TIMESTAMP),
]

# SQLServer - MySQL - Oracle - PostgreSQL (TO BE COMPLETED)
SQLS_EQUIVALENCES: Final[list[tuple]] = [
    (REF_DATETIME, REF_DATETIME, ORCL_DATE, PG_TIMESTAMP),
    (SQLS_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP),
    (SQLS_VARBINARY, MSQL_LONGBLOB, REF_BLOB, PG_BYTEA),
]

LOBS: Final[list[str]] = [
    str(MSQL_LONGBLOB()),
    str(MSQL_LONGTEXT()),
    str(MSQL_MEDIUMBLOB()),
    str(MSQL_TEXT()),
    str(MSQL_TINYBLOB()),
    str(ORCL_BFILE()),
    str(ORCL_LONG()),
    str(ORCL_NCLOB()),
    str(ORCL_RAW()),
    str(PG_BYTEA()),
    str(REF_BLOB()),
    str(REF_CLOB()),
    str(REF_TEXT()),
    str(REF_VARBINARY()),
    str(SQLS_IMAGE()),
    str(SQLS_VARBINARY()),
]


def migrate_column(source_rdbms: DbEngine,
                   target_rdbms: DbEngine,
                   ref_column: Column,
                   override_columns: dict[str, Type],
                   logger: Logger) -> Any:

    # declare the return variable
    result: Any

    # obtain needed characteristics
    col_type_class: Type = ref_column.type.__class__
    col_type_obj: Any = ref_column.type
    col_name: str = f"{ref_column.table.name}.{ref_column.name}"

    is_pk: bool = (hasattr(ref_column, "primary_key") and
                   ref_column.primary_key) or False
    is_fk: bool = (hasattr(ref_column, "foreign_keys") and
                   isinstance(ref_column.foreign_keys, set) and
                   len(ref_column.foreign_keys) > 0)
    is_identity: bool = (hasattr(ref_column, "identity") and
                         ref_column.identity) or False
    is_lob: bool = str(col_type_obj) in LOBS
    is_number: bool = (col_type_class in
                       [REF_NUMERIC, ORCL_NUMBER, MSQL_DECIMAL, MSQL_NUMERIC])
    is_number_int: bool = (is_number and
                           hasattr(col_type_obj, "asdecimal") and
                           not col_type_obj.asdecimal)
    col_precision: int = (col_type_obj.precision
                          if is_number and hasattr(col_type_obj, "precision") else None)
    msg: str = f"Rdbms {target_rdbms}, type {col_type_obj} in {source_rdbms}.{col_name}"

    # PostgreSQL does not accept value '0' in 'CACHE' clause, at table creation time
    # (cannot just remove the attribute, as SQLAlchemy requires it to exist in identity columns)
    if target_rdbms == DbEngine.POSTGRES and is_identity and \
       hasattr(ref_column.identity, "cache") and \
       ref_column.identity.cache == 0:
        ref_column.identity.cache = 1

    # if provided, the override type has precedence
    type_equiv: Type = override_columns.get(col_name)
    if type_equiv is None:
        # establish the migration equivalences
        (native_ordinal, reference_ordinal, nat_equivalences) = \
            establish_equivalences(source_rdbms=source_rdbms,
                                   target_rdbms=target_rdbms)

        # inspect the native equivalences first
        for nat_equivalence in nat_equivalences:
            if isinstance(col_type_obj, nat_equivalence[0]):
                type_equiv = nat_equivalence[native_ordinal]
                break

        # inspect the reference equivalences next
        if type_equiv is None:
            for ref_equivalence in REF_EQUIVALENCES:
                if isinstance(col_type_obj, ref_equivalence[0]):
                    type_equiv = ref_equivalence[reference_ordinal]
                    break

        if is_fk:
            # the column a foreign key, force type conformity
            fk_column: Column = next(iter(ref_column.foreign_keys)).column
            fk_type: Any = migrate_column(source_rdbms=source_rdbms,
                                          target_rdbms=target_rdbms,
                                          ref_column=fk_column,
                                          override_columns=override_columns,
                                          logger=logger)
            type_equiv = fk_type.__class__

        if type_equiv is None:
            logger.warning(msg=f"{msg} - unable to convert, using the source type")
            # use the source type
            type_equiv = col_type_class

        # fine-tune the type equivalence
        if is_number_int:
            if is_identity:
                if hasattr(ref_column.identity, "maxvalue"):
                    if ref_column.identity.maxvalue <= 2147483647:  # max value for REF_INTEGER
                        type_equiv = REF_INTEGER
                    elif ref_column.identity.maxvalue > 9223372036854775807:  # max value for REF_BIGINT
                        if target_rdbms == DbEngine.ORACLE:
                            type_equiv = ORCL_NUMBER
                        else:
                            # potential problem, as most DBs restrict REF_BIGINT to 8 bytes
                            type_equiv = REF_BIGINT
                elif not col_precision or col_precision > 9:
                    type_equiv = REF_BIGINT
                else:
                    type_equiv = REF_INTEGER
            elif is_pk and type_equiv == REF_NUMERIC:
                # optimize primary keys
                if not col_precision or 19 < col_precision > 9:
                    type_equiv = REF_BIGINT
                elif col_precision < 10:
                    type_equiv = REF_INTEGER

    # instantiate the type object
    result = type_equiv()
    logger.debug(msg=f"{msg} converted to {result}")

    # wrap-up the type migration
    if hasattr(col_type_obj, "nullable") and hasattr(result, "nullable"):
        result.nullable = True if is_lob else col_type_obj.nullable
    if hasattr(col_type_obj, "length") and hasattr(result, "length"):
        result.length = col_type_obj.length
    if hasattr(col_type_obj, "asdecimal") and hasattr(result, "asdecimal"):
        result.asdecimal = col_type_obj.asdecimal
    if hasattr(col_type_obj, "precision") and hasattr(result, "precision"):
        result.precision = col_type_obj.precision
    if hasattr(col_type_obj, "scale") and hasattr(result, "scale"):
        result.scale = col_type_obj.scale
    if hasattr(col_type_obj, "timezone") and hasattr(result, "timezone"):
        result.timezone = col_type_obj.timezone

    return result


def establish_equivalences(source_rdbms: DbEngine,
                           target_rdbms: DbEngine) -> tuple[int, int, list[tuple]]:

    # make 'nat_equivalences' point to the appropriate list
    nat_equivalences: list[tuple] | None = None
    match source_rdbms:
        case DbEngine.MYSQL:
            nat_equivalences = MSQL_EQUIVALENCES
        case DbEngine.ORACLE:
            nat_equivalences = ORCL_EQUIVALENCES
        case DbEngine.POSTGRES:
            nat_equivalences = PG_EQUIVALENCES
        case DbEngine.SQLSERVER:
            nat_equivalences = SQLS_EQUIVALENCES

    # establish the ordinals
    nat_ordinal: int | None = None
    ref_ordinal: int | None = None
    match target_rdbms:
        case DbEngine.MYSQL:
            ref_ordinal = 1
            match source_rdbms:
                case DbEngine.ORACLE:
                    nat_ordinal = 1
                case DbEngine.POSTGRES:
                    nat_ordinal = 2
                case DbEngine.SQLSERVER:
                    nat_ordinal = 3
        case DbEngine.ORACLE:
            ref_ordinal = 2
            match source_rdbms:
                case DbEngine.MYSQL:
                    nat_ordinal = 1
                case DbEngine.POSTGRES:
                    nat_ordinal = 2
                case DbEngine.SQLSERVER:
                    nat_ordinal = 3
        case DbEngine.POSTGRES:
            ref_ordinal = 3
            match source_rdbms:
                case DbEngine.MYSQL:
                    nat_ordinal = 1
                case DbEngine.ORACLE:
                    nat_ordinal = 2
                case DbEngine.SQLSERVER:
                    nat_ordinal = 3
        case DbEngine.SQLSERVER:
            ref_ordinal = 4
            match source_rdbms:
                case DbEngine.MYSQL:
                    nat_ordinal = 1
                case DbEngine.ORACLE:
                    nat_ordinal = 2
                case DbEngine.POSTGRES:
                    nat_ordinal = 3

    return nat_ordinal, ref_ordinal, nat_equivalences


def name_to_type(rdbms: DbEngine,
                 type_name: str) -> Type | None:

    prefix: str = str_positional(source=rdbms,
                                 list_from=tuple(map(str, DbEngine)),
                                 list_to=("msql", "orcl", "pg", "sqls")) + "_"
    return COLUMN_TYPES.get(f"{type_name}") if type_name.startswith(prefix) else None


def type_to_name(rdbms: DbEngine,
                 col_type: Type) -> str:

    prefix: str = str_positional(source=rdbms,
                                 list_from=tuple(map(str, DbEngine)),
                                 list_to=("msql", "orcl", "pg", "sqls")) + "_"

    type_name: str = dict_get_key(source=COLUMN_TYPES,
                                  value=col_type)
    if type_name.startswith(prefix):
        result: str = type_name
    else:
        result: str = prefix + type_name[len(prefix)+1:]
    return result


def is_lob_column(col_type: str) -> bool:

    return col_type in LOBS
