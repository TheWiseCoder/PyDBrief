import copy
from logging import DEBUG, WARNING, Logger
from sqlalchemy.sql.schema import Column
from typing import Any, Final

from . import pydb_common

# Generic types specify a column that can read, write and store a particular type of Python data.
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
    Uuid as Ref_Uuid                   # represents a database agnostic UUID datatype
)

# This category of types refers to types that are either part of the SQL standard,
# or are potentially found within a subset of database backends.
# Unlike the “generic” types, the SQL standard/multi-vendor types have no guarantee of working
# on all backends, and will only work on those backends that explicitly support them by name.
from sqlalchemy.types import (
    # ARRAY as REF_ARRAY,                      # represents a SQL Array type
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
    VARCHAR as REF_VARCHAR                     # the SQL VARCHAR type
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.mysql import (
    BIGINT as MSQL_BIGINT,
    # BINARY as MSQL_BINARY,  # same as REF_BINARY
    BIT as MSQL_BIT,
    # BLOB as MSQL_BLOB  # same as REF_BLOB
    # BOOLEAN as MSQL_BOOLEAN,  @ same as REF_BOOLEAN
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
    YEAR as MSQL_YEAR
)

from sqlalchemy.dialects.oracle import (
    BFILE as ORCL_BFILE,
    BINARY_DOUBLE as ORCL_BINARY_DOUBLE,
    BINARY_FLOAT as ORCL_BINARY_FLOAT,
    # BLOB as ORCL_BLOB,  # same as REF_BLOB
    # CHAR as ORCL_CHAR,  # same as REF_CHAR
    # CLOB as ORCL_CLOB,  # same as REF_BLOB
    DATE as ORCL_DATE,
    # DOUBLE_PRECISION as ORCL_DOUBLE_PRECISION,  # same as REF_DOUBLE_PRECISION
    FLOAT as ORCL_FLOAT,
    INTERVAL as ORCL_INTERVAL,
    LONG as ORCL_LONG,
    # NCHAR as ORCL_NCHAR,
    NCLOB as ORCL_NCLOB,
    NUMBER as ORCL_NUMBER,
    # NVARCHAR as ORCL_NVARCHAR,   # same as REF_NVARCHAR
    # NVARCHAR2 as REF_NVARCHAR2,  # same as REF_NVARCHAR
    RAW as ORCL_RAW,
    # REAL as ORCL_REAL,  # same as REF_REAF
    # ROWID as ORCL_ROWID,
    TIMESTAMP as ORCL_TIMESTAMP,
    # VARCHAR as ORCL_VARCHAR,  # same as REF_VARCHAR
    VARCHAR2 as ORCL_VARCHAR2
)

# noinspection PyUnresolvedReferences
from sqlalchemy.dialects.postgresql import (
    # ARRAY as PG_ARRAY,
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
    # VARCHAR as PG_VARCHAR  # same as REF_VARCHAR
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
    DECIMAL as SQLS_DECIMAL,  # synonym of NUMERIC
    DOUBLE_PRECISION as SQLS_DOUBLE_PRECISION,
    # FLOAT as SQLS_FLOAT,  # saem as REF_FLOAT, synonym of SQLS_REAL
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
    XML as SQLS_XML
)

# to be filled at migration time
nat_equivalences: list[tuple] = []

# Reference - MySQL - Oracle - PostgreSQL - SQLServer
REF_EQUIVALENCES: Final[list[tuple]] = [
    (Ref_BigInteger, MSQL_BIGINT, ORCL_NUMBER, REF_BIGINT, REF_BIGINT),
    (Ref_Boolean, REF_BOOLEAN, None, REF_BOOLEAN, None),
    (Ref_Date, REF_DATE, ORCL_DATE, REF_DATE, REF_DATE),
    (Ref_DateTime, REF_DATETIME, ORCL_DATE, PG_TIMESTAMP, REF_DATETIME),
    (Ref_Double, MSQL_DOUBLE, ORCL_BINARY_DOUBLE, REF_DOUBLE_PRECISION, SQLS_DOUBLE_PRECISION),
    (Ref_Enum, MSQL_ENUM, None, PG_ENUM, None),
    (Ref_Float, MSQL_FLOAT, ORCL_BINARY_FLOAT, REF_FLOAT),
    (Ref_Integer, ORCL_NUMBER, MSQL_INTEGER, REF_INTEGER, REF_INTEGER),
    (Ref_Interval, None, ORCL_INTERVAL, PG_INTERVAL, None),
    (Ref_LargeBinary, MSQL_LONGBLOB, REF_BLOB, PG_BYTEA, SQLS_VARBINARY),
    (Ref_NullType, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_Numeric, MSQL_NUMERIC, ORCL_NUMBER, REF_NUMERIC, REF_NUMERIC),
    (Ref_SmallInteger, MSQL_SMALLINT, ORCL_NUMBER, REF_SMALLINT, REF_INTEGER),
    (Ref_String, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_Text, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR),
    (Ref_Time, MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP, SQLS_TIMESTAMP),
    (Ref_Uuid, MSQL_VARCHAR, ORCL_VARCHAR2, REF_UUID, SQLS_UNIQUEIDENTIFIER),
    (REF_BIGINT, MSQL_BIGINT, ORCL_NUMBER, REF_BIGINT, REF_BIGINT),
    (REF_BINARY, REF_BINARY, ORCL_RAW, PG_BYTEA, REF_BINARY),
    (REF_BLOB, MSQL_LONGBLOB, REF_CLOB, PG_BYTEA, SQLS_VARBINARY),
    (REF_BOOLEAN, REF_BOOLEAN, None, REF_BOOLEAN, None),
    (REF_CHAR, MSQL_CHAR, REF_CHAR, REF_CHAR, REF_CHAR),
    (REF_CLOB, MSQL_TEXT, REF_CLOB, REF_TEXT, None),
    (REF_DATE, REF_DATE, ORCL_DATE, REF_DATE, REF_DATE),
    (REF_DATETIME, REF_DATETIME, ORCL_DATE, PG_TIMESTAMP, REF_DATETIME),
    (REF_DECIMAL, MSQL_DECIMAL, ORCL_NUMBER, REF_NUMERIC, SQLS_DECIMAL),
    (REF_DOUBLE, MSQL_DOUBLE, ORCL_BINARY_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (REF_DOUBLE_PRECISION, MSQL_DOUBLE, ORCL_BINARY_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (REF_FLOAT, MSQL_FLOAT, ORCL_FLOAT, REF_FLOAT, REF_FLOAT),
    (REF_INT, ORCL_NUMBER, MSQL_INTEGER, REF_INTEGER, REF_INTEGER),
    (REF_INTEGER, ORCL_NUMBER, MSQL_INTEGER, REF_INTEGER, REF_INTEGER),
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
    (REF_VARCHAR, MSQL_VARCHAR, ORCL_VARCHAR2, REF_VARCHAR, REF_VARCHAR)
]

# MySQL - Oracle - PostgreSQL - SLServer
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
    (ORCL_BFILE, MSQL_VARCHAR, REF_VARCHAR, REF_VARCHAR),
    (ORCL_BINARY_DOUBLE, MSQL_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (ORCL_BINARY_FLOAT, MSQL_FLOAT, REF_REAL, REF_FLOAT),
    (ORCL_DATE, REF_DATETIME, PG_TIMESTAMP, REF_DATETIME),
    (ORCL_FLOAT, MSQL_DOUBLE, REF_DOUBLE_PRECISION, REF_FLOAT),
    (ORCL_INTERVAL, None, PG_INTERVAL, None),
    (ORCL_LONG, MSQL_LONGTEXT, REF_TEXT, REF_TEXT),
    (ORCL_NCLOB, MSQL_LONGTEXT, REF_TEXT, SQLS_NTEXT),
    (ORCL_NUMBER, MSQL_NUMERIC, REF_NUMERIC, REF_NUMERIC),
    (ORCL_RAW, REF_VARBINARY, PG_BYTEA, SQLS_VARBINARY),
    (ORCL_TIMESTAMP, REF_DATETIME, PG_TIMESTAMP, REF_DATETIME),
    (ORCL_VARCHAR2, MSQL_VARCHAR, REF_VARCHAR, REF_VARCHAR)
]

# PostgreSQL - MySQL - Oracle - SQLServer
PG_EQUIVALENCES: Final[list[tuple]] = [
    (PG_BYTEA, MSQL_LONGBLOB, REF_BLOB, SQLS_VARBINARY),
    (PG_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, SQLS_TIMESTAMP),
]

# SQLServer - MySQL - Oracle - PostgreSQL
SQLS_EQUIVALENCES: Final[list[tuple]] = [
    (REF_DATETIME, REF_DATETIME, ORCL_DATE, PG_TIMESTAMP),
    (SQLS_TIMESTAMP, MSQL_TIMESTAMP, ORCL_TIMESTAMP, PG_TIMESTAMP),
    (SQLS_VARBINARY, MSQL_LONGBLOB, REF_BLOB, PG_BYTEA),
]


def migrate_type(source_rdbms: str,
                 target_rdbms: str,
                 native_ordinal: int,
                 reference_ordinal: int,
                 source_column: Column,
                 logger: Logger) -> Any:

    # declare the return variable
    result: Any

    type_equiv: Any = None
    col_type: Any = source_column.type
    col_name: str = f"{source_column.table.name}.{source_column.name}"

    # inspect the native equivalences, first
    for nat_equivalence in nat_equivalences:
        if isinstance(col_type, nat_equivalence[0]):
            type_equiv = nat_equivalence[native_ordinal]
            break

    # inspect the reference equivalences, if necessary
    if type_equiv is None:
        for ref_equivalence in REF_EQUIVALENCES:
            if isinstance(col_type, ref_equivalence[0]):
                type_equiv = ref_equivalence[reference_ordinal]
                break

    # wrap-up the migration
    msg: str = f"From {source_rdbms} to {target_rdbms}, type {str(col_type)} in column {col_name}"
    if type_equiv is None:
        pydb_common.log(logger, WARNING,
                        f"{msg}  - unable to convert")
        result = copy.copy(col_type)
    else:
        if type_equiv == REF_NUMERIC and \
           hasattr(col_type, "asdecimal") and not col_type.asdecimal:
            if hasattr(source_column, "identity") and \
               hasattr(source_column.identity, "maxvalue"):
                if source_column.identity.maxvalue > 9223372036854775807:  # max value for REF_BIGINT
                    source_column.identity.maxvalue = 9223372036854775807
                if source_column.identity.maxvalue > 2147483647:  # max value for REF_INTEGER
                    type_equiv = REF_BIGINT
                else:
                    type_equiv = REF_INTEGER
            else:
                type_equiv = REF_INTEGER
        result = type_equiv()
        if hasattr(col_type, "length") and hasattr(result, "length"):
            result.length = col_type.length
        if hasattr(col_type, "asdecimal") and hasattr(result, "asdecimal"):
            result.asdecimal = col_type.asdecimal
        if hasattr(col_type, "precision") and hasattr(result, "precision"):
            result.precision = col_type.precision
        if hasattr(col_type, "scale") and hasattr(result, "scale"):
            result.scale = col_type.scale
        pydb_common.log(logger, DEBUG,
                        f"{msg} converted to type {str(result)}")

    return result


def establish_equivalences(source_rdbms: str,
                           target_rdbms: str) -> tuple[int, int]:

    # make 'nat_equivalences' point to the appropriate list
    global nat_equivalences
    match source_rdbms:
        case "mysql":
            nat_equivalences = MSQL_EQUIVALENCES
        case "oracle":
            nat_equivalences = ORCL_EQUIVALENCES
        case "postgres":
            nat_equivalences = PG_EQUIVALENCES
        case "sqlserver":
            nat_equivalences = SQLS_EQUIVALENCES

    # establish the ordinals
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

    return nat_ordinal, ref_ordinal
