package org.iceforge.skadi.sqlgateway.mysqwire;

import java.sql.Types;

/** Maps JDBC column types to MySQL wire-protocol type codes. */
final class JdbcToMySqlTypeMapper {
    // MySQL type codes (subset used for result set column definitions)
    static final int MYSQL_TYPE_TINY      = 1;
    static final int MYSQL_TYPE_SHORT     = 2;
    static final int MYSQL_TYPE_LONG      = 3;
    static final int MYSQL_TYPE_FLOAT     = 4;
    static final int MYSQL_TYPE_DOUBLE    = 5;
    static final int MYSQL_TYPE_TIMESTAMP = 7;
    static final int MYSQL_TYPE_LONGLONG  = 8;
    static final int MYSQL_TYPE_DATE      = 10;
    static final int MYSQL_TYPE_DATETIME  = 12;
    static final int MYSQL_TYPE_BLOB      = 252;
    static final int MYSQL_TYPE_VAR_STRING = 253;
    static final int MYSQL_TYPE_STRING    = 254;
    static final int MYSQL_TYPE_NEWDECIMAL = 246;

    // MySQL column flags
    static final int FLAG_NOT_NULL     = 0x0001;
    static final int FLAG_BINARY       = 0x0080;
    static final int FLAG_NUM          = 0x8000;

    private JdbcToMySqlTypeMapper() {}

    static int toMySqlType(int jdbcType) {
        return switch (jdbcType) {
            case Types.BIGINT                      -> MYSQL_TYPE_LONGLONG;
            case Types.INTEGER                     -> MYSQL_TYPE_LONG;
            case Types.SMALLINT, Types.TINYINT     -> MYSQL_TYPE_SHORT;
            case Types.DOUBLE                      -> MYSQL_TYPE_DOUBLE;
            case Types.FLOAT, Types.REAL           -> MYSQL_TYPE_FLOAT;
            case Types.DECIMAL, Types.NUMERIC      -> MYSQL_TYPE_NEWDECIMAL;
            case Types.BOOLEAN, Types.BIT          -> MYSQL_TYPE_TINY;
            case Types.DATE                        -> MYSQL_TYPE_DATE;
            case Types.TIMESTAMP,
                 Types.TIMESTAMP_WITH_TIMEZONE     -> MYSQL_TYPE_DATETIME;
            case Types.CLOB, Types.NCLOB,
                 Types.LONGVARCHAR, Types.LONGNVARCHAR -> MYSQL_TYPE_BLOB;
            default                                -> MYSQL_TYPE_VAR_STRING;
        };
    }

    static int flagsFor(int jdbcType) {
        return switch (jdbcType) {
            case Types.BIGINT, Types.INTEGER, Types.SMALLINT, Types.TINYINT,
                 Types.DOUBLE, Types.FLOAT, Types.REAL, Types.DECIMAL, Types.NUMERIC,
                 Types.BOOLEAN, Types.BIT -> FLAG_NUM;
            default -> 0;
        };
    }

    static int displayLen(int jdbcType, int precision) {
        if (precision > 0) return precision;
        return switch (jdbcType) {
            case Types.BIGINT    -> 20;
            case Types.INTEGER   -> 11;
            case Types.SMALLINT  -> 6;
            case Types.TINYINT   -> 4;
            case Types.DOUBLE    -> 22;
            case Types.FLOAT     -> 12;
            case Types.BOOLEAN   -> 1;
            case Types.DATE      -> 10;
            case Types.TIMESTAMP -> 19;
            default              -> 255;
        };
    }
}
