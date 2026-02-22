package org.iceforge.skadi.sqlgateway.pgwire;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/** Maps JDBC/Databricks result-set types to PostgreSQL type OIDs for RowDescription. */
final class JdbcToPgTypeMapper {
    private JdbcToPgTypeMapper() {}

    static int toPgOid(ResultSetMetaData md, int columnIndex) throws SQLException {
        int jdbcType = md.getColumnType(columnIndex);
        return toPgOid(jdbcType, md.getPrecision(columnIndex), md.getScale(columnIndex), md.getColumnTypeName(columnIndex));
    }

    static int toPgOid(int jdbcType, int precision, int scale, String typeName) {
        return switch (jdbcType) {
            case Types.BIGINT -> PgType.INT8;
            case Types.INTEGER -> PgType.INT4;
            case Types.SMALLINT, Types.TINYINT -> PgType.INT2;

            case Types.DECIMAL, Types.NUMERIC -> PgType.NUMERIC;

            case Types.DOUBLE -> PgType.FLOAT8;
            case Types.FLOAT, Types.REAL -> PgType.FLOAT4;

            case Types.BOOLEAN, Types.BIT -> PgType.BOOL;

            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR, Types.CHAR, Types.NCHAR -> PgType.VARCHAR;
            case Types.CLOB, Types.NCLOB -> PgType.TEXT;

            case Types.DATE -> PgType.DATE;

            // Databricks reports TIMESTAMP for timestamp_ntz; some driver versions may reuse TIMESTAMP_WITH_TIMEZONE.
            // Policy: advertise timestamptz only when the JDBC type indicates so; otherwise timestamp.
            case Types.TIMESTAMP_WITH_TIMEZONE -> PgType.TIMESTAMPTZ;
            case Types.TIMESTAMP -> PgType.TIMESTAMP;

            // For anything else, fall back to TEXT rather than UNKNOWN because Tableau/JDBC is happier.
            default -> PgType.TEXT;
        };
    }

    /** Compute pg "type modifier" for NUMERIC/DECIMAL columns. */
    static int numericTypmod(int precision, int scale) {
        if (precision <= 0) return 0; // "no typmod"
        // Per PostgreSQL: typmod = ((precision << 16) | scale) + VARHDRSZ, VARHDRSZ=4
        return ((precision & 0xFFFF) << 16) | (scale & 0xFFFF) | 4;
    }
}

