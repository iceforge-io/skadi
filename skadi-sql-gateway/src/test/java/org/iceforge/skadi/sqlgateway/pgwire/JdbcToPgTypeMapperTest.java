package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcToPgTypeMapperTest {

    @Test
    void maps_integral_types() {
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.BIGINT, 0, 0, null)).isEqualTo(PgType.INT8);
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.INTEGER, 0, 0, null)).isEqualTo(PgType.INT4);
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.SMALLINT, 0, 0, null)).isEqualTo(PgType.INT2);
    }

    @Test
    void maps_decimal_to_numeric_and_sets_typmod() {
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.DECIMAL, 10, 2, "DECIMAL")).isEqualTo(PgType.NUMERIC);
        // typmod should be non-zero when precision set
        assertThat(JdbcToPgTypeMapper.numericTypmod(10, 2)).isNotZero();
    }

    @Test
    void maps_double_and_string() {
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.DOUBLE, 0, 0, null)).isEqualTo(PgType.FLOAT8);
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.VARCHAR, 0, 0, null)).isEqualTo(PgType.VARCHAR);
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.CLOB, 0, 0, null)).isEqualTo(PgType.TEXT);
    }

    @Test
    void maps_date_and_timestamps() {
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.DATE, 0, 0, null)).isEqualTo(PgType.DATE);
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.TIMESTAMP, 0, 0, null)).isEqualTo(PgType.TIMESTAMP);
        assertThat(JdbcToPgTypeMapper.toPgOid(Types.TIMESTAMP_WITH_TIMEZONE, 0, 0, null)).isEqualTo(PgType.TIMESTAMPTZ);
    }
}

