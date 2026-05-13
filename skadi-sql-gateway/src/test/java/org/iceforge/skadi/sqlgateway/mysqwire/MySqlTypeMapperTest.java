package org.iceforge.skadi.sqlgateway.mysqwire;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B8 — Verifies JDBC → MySQL wire-type mapping.
 */
class MySqlTypeMapperTest {

    @Test
    void bigint_maps_to_longlong() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.BIGINT))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_LONGLONG);
    }

    @Test
    void integer_maps_to_long() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.INTEGER))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_LONG);
    }

    @Test
    void smallint_maps_to_short() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.SMALLINT))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_SHORT);
    }

    @Test
    void double_maps_to_double() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.DOUBLE))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_DOUBLE);
    }

    @Test
    void decimal_maps_to_newdecimal() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.DECIMAL))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_NEWDECIMAL);
    }

    @Test
    void date_maps_to_date() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.DATE))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_DATE);
    }

    @Test
    void timestamp_maps_to_datetime() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.TIMESTAMP))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_DATETIME);
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.TIMESTAMP_WITH_TIMEZONE))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_DATETIME);
    }

    @Test
    void varchar_maps_to_var_string() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.VARCHAR))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_VAR_STRING);
    }

    @Test
    void clob_maps_to_blob() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.CLOB))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_BLOB);
    }

    @Test
    void unknown_type_defaults_to_var_string() {
        assertThat(JdbcToMySqlTypeMapper.toMySqlType(Types.OTHER))
                .isEqualTo(JdbcToMySqlTypeMapper.MYSQL_TYPE_VAR_STRING);
    }

    @Test
    void numeric_types_have_num_flag() {
        assertThat(JdbcToMySqlTypeMapper.flagsFor(Types.INTEGER))
                .isEqualTo(JdbcToMySqlTypeMapper.FLAG_NUM);
        assertThat(JdbcToMySqlTypeMapper.flagsFor(Types.BIGINT))
                .isEqualTo(JdbcToMySqlTypeMapper.FLAG_NUM);
    }

    @Test
    void string_type_has_no_num_flag() {
        assertThat(JdbcToMySqlTypeMapper.flagsFor(Types.VARCHAR)).isEqualTo(0);
    }

    @Test
    void display_len_falls_back_to_precision() {
        assertThat(JdbcToMySqlTypeMapper.displayLen(Types.VARCHAR, 100)).isEqualTo(100);
    }

    @Test
    void display_len_default_for_bigint_when_no_precision() {
        assertThat(JdbcToMySqlTypeMapper.displayLen(Types.BIGINT, 0)).isEqualTo(20);
    }
}
