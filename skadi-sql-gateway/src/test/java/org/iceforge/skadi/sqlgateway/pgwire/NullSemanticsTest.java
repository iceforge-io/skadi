package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B4 — Null semantics: SQL NULL must arrive as Java {@code null} at the JDBC client,
 * not as the string {@code "null"} or an empty string.
 *
 * <p>Verifies pgwire length=-1 encoding for nulls in both varchar and numeric columns.
 */
class NullSemanticsTest {

    private GatewayCorrectnessHarness harness;

    @BeforeEach
    void start() throws Exception {
        harness = new GatewayCorrectnessHarness();
    }

    @AfterEach
    void stop() {
        harness.close();
    }

    @Test
    void varchar_null_arrives_as_java_null() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT val FROM skadi_nulls WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("val")).isNull();
            assertThat(rs.wasNull()).isTrue();
        }
    }

    @Test
    void numeric_null_arrives_as_java_null() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT num FROM skadi_nulls WHERE id = 1")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getObject("num")).isNull();
            assertThat(rs.wasNull()).isTrue();
        }
    }

    @Test
    void non_null_column_is_not_null() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT val, num FROM skadi_nulls WHERE id = 2")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("val")).isEqualTo("present");
            assertThat(rs.wasNull()).isFalse();
            assertThat(rs.getBigDecimal("num")).isNotNull();
        }
    }

    @Test
    void null_in_typed_column_from_main_table() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT null_col FROM skadi_types")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("null_col")).isNull();
            assertThat(rs.wasNull()).isTrue();
        }
    }
}
