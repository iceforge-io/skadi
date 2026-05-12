package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B4 — Decimal scale and value correctness.
 *
 * <p>Tableau uses NUMERIC/DECIMAL column values for measure aggregation. The wire
 * representation must preserve declared scale so clients parse the right magnitude.
 */
class DecimalCorrectnessTest {

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
    void scale_preserved_for_positive_value() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT val FROM skadi_decimals WHERE label = 'positive'")) {
            assertThat(rs.next()).isTrue();
            BigDecimal v = rs.getBigDecimal("val");
            // 1234.5600 stored with scale=4; wire value must not lose or gain digits
            assertThat(v.toPlainString()).isEqualTo("1234.5600");
        }
    }

    @Test
    void negative_decimal_sign_preserved() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT val FROM skadi_decimals WHERE label = 'negative'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("val").signum()).isNegative();
            assertThat(rs.getString("val")).startsWith("-");
        }
    }

    @Test
    void zero_decimal_renders_without_sign_error() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT val FROM skadi_decimals WHERE label = 'zero'")) {
            assertThat(rs.next()).isTrue();
            assertThat(rs.getBigDecimal("val").compareTo(BigDecimal.ZERO)).isZero();
        }
    }

    @Test
    void small_fractional_value_not_truncated() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT val FROM skadi_decimals WHERE label = 'small'")) {
            assertThat(rs.next()).isTrue();
            BigDecimal v = rs.getBigDecimal("val");
            assertThat(v.compareTo(new BigDecimal("0.0001"))).isZero();
        }
    }

    @Test
    void inline_decimal_cast_scale_preserved() throws Exception {
        // Inline CAST — exercises the type OID path with no table involved.
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT CAST(99.90 AS DECIMAL(8,2)) AS v")) {
            assertThat(rs.next()).isTrue();
            // String representation via JDBC BigDecimal should be "99.90"
            assertThat(rs.getBigDecimal("v").toPlainString()).isEqualTo("99.90");
        }
    }
}
