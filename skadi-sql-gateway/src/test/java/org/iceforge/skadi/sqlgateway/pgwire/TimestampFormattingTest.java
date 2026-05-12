package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B4 — Timestamp and date wire-format correctness.
 *
 * <p>Guarded regressions:
 * <ul>
 *   <li>Timestamps must arrive as {@code "yyyy-MM-dd HH:mm:ss"}, not as a raw
 *       epoch-millisecond integer or as {@code "yyyy-MM-dd HH:mm:ss.0"} (trailing ".0"
 *       from {@link java.sql.Timestamp#toString()}).</li>
 *   <li>Dates must arrive as {@code "yyyy-MM-dd"}.</li>
 *   <li>UTC baseline: the gateway reports {@code TimeZone=UTC}; timestamps stored without
 *       time-zone info are transmitted as-is.</li>
 * </ul>
 */
class TimestampFormattingTest {

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
    void timestamp_no_trailing_dot_zero() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT ts_col FROM skadi_types")) {
            assertThat(rs.next()).isTrue();
            String raw = rs.getString("ts_col");
            // Must not end with ".0" — that is Timestamp.toString() artifact
            assertThat(raw).doesNotEndWith(".0");
            assertThat(raw).doesNotEndWith(".000");
        }
    }

    @Test
    void timestamp_format_is_iso_like() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT ts_col FROM skadi_types")) {
            assertThat(rs.next()).isTrue();
            String raw = rs.getString("ts_col");
            // Expected: "2021-06-15 12:30:00"
            assertThat(raw).isEqualTo("2021-06-15 12:30:00");
        }
    }

    @Test
    void date_format_is_iso() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery("SELECT date_col FROM skadi_types")) {
            assertThat(rs.next()).isTrue();
            String raw = rs.getString("date_col");
            assertThat(raw).isEqualTo("2021-06-15");
        }
    }

    @Test
    void epoch_unix_timestamp_renders_as_date_not_number() throws Exception {
        // 1970-01-01 00:00:00 — epoch zero; must not come back as "0" or "0.0"
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT TIMESTAMP '1970-01-01 00:00:00' AS epoch_ts")) {
            assertThat(rs.next()).isTrue();
            String raw = rs.getString("epoch_ts");
            assertThat(raw).isEqualTo("1970-01-01 00:00:00");
        }
    }

    @Test
    void epoch_zero_date_renders_as_date_not_number() throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(
                     "SELECT DATE '1970-01-01' AS epoch_d")) {
            assertThat(rs.next()).isTrue();
            String raw = rs.getString("epoch_d");
            assertThat(raw).isEqualTo("1970-01-01");
        }
    }
}
