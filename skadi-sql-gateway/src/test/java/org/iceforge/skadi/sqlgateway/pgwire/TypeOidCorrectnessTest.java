package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B4 — PostgreSQL OID round-trip correctness.
 *
 * <p>The gateway sets OIDs in RowDescription via {@link JdbcToPgTypeMapper}. The
 * PostgreSQL JDBC driver on the client side maps those OIDs back to JDBC types.
 * This test asserts the full round-trip: H2 JDBC type → PG OID (in RowDescription) →
 * JDBC type reported by the client driver.
 *
 * <p>Correct OIDs matter because Tableau uses them to classify columns as measures
 * (numeric OIDs) or dimensions (text/date OIDs).
 */
class TypeOidCorrectnessTest {

    private GatewayCorrectnessHarness harness;
    private ResultSetMetaData meta;

    @BeforeEach
    void start() throws Exception {
        harness = new GatewayCorrectnessHarness();
    }

    @AfterEach
    void stop() {
        harness.close();
    }

    @Test
    void bigint_oid_roundtrip() throws Exception {
        assertColumnType("SELECT id_col FROM skadi_types", 1, Types.BIGINT);
    }

    @Test
    void integer_oid_roundtrip() throws Exception {
        assertColumnType("SELECT int_col FROM skadi_types", 1, Types.INTEGER);
    }

    @Test
    void smallint_oid_roundtrip() throws Exception {
        assertColumnType("SELECT small_col FROM skadi_types", 1, Types.SMALLINT);
    }

    @Test
    void double_oid_roundtrip() throws Exception {
        assertColumnType("SELECT double_col FROM skadi_types", 1, Types.DOUBLE);
    }

    @Test
    void numeric_oid_roundtrip() throws Exception {
        // PG OID 1700 (NUMERIC) → JDBC driver maps to Types.NUMERIC
        assertColumnType("SELECT dec_col FROM skadi_types", 1, Types.NUMERIC);
    }

    @Test
    void varchar_oid_roundtrip() throws Exception {
        assertColumnType("SELECT str_col FROM skadi_types", 1, Types.VARCHAR);
    }

    @Test
    void date_oid_roundtrip() throws Exception {
        assertColumnType("SELECT date_col FROM skadi_types", 1, Types.DATE);
    }

    @Test
    void timestamp_oid_roundtrip() throws Exception {
        assertColumnType("SELECT ts_col FROM skadi_types", 1, Types.TIMESTAMP);
    }

    // --- helper ---

    private void assertColumnType(String sql, int col, int expectedJdbcType) throws Exception {
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int actualType = md.getColumnType(col);
            assertThat(actualType)
                    .as("JDBC type for column %d of [%s]", col, sql)
                    .isEqualTo(expectedJdbcType);
        }
    }
}
