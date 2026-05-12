package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B4 — ORDER BY, LIMIT, and OFFSET correctness.
 *
 * <p>Tableau issues queries with ORDER BY + LIMIT to fetch sorted previews.
 * These tests verify that the gateway forwards ordering and cardinality constraints
 * faithfully to the backend and returns results in the declared order.
 */
class OrderingLimitTest {

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
    void order_by_asc_returns_sorted_rows() throws Exception {
        List<Integer> values = queryInts("SELECT n FROM skadi_orders ORDER BY n ASC");
        assertThat(values).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    void order_by_desc_returns_reverse_sorted_rows() throws Exception {
        List<Integer> values = queryInts("SELECT n FROM skadi_orders ORDER BY n DESC");
        assertThat(values).containsExactly(5, 4, 3, 2, 1);
    }

    @Test
    void limit_restricts_row_count() throws Exception {
        List<Integer> values = queryInts("SELECT n FROM skadi_orders ORDER BY n ASC LIMIT 3");
        assertThat(values).hasSize(3);
        assertThat(values).containsExactly(1, 2, 3);
    }

    @Test
    void limit_with_offset_skips_leading_rows() throws Exception {
        List<Integer> values = queryInts(
                "SELECT n FROM skadi_orders ORDER BY n ASC LIMIT 2 OFFSET 2");
        assertThat(values).containsExactly(3, 4);
    }

    @Test
    void order_by_string_column_is_lexicographic() throws Exception {
        List<String> labels = queryStrings(
                "SELECT label FROM skadi_orders ORDER BY label ASC");
        assertThat(labels).containsExactly("a", "b", "c", "d", "e");
    }

    @Test
    void limit_1_returns_exactly_one_row() throws Exception {
        List<Integer> values = queryInts(
                "SELECT n FROM skadi_orders ORDER BY n ASC LIMIT 1");
        assertThat(values).hasSize(1).containsExactly(1);
    }

    // --- helpers ---

    private List<Integer> queryInts(String sql) throws Exception {
        List<Integer> result = new ArrayList<>();
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) result.add(rs.getInt(1));
        }
        return result;
    }

    private List<String> queryStrings(String sql) throws Exception {
        List<String> result = new ArrayList<>();
        try (Connection c = harness.jdbcClient();
             Statement st = c.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            while (rs.next()) result.add(rs.getString(1));
        }
        return result;
    }
}
