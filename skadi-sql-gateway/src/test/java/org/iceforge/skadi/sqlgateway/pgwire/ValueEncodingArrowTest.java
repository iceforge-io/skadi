package org.iceforge.skadi.sqlgateway.pgwire;

import org.apache.arrow.memory.RootAllocator;
import org.h2.jdbcx.JdbcDataSource;
import org.iceforge.skadi.arrow.JdbcArrowStreamer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * B4 — Verifies that {@link ArrowIpcRowWriter} renders each JDBC/Arrow type as the correct
 * pgwire text-format string.
 *
 * <p>Key regressions guarded:
 * <ul>
 *   <li>Timestamps must be {@code "yyyy-MM-dd HH:mm:ss"}, not epoch milliseconds.</li>
 *   <li>Dates must be {@code "yyyy-MM-dd"}, not epoch days.</li>
 *   <li>Decimals must preserve declared scale ({@code "1234.56"} not {@code "1234.5600"}).</li>
 *   <li>SQL NULL must produce a Java {@code null} value, not the string {@code "null"}.</li>
 * </ul>
 */
class ValueEncodingArrowTest {

    private static JdbcDataSource ds;

    @BeforeAll
    static void setup() {
        ds = new JdbcDataSource();
        ds.setURL(GatewayCorrectnessHarness.H2_URL);
    }

    @Test
    void timestamp_rendered_as_iso_not_epoch_millis() throws Exception {
        List<List<String>> rows = queryRows("SELECT TIMESTAMP '2021-06-15 12:30:00' AS ts");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get(0)).isEqualTo("2021-06-15 12:30:00");
    }

    @Test
    void date_rendered_as_iso_not_epoch_days() throws Exception {
        List<List<String>> rows = queryRows("SELECT DATE '2021-06-15' AS d");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get(0)).isEqualTo("2021-06-15");
    }

    @Test
    void decimal_scale_preserved() throws Exception {
        List<List<String>> rows = queryRows("SELECT CAST(1234.56 AS DECIMAL(10,2)) AS d");
        assertThat(rows).hasSize(1);
        // Must be "1234.56" — not "1234.5600" or "1234.6"
        assertThat(rows.get(0).get(0)).isEqualTo("1234.56");
    }

    @Test
    void integer_renders_correctly() throws Exception {
        List<List<String>> rows = queryRows("SELECT CAST(42 AS INTEGER) AS n");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get(0)).isEqualTo("42");
    }

    @Test
    void bigint_renders_correctly() throws Exception {
        List<List<String>> rows = queryRows("SELECT CAST(9000000000001 AS BIGINT) AS n");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get(0)).isEqualTo("9000000000001");
    }

    @Test
    void boolean_renders_as_true_false() throws Exception {
        List<List<String>> rows = queryRows("SELECT TRUE AS b");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get(0)).isIn("true", "TRUE", "1"); // Arrow BitVector → "true"
    }

    @Test
    void null_value_is_null_not_string_null() throws Exception {
        // Seed H2 table is already created; use a direct literal.
        List<List<String>> rows = queryRows("SELECT CAST(NULL AS VARCHAR) AS v");
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).get(0)).isNull();
    }

    @Test
    void multi_row_count_matches() throws Exception {
        List<List<String>> rows = queryRows(
                "SELECT n FROM skadi_orders ORDER BY n");
        assertThat(rows).hasSize(5);
        assertThat(rows.get(0).get(0)).isEqualTo("1");
        assertThat(rows.get(4).get(0)).isEqualTo("5");
    }

    // --- helpers ---

    private List<List<String>> queryRows(String sql) throws Exception {
        byte[] arrowIpc = queryToArrow(sql);
        ByteArrayOutputStream pgBuf = new ByteArrayOutputStream();
        ArrowIpcRowWriter.writeRows(arrowIpc, new DataOutputStream(pgBuf));
        return parsePgwireDataRows(pgBuf.toByteArray());
    }

    private byte[] queryToArrow(String sql) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (Connection conn = ds.getConnection()) {
            try (var allocator = new RootAllocator()) {
                JdbcArrowStreamer.stream(conn, sql, 0, 1024, allocator, buf);
            }
        }
        return buf.toByteArray();
    }

    /**
     * Minimal pgwire response parser: extracts DataRow ('D') string values.
     * Skips RowDescription ('T') and stops at the first non-D/T byte.
     */
    static List<List<String>> parsePgwireDataRows(byte[] pgwireBytes) {
        List<List<String>> result = new ArrayList<>();
        ByteBuffer buf = ByteBuffer.wrap(pgwireBytes).order(ByteOrder.BIG_ENDIAN);

        while (buf.hasRemaining()) {
            byte msgType = buf.get();
            int msgLen = buf.getInt(); // includes the 4-byte length itself
            int bodyLen = msgLen - 4;

            if (msgType == 'T') {
                // RowDescription — skip
                buf.position(buf.position() + bodyLen);
            } else if (msgType == 'D') {
                // DataRow
                short numFields = buf.getShort();
                List<String> row = new ArrayList<>(numFields);
                for (int i = 0; i < numFields; i++) {
                    int fieldLen = buf.getInt();
                    if (fieldLen == -1) {
                        row.add(null);
                    } else {
                        byte[] bytes = new byte[fieldLen];
                        buf.get(bytes);
                        row.add(new String(bytes, StandardCharsets.UTF_8));
                    }
                }
                result.add(row);
            } else {
                // CommandComplete or anything else — done
                break;
            }
        }
        return result;
    }
}
