package org.iceforge.skadi.sqlgateway.pgwire;

import org.apache.arrow.memory.RootAllocator;
import org.h2.jdbcx.JdbcDataSource;
import org.iceforge.skadi.arrow.JdbcArrowStreamer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.sql.Connection;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that Arrow IPC bytes produced by {@link JdbcArrowStreamer} can be decoded
 * into pgwire RowDescription + DataRow messages by {@link ArrowIpcRowWriter}.
 */
class ArrowIpcRowWriterTest {

    private static JdbcDataSource ds;

    @BeforeAll
    static void setup() {
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:arrowritertest;DB_CLOSE_DELAY=-1");
    }

    @Test
    void decodesIntegerResult() throws Exception {
        byte[] arrowIpc = queryToArrow("SELECT 42 AS answer");

        ByteArrayOutputStream pgBuf = new ByteArrayOutputStream();
        long rows = ArrowIpcRowWriter.writeRows(arrowIpc, new DataOutputStream(pgBuf));

        assertThat(rows).isEqualTo(1);
        // Output must contain RowDescription ('T') and DataRow ('D')
        byte[] pg = pgBuf.toByteArray();
        assertThat(pg[0]).isEqualTo((byte) 'T'); // RowDescription
        assertThat(containsByte(pg, (byte) 'D')).isTrue(); // at least one DataRow
    }

    @Test
    void decodesMultiRowResult() throws Exception {
        byte[] arrowIpc = queryToArrow(
                "SELECT x FROM (VALUES (1), (2), (3)) AS t(x)");

        ByteArrayOutputStream pgBuf = new ByteArrayOutputStream();
        long rows = ArrowIpcRowWriter.writeRows(arrowIpc, new DataOutputStream(pgBuf));

        assertThat(rows).isEqualTo(3);
    }

    @Test
    void emptyBytesProducesZeroRows() throws Exception {
        ByteArrayOutputStream pgBuf = new ByteArrayOutputStream();
        long rows = ArrowIpcRowWriter.writeRows(new byte[0], new DataOutputStream(pgBuf));

        assertThat(rows).isEqualTo(0);
        // Should still write an empty RowDescription
        byte[] pg = pgBuf.toByteArray();
        assertThat(pg[0]).isEqualTo((byte) 'T');
    }

    @Test
    void decodesStringColumn() throws Exception {
        byte[] arrowIpc = queryToArrow("SELECT 'hello' AS greeting");

        ByteArrayOutputStream pgBuf = new ByteArrayOutputStream();
        long rows = ArrowIpcRowWriter.writeRows(arrowIpc, new DataOutputStream(pgBuf));

        assertThat(rows).isEqualTo(1);
        // Check that the column value 'hello' appears in the raw bytes
        String pgOutput = new String(pgBuf.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        assertThat(pgOutput).contains("hello");
    }

    // --- helpers ---

    private static byte[] queryToArrow(String sql) throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (Connection conn = ds.getConnection();
             Statement st = conn.createStatement()) {
            try (var allocator = new RootAllocator()) {
                JdbcArrowStreamer.stream(conn, sql, 0, 1024, allocator, buf);
            }
        }
        return buf.toByteArray();
    }

    private static boolean containsByte(byte[] arr, byte target) {
        for (byte b : arr) if (b == target) return true;
        return false;
    }
}
