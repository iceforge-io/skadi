package org.iceforge.skadi.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;

import static org.junit.jupiter.api.Assertions.*;

class JdbcArrowStreamerTest {

    @Test
    void stream_emits_all_rows_across_multiple_batches() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             Connection conn = DriverManager.getConnection("jdbc:h2:mem:test1;DB_CLOSE_DELAY=-1")) {

            try (Statement s = conn.createStatement()) {
                // use BLOB for variable-length binary data to avoid "value too long" errors
                s.execute("CREATE TABLE t(id INT, txt VARCHAR(100), d DATE, ts TIMESTAMP, dec DECIMAL(10,2), b BOOLEAN, blob BLOB)");
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t VALUES(?, ?, CURRENT_DATE, CURRENT_TIMESTAMP, ?, ?, ?)")) {
                    for (int i = 1; i <= 7; i++) {
                        ps.setInt(1, i);
                        ps.setString(2, "row-" + i);
                        ps.setBigDecimal(3, java.math.BigDecimal.valueOf(i * 1.5));
                        ps.setBoolean(4, i % 2 == 0);
                        ps.setBytes(5, ("b" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            // batchRows=3 to force multiple batches (3,3,1)
            long total = JdbcArrowStreamer.stream(conn, "SELECT * FROM t ORDER BY id", /*fetchSize*/100, /*batchRows*/3, allocator, out);

            assertEquals(7L, total);
            assertTrue(out.size() > 0, "Arrow output should be written");
        }
    }

    @Test
    void stream_respects_cancel_requested_after_first_batch() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             Connection conn = DriverManager.getConnection("jdbc:h2:mem:test2;DB_CLOSE_DELAY=-1")) {

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE t2(id INT, txt VARCHAR(100))");
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t2 VALUES(?, ?)")) {
                    for (int i = 1; i <= 25; i++) {
                        ps.setInt(1, i);
                        ps.setString(2, "row-" + i);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            AtomicBoolean cancelFlag = new AtomicBoolean(false);
            IntConsumer onBatch = batchSize -> {
                // request cancel after first batch is written
                cancelFlag.set(true);
            };

            long total = JdbcArrowStreamer.stream(
                    conn,
                    "SELECT * FROM t2 ORDER BY id",
                    /*fetchSize*/100,
                    /*batchRows*/10, // expect first batch to be 10
                    allocator,
                    out,
                    cancelFlag::get,
                    onBatch
            );

            assertEquals(10L, total, "Should have stopped after the first batch due to cancellation");
            assertTrue(out.size() > 0, "Arrow output should contain at least the first batch");
        }
    }
}