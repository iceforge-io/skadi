package org.iceforge.skadi.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcArrowStreamerPreparedStatementTest {

    @Test
    void stream_preparedStatement_uses_bound_parameters() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             Connection conn = DriverManager.getConnection("jdbc:h2:mem:psparamtest;DB_CLOSE_DELAY=-1")) {

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE t_ps(id INT)");
                s.execute("INSERT INTO t_ps(id) VALUES (1), (2), (3)");
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM t_ps WHERE id > ? ORDER BY id")) {
                ps.setInt(1, 1);
                long rows = JdbcArrowStreamer.stream(ps, 1024, allocator, out, () -> false);
                assertEquals(2L, rows);
            }

            assertTrue(out.size() > 0);
        }
    }
}

