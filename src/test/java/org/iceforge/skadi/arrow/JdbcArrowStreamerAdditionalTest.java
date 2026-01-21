// java
package org.iceforge.skadi.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JdbcArrowStreamerAdditionalTest {

    @Test
    void stream_empty_result_writes_schema_and_returns_zero() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             Connection conn = DriverManager.getConnection("jdbc:h2:mem:emptytest;DB_CLOSE_DELAY=-1")) {

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE t_empty(id INT, txt VARCHAR(10))");
            }

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long total = JdbcArrowStreamer.stream(conn, "SELECT * FROM t_empty ORDER BY id", 100, 10, allocator, out);

            assertEquals(0L, total, "No rows should be emitted");
            assertTrue(out.size() > 0, "Schema/IPC header should have been written even with zero rows");
        }
    }

    @Test
    void stream_propagates_io_exception_when_output_fails_during_batch_write() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
             Connection conn = DriverManager.getConnection("jdbc:h2:mem:iofailtest;DB_CLOSE_DELAY=-1")) {

            try (Statement s = conn.createStatement()) {
                s.execute("CREATE TABLE t_io(id INT, txt VARCHAR(20))");
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO t_io VALUES(?, ?)")) {
                    ps.setInt(1, 1);
                    ps.setString(2, "row-1");
                    ps.addBatch();
                    ps.executeBatch();
                }
            }

            // OutputStream that allows the initial schema write but throws on the subsequent write (batch)
            OutputStream failing = new OutputStream() {
                final AtomicInteger calls = new AtomicInteger(0);
                @Override
                public void write(int b) throws IOException {
                    // delegate to byte-array write to centralize logic
                    write(new byte[]{(byte) b}, 0, 1);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    int c = calls.incrementAndGet();
                    // allow the first write (schema/header), but throw on the next write attempt
                    if (c >= 2) {
                        throw new IOException("simulated write failure");
                    }
                    // consume bytes silently otherwise
                }
            };

            // batchRows=1 to force a batch write and trigger the failing stream
            assertThrows(IOException.class, () ->
                    JdbcArrowStreamer.stream(conn, "SELECT * FROM t_io ORDER BY id", 100, 1, allocator, failing));
        }
    }

    @Test
    void toArrowSchema_reflects_column_names_and_count() throws Exception {
        // mock ResultSetMetaData to exercise toArrowSchema without requiring a DB roundtrip
        ResultSetMetaData md = mock(ResultSetMetaData.class);
        when(md.getColumnCount()).thenReturn(3);
        when(md.getColumnLabel(1)).thenReturn("col_a");
        when(md.getColumnLabel(2)).thenReturn("col_b");
        when(md.getColumnLabel(3)).thenReturn("col_c");
        when(md.getColumnType(1)).thenReturn(java.sql.Types.INTEGER);
        when(md.getColumnType(2)).thenReturn(java.sql.Types.VARCHAR);
        when(md.getColumnType(3)).thenReturn(java.sql.Types.TIMESTAMP);
        when(md.getPrecision(1)).thenReturn(0);
        when(md.getPrecision(2)).thenReturn(0);
        when(md.getPrecision(3)).thenReturn(0);
        when(md.getScale(1)).thenReturn(0);
        when(md.getScale(2)).thenReturn(0);
        when(md.getScale(3)).thenReturn(0);
        when(md.isNullable(anyInt())).thenReturn(ResultSetMetaData.columnNullable);

        Method toArrowSchema = JdbcArrowStreamer.class.getDeclaredMethod("toArrowSchema", ResultSetMetaData.class);
        toArrowSchema.setAccessible(true);
        Schema schema = (Schema) toArrowSchema.invoke(null, md);

        assertNotNull(schema);
        assertEquals(3, schema.getFields().size());
        assertEquals("col_a", schema.getFields().get(0).getName());
        assertEquals("col_b", schema.getFields().get(1).getName());
        assertEquals("col_c", schema.getFields().get(2).getName());
    }
    @Test
    void writeTimestampIntoVector_writes_millis_for_timestamp_vectors() throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE)) {
            Timestamp ts = Timestamp.from(java.time.Instant.parse("2020-01-02T03:04:05.678Z"));
            long expectedMillis = ts.toInstant().toEpochMilli();

            // TimeStampMilliVector
            try (TimeStampMilliVector tv = new TimeStampMilliVector("ts", allocator)) {
                tv.allocateNewSafe();
                Method writeTs = JdbcArrowStreamer.class.getDeclaredMethod("writeTimestampIntoVector", Timestamp.class, org.apache.arrow.vector.FieldVector.class, int.class);
                writeTs.setAccessible(true);
                writeTs.invoke(null, ts, (FieldVector) tv, 0);

                Object gotObj = tv.getObject(0);
                long gotMillis;
                if (gotObj instanceof Long) {
                    gotMillis = (Long) gotObj;
                } else if (gotObj instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) gotObj;
                    gotMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                } else {
                    fail("Unexpected value type from TimeStampMilliVector: " + (gotObj == null ? "null" : gotObj.getClass()));
                    return;
                }
                assertEquals(expectedMillis, gotMillis);
            }

            // TimeStampMilliTZVector (construct with Field + FieldType)
            FieldType tzFieldType = new FieldType(true, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null);
            Field tzField = new Field("tstz", tzFieldType, null);
            try (TimeStampMilliTZVector tvtz = new TimeStampMilliTZVector(tzField, allocator)) {
                tvtz.allocateNewSafe();
                Method writeTs = JdbcArrowStreamer.class.getDeclaredMethod("writeTimestampIntoVector", Timestamp.class, org.apache.arrow.vector.FieldVector.class, int.class);
                writeTs.setAccessible(true);
                writeTs.invoke(null, ts, (FieldVector) tvtz, 0);

                Object gotObj = tvtz.getObject(0);
                long gotMillis;
                if (gotObj instanceof Long) {
                    gotMillis = (Long) gotObj;
                } else if (gotObj instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) gotObj;
                    gotMillis = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                } else {
                    fail("Unexpected value type from TimeStampMilliTZVector: " + (gotObj == null ? "null" : gotObj.getClass()));
                    return;
                }
                assertEquals(expectedMillis, gotMillis);
            }
        }
    }
}