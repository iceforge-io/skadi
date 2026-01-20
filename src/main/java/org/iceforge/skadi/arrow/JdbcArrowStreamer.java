package org.iceforge.skadi.arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal JDBC -> Arrow IPC stream encoder.
 * Notes:
 * - This is intentionally "small and predictable" for v1.
 * - Add more JDBC type mappings over time as needed.
 */
public final class JdbcArrowStreamer {

    private JdbcArrowStreamer() {}

    public static void stream(Connection conn,
                              String sql,
                              int fetchSize,
                              int batchRows,
                              BufferAllocator allocator,
                              OutputStream out) throws Exception {

        stream(conn, sql, fetchSize, batchRows, allocator, out, () -> false);
    }

    public static void stream(Connection conn,
                              String sql,
                              int fetchSize,
                              int batchRows,
                              BufferAllocator allocator,
                              OutputStream out,
                              java.util.function.BooleanSupplier cancelRequested) throws Exception {

        stream(conn, sql, fetchSize, batchRows, allocator, out, cancelRequested, null);
    }

    /**
     * Streams the query as Arrow IPC and returns the total rows emitted.
     * If {@code onBatchRows} is non-null, it is invoked after each batch is written.
     */
    public static long stream(Connection conn,
                              String sql,
                              int fetchSize,
                              int batchRows,
                              BufferAllocator allocator,
                              OutputStream out,
                              java.util.function.BooleanSupplier cancelRequested,
                              java.util.function.IntConsumer onBatchRows) throws Exception {

        long totalRows = 0L;

        try (PreparedStatement ps = conn.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {
            if (fetchSize > 0) {
                ps.setFetchSize(fetchSize);
            }

            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                Schema schema = toArrowSchema(md);
                try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                     ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

                    writer.start();

                    int rowCount;
                    boolean canceled = false;
                    while (true) {
                        if (cancelRequested.getAsBoolean()) {
                            canceled = true;
                            break;
                        }
                        root.allocateNew();
                        rowCount = 0;

                        while (rowCount < batchRows && rs.next()) {
                            if (cancelRequested.getAsBoolean()) {
                                canceled = true;
                                break;
                            }
                            writeRow(root, md, rs, rowCount);
                            rowCount++;
                        }

                        if (canceled) {
                            if (rowCount > 0) {
                                root.setRowCount(rowCount);
                                writer.writeBatch();
                                totalRows += rowCount;
                                if (onBatchRows != null) {
                                    onBatchRows.accept(rowCount);
                                }
                                root.clear();
                            }
                            break;
                        }

                        if (rowCount == 0) {
                            break;
                        }

                        root.setRowCount(rowCount);
                        writer.writeBatch();
                        totalRows += rowCount;
                        if (onBatchRows != null) {
                            onBatchRows.accept(rowCount);
                        }
                        root.clear();

                        if (rowCount < batchRows) {
                            // drained
                            break;
                        }
                    }

                    writer.end();
                }
            }
        } catch (IOException ioe) {
            // propagate IO issues as-is (client disconnect, etc.)
            throw ioe;
        }

        return totalRows;
    }

    private static Schema toArrowSchema(ResultSetMetaData md) throws SQLException {
        int n = md.getColumnCount();
        List<Field> fields = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            String name = md.getColumnLabel(i);
            int jdbcType = md.getColumnType(i);
            int precision = md.getPrecision(i);
            int scale = md.getScale(i);
            boolean nullable = md.isNullable(i) != ResultSetMetaData.columnNoNulls;

            FieldType ft = new FieldType(nullable, toArrowType(jdbcType, precision, scale), null);
            fields.add(new Field(name, ft, /*children*/ null));
        }
        return new Schema(fields);
    }

    private static ArrowType toArrowType(int jdbcType, int precision, int scale) {
        return switch (jdbcType) {
            case Types.BOOLEAN, Types.BIT -> new ArrowType.Bool();
            case Types.TINYINT -> new ArrowType.Int(8, true);
            case Types.SMALLINT -> new ArrowType.Int(16, true);
            case Types.INTEGER -> new ArrowType.Int(32, true);
            case Types.BIGINT -> new ArrowType.Int(64, true);
            case Types.FLOAT, Types.REAL ->  new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case Types.DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case Types.DECIMAL, Types.NUMERIC -> new ArrowType.Decimal(Math.max(precision, 38), Math.max(scale, 0), 128);
            case Types.DATE -> new ArrowType.Date(DateUnit.DAY);
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR,
                 Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR -> new ArrowType.Utf8();
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> new ArrowType.Binary();
            default -> new ArrowType.Utf8();
        };
    }

    private static void writeRow(VectorSchemaRoot root, ResultSetMetaData md, ResultSet rs, int row) throws SQLException {
        int n = md.getColumnCount();
        for (int i = 1; i <= n; i++) {
            FieldVector v = root.getVector(i - 1);
            int jdbcType = md.getColumnType(i);

            if (rs.getObject(i) == null) {
                v.setNull(row);
                continue;
            }

            switch (jdbcType) {
                case Types.BOOLEAN, Types.BIT -> ((BitVector) v).setSafe(row, rs.getBoolean(i) ? 1 : 0);
                case Types.TINYINT -> ((TinyIntVector) v).setSafe(row, rs.getByte(i));
                case Types.SMALLINT -> ((SmallIntVector) v).setSafe(row, rs.getShort(i));
                case Types.INTEGER -> ((IntVector) v).setSafe(row, rs.getInt(i));
                case Types.BIGINT -> ((BigIntVector) v).setSafe(row, rs.getLong(i));
                case Types.FLOAT, Types.REAL -> ((Float4Vector) v).setSafe(row, rs.getFloat(i));
                case Types.DOUBLE -> ((Float8Vector) v).setSafe(row, rs.getDouble(i));
                case Types.DECIMAL, Types.NUMERIC -> {
                    BigDecimal bd = rs.getBigDecimal(i);
                    ((DecimalVector) v).setSafe(row, bd);
                }
                case Types.DATE -> {
                    java.sql.Date d = rs.getDate(i);
                    // Arrow DateUnit.DAY stores days since epoch
                    int days = (int) d.toLocalDate().toEpochDay();
                    ((DateDayVector) v).setSafe(row, days);
                }
                case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                    Timestamp ts = rs.getTimestamp(i);
                    long millis = ts.toInstant().toEpochMilli();
                    ((TimeStampMilliVector) v).setSafe(row, millis);
                }
                case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> {
                    byte[] b = rs.getBytes(i);
                    ((VarBinaryVector) v).setSafe(row, b);
                }
                default -> {
                    String s = rs.getString(i);
                    ((VarCharVector) v).setSafe(row, s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
                }
            }
        }
    }
}
