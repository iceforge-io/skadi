package org.iceforge.skadi.sqlgateway.pgwire;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Reads Arrow IPC streaming bytes and writes pgwire RowDescription + DataRow messages.
 *
 * <p>All values are rendered in text format (formatCode=0) so the encoding is transparent
 * to any PostgreSQL-compatible client.
 */
final class ArrowIpcRowWriter {

    private ArrowIpcRowWriter() {}

    /**
     * Decodes Arrow IPC streaming bytes and writes pgwire RowDescription + DataRows.
     *
     * @param arrowIpc Arrow IPC bytes (streaming format) written by {@link org.iceforge.skadi.arrow.JdbcArrowStreamer}
     * @param out      pgwire output stream; caller is responsible for flushing and writing CommandComplete
     * @return number of DataRow messages written
     */
    static long writeRows(byte[] arrowIpc, DataOutputStream out) throws IOException {
        if (arrowIpc == null || arrowIpc.length == 0) {
            writeEmptyRowDescription(out);
            return 0;
        }

        try (BufferAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(arrowIpc), allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<Field> fields = root.getSchema().getFields();
            writeRowDescription(out, fields);

            long rowCount = 0;
            final int flushEvery = 256;

            while (reader.loadNextBatch()) {
                int batchRows = root.getRowCount();
                for (int r = 0; r < batchRows; r++) {
                    String[] row = new String[fields.size()];
                    for (int c = 0; c < fields.size(); c++) {
                        FieldVector v = root.getVector(c);
                        row[c] = v.isNull(r) ? null : formatValue(v.getObject(r));
                    }
                    PgRowWriter.writeDataRow(out, row);
                    rowCount++;
                    if (rowCount % flushEvery == 0) {
                        out.flush();
                    }
                }
            }

            return rowCount;

        } catch (IOException ioe) {
            throw ioe;
        } catch (Exception e) {
            throw new IOException("Failed to decode Arrow IPC bytes: " + e.getMessage(), e);
        }
    }

    private static String formatValue(Object val) {
        if (val == null) return null;
        // Arrow Text wraps bytes; toString() returns the string correctly.
        return val.toString();
    }

    private static void writeEmptyRowDescription(DataOutputStream out) throws IOException {
        // RowDescription with 0 fields: 'T' + int32(6) + int16(0)
        out.writeByte('T');
        out.writeInt(6);
        out.writeShort(0);
    }

    private static void writeRowDescription(DataOutputStream out, List<Field> fields) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(Math.max(256, fields.size() * 64))
                .order(ByteOrder.BIG_ENDIAN);
        b.putShort((short) fields.size());
        for (Field field : fields) {
            putCString(b, field.getName());
            b.putInt(0);            // table OID
            b.putShort((short) 0);  // attribute number
            b.putInt(arrowTypeToPgOid(field.getType()));
            b.putShort((short) -1); // type size (variable)
            b.putInt(0);            // type modifier
            b.putShort((short) 0);  // format: text
            if (b.remaining() < 128) {
                ByteBuffer nb = ByteBuffer.allocate(b.capacity() * 2).order(ByteOrder.BIG_ENDIAN);
                b.flip();
                nb.put(b);
                b = nb;
            }
        }
        int msgLen = b.position();
        out.writeByte('T');
        out.writeInt(4 + msgLen);
        out.write(b.array(), 0, msgLen);
    }

    private static int arrowTypeToPgOid(ArrowType type) {
        return switch (type.getTypeID()) {
            case Bool -> PgType.BOOL;
            case Int -> {
                ArrowType.Int i = (ArrowType.Int) type;
                yield switch (i.getBitWidth()) {
                    case 8, 16 -> PgType.INT2;
                    case 32    -> PgType.INT4;
                    case 64    -> PgType.INT8;
                    default    -> PgType.TEXT;
                };
            }
            case FloatingPoint -> {
                ArrowType.FloatingPoint fp = (ArrowType.FloatingPoint) type;
                yield (fp.getPrecision() == org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE)
                        ? PgType.FLOAT4
                        : PgType.FLOAT8;
            }
            case Decimal   -> PgType.NUMERIC;
            case Date      -> PgType.DATE;
            case Time      -> PgType.TIME;
            case Timestamp -> PgType.TIMESTAMP;
            default        -> PgType.TEXT;
        };
    }

    private static void putCString(ByteBuffer b, String s) {
        if (s != null) b.put(s.getBytes(StandardCharsets.UTF_8));
        b.put((byte) 0);
    }
}
