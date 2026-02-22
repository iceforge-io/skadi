package org.iceforge.skadi.sqlgateway.pgwire;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

/**
 * Low-level pgwire row encoding helpers.
 *
 * <p>We only support text format (formatCode=0) for now.
 */
final class PgRowWriter {
    private PgRowWriter() {}

    static void writeDataRow(DataOutputStream out, String[] values) throws IOException {
        // Size dynamically to avoid truncation for large fields.
        int size = 2;
        byte[][] encoded = new byte[values.length][];
        for (int i = 0; i < values.length; i++) {
            String v = values[i];
            if (v == null) {
                size += 4;
            } else {
                byte[] b = v.getBytes(StandardCharsets.UTF_8);
                encoded[i] = b;
                size += 4 + b.length;
            }
        }

        ByteBuffer b = ByteBuffer.allocate(size).order(ByteOrder.BIG_ENDIAN);
        b.putShort((short) values.length);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                b.putInt(-1);
            } else {
                byte[] bytes = encoded[i];
                b.putInt(bytes.length);
                b.put(bytes);
            }
        }

        out.writeByte('D');
        out.writeInt(4 + b.position());
        out.write(b.array(), 0, b.position());
    }
}

