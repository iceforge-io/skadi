package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.iceforge.skadi.sqlgateway.metadata.MetadataCache;
import org.iceforge.skadi.sqlgateway.metadata.MetadataQueryRouter;
import org.iceforge.skadi.sqlgateway.metadata.MetadataRowSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class PgWireSession implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PgWireSession.class);

    private static final MetadataCache METADATA_CACHE = new MetadataCache(Clock.systemUTC());

    private final Socket socket;
    private final SqlGatewayProperties.PgWire props;
    private final MetadataQueryRouter metadata;

    // Minimal extended-query state.
    private String lastPreparedSql;
    private String lastStatementName;
    private String lastPortalName;
    private boolean portalHasResult;

    PgWireSession(Socket socket, SqlGatewayProperties.PgWire props) {
        this.socket = Objects.requireNonNull(socket);
        this.props = Objects.requireNonNull(props);

        // Metadata facade defaults. (We can plumb in SqlGatewayProperties.Metadata once PgWireServer passes it.)
        Duration ttl = Duration.ofMinutes(2);
        String pgDb = "postgres";
        String catalog = "main";
        String schema = "public";
        this.metadata = new MetadataQueryRouter(METADATA_CACHE, ttl, pgDb, catalog, schema);
    }

    @Override
    public void run() {
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            // Startup packet is untyped: int32 len, int32 protocol/version or special request code.
            int len = in.readInt();
            byte[] payload = in.readNBytes(len - 4);
            ByteBuffer buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
            int code = buf.getInt();

            if (code == 80877103) { // SSLRequest
                out.writeByte('N');
                out.flush();

                // Next packet must be StartupMessage.
                len = in.readInt();
                payload = in.readNBytes(len - 4);
                buf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
                code = buf.getInt();
            }

            if (code != 196608) { // protocol 3.0
                writeError(out, "08000", "Unsupported protocol");
                writeReady(out);
                out.flush();
                return;
            }

            Map<String, String> params = readStartupParams(buf);
            String user = params.getOrDefault("user", "");

            if (requiresPassword()) {
                writeAuthCleartext(out);
                out.flush();

                // Expect PasswordMessage: 'p' + len + password\0
                byte type = in.readByte();
                if (type != 'p') {
                    writeError(out, "28P01", "password authentication failed");
                    writeReady(out);
                    out.flush();
                    return;
                }
                int mlen = in.readInt();
                byte[] pwdBytes = in.readNBytes(mlen - 4);
                String password = cstring(pwdBytes, 0);

                if (!isPasswordValid(user, password)) {
                    writeError(out, "28P01", "password authentication failed");
                    writeReady(out);
                    out.flush();
                    return;
                }
            }

            writeAuthOk(out);
            writeParameterStatus(out, "server_version", "15.0");
            writeParameterStatus(out, "client_encoding", "UTF8");
            writeParameterStatus(out, "DateStyle", "ISO, MDY");
            writeParameterStatus(out, "standard_conforming_strings", "on");
            writeParameterStatus(out, "TimeZone", "UTC");
            writeBackendKeyData(out, 1, 1);
            writeReady(out);
            out.flush();

            // Main loop.
            while (true) {
                byte type;
                try {
                    type = in.readByte();
                } catch (EOFException eof) {
                    return;
                }
                int mlen = in.readInt();
                byte[] msg = in.readNBytes(mlen - 4);

                if (log.isDebugEnabled()) {
                    log.debug("pgwire <= type={} len={}", (char) type, mlen);
                }

                if (type == 'X') {
                    return;
                }

                if (type == 'Q') {
                    String sql = cstring(msg, 0);
                    if (log.isDebugEnabled()) log.debug("pgwire simple query: {}", sql);
                    handleSimpleQuery(out, sql);
                    writeReady(out);
                    out.flush();
                    continue;
                }

                // Extended query flow (minimal subset for JDBC/Tableau).
                if (type == 'P') { // Parse
                    ByteBuffer mb = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
                    this.lastStatementName = readCString(mb);
                    String sql = readCString(mb);
                    this.lastPreparedSql = sql;
                    writeParseComplete(out);
                    out.flush();
                    continue;
                }

                if (type == 'B') { // Bind
                    ByteBuffer mb = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
                    this.lastPortalName = readCString(mb);
                    readCString(mb); // statement name (ignored, we only keep last)
                    this.portalHasResult = guessHasResult(this.lastPreparedSql);
                    writeBindComplete(out);
                    out.flush();
                    continue;
                }

                if (type == 'D') { // Describe
                    ByteBuffer mb = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
                    byte what = mb.get();
                    readCString(mb); // name

                    // JDBC expects ParameterDescription before RowDescription/NoData.
                    if (what == 'S' || what == 'P') {
                        writeParameterDescription(out, new int[0]);
                    }

                    if (what == 'S') {
                        // Statement metadata.
                        if (guessHasResult(this.lastPreparedSql)) {
                            writeRowDescription(out, new String[]{"?column?"});
                        } else {
                            writeNoData(out);
                        }
                        out.flush();
                        continue;
                    }

                    if (what == 'P') {
                        // Portal metadata.
                        if (this.portalHasResult) {
                            writeRowDescription(out, new String[]{"?column?"});
                        } else {
                            writeNoData(out);
                        }
                        out.flush();
                        continue;
                    }

                    writeError(out, "0A000", "Unsupported Describe target: " + (char) what);
                    out.flush();
                    continue;
                }

                if (type == 'E') { // Execute
                    // Payload: portal name (cstring) + int32 maxRows
                    ByteBuffer mb = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
                    readCString(mb); // portal name
                    mb.getInt(); // maxRows

                    String sql = this.lastPreparedSql;
                    handleExecute(out, sql);
                    writeReady(out);
                    out.flush();
                    continue;
                }

                if (type == 'C') { // Close
                    ByteBuffer mb = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
                    mb.get();
                    readCString(mb); // name
                    writeCloseComplete(out);
                    out.flush();
                    continue;
                }

                if (type == 'H') { // Flush
                    out.flush();
                    continue;
                }

                if (type == 'S') { // Sync
                    writeReady(out);
                    out.flush();
                    continue;
                }

                // Unsupported message.
                writeError(out, "0A000", "Unsupported message type: " + (char) type);
                writeReady(out);
                out.flush();
            }

        } catch (Exception e) {
            log.debug("pgwire session ended with error: {}", e.toString());
        }
    }

    private void handleExecute(DataOutputStream out, String sql) throws IOException {
        String s = sql == null ? "" : sql.trim();
        if (s.isEmpty()) {
            writeEmptyQueryResponse(out);
            return;
        }

        Optional<MetadataRowSet> meta = metadata.tryAnswer(s);
        if (meta.isPresent()) {
            writeRowSet(out, meta.get());
            return;
        }

        String lower = s.toLowerCase(Locale.ROOT);
        if (lower.equals("select 1") || lower.equals("select 1;") || lower.equals("select 1 as one") || lower.equals("select 1 as one;")) {
            writeRowDescription(out, new String[]{"?column?"});
            PgRowWriter.writeDataRow(out, new String[]{"1"});
            writeCommandComplete(out, "SELECT 1");
            return;
        }

        // Default: reuse simple handlers (which may return command complete / error).
        handleSimpleQuery(out, s);
    }

    private static boolean guessHasResult(String sql) {
        if (sql == null) return false;
        String lower = sql.trim().toLowerCase(Locale.ROOT);
        return lower.startsWith("select") || lower.startsWith("show") || lower.startsWith("with") || lower.startsWith("values");
    }

    private void handleSimpleQuery(DataOutputStream out, String sql) throws IOException {
        String s = sql == null ? "" : sql.trim();
        if (s.isEmpty()) {
            writeEmptyQueryResponse(out);
            return;
        }

        Optional<MetadataRowSet> meta = metadata.tryAnswer(s);
        if (meta.isPresent()) {
            writeRowSet(out, meta.get());
            return;
        }

        String lower = s.toLowerCase(Locale.ROOT);

        // --- Common JDBC bootstrap queries ---

        // e.g. "SET extra_float_digits = 3", "SET application_name = 'PostgreSQL JDBC Driver'"
        if (lower.startsWith("set ") || lower.startsWith("reset ")) {
            writeCommandComplete(out, "SET");
            return;
        }

        // e.g. "SHOW standard_conforming_strings"
        if (lower.startsWith("show ")) {
            String setting = lower.substring(5).trim();
            if (setting.endsWith(";")) setting = setting.substring(0, setting.length() - 1).trim();
            String val = switch (setting) {
                case "standard_conforming_strings" -> "on";
                case "client_encoding" -> "UTF8";
                case "datestyle" -> "ISO, MDY";
                case "timezone" -> "UTC";
                default -> "";
            };
            writeRowDescription(out, new String[]{setting});
            PgRowWriter.writeDataRow(out, new String[]{val});
            writeCommandComplete(out, "SHOW");
            return;
        }

        // Many drivers call select current_setting('...')
        if (lower.startsWith("select current_setting")) {
            writeRowDescription(out, new String[]{"current_setting"});
            PgRowWriter.writeDataRow(out, new String[]{""});
            writeCommandComplete(out, "SELECT 1");
            return;
        }

        // Many drivers call select version()
        if (lower.startsWith("select version()")) {
            writeRowDescription(out, new String[]{"version"});
            PgRowWriter.writeDataRow(out, new String[]{"Skadi SQL Gateway (pgwire)"});
            writeCommandComplete(out, "SELECT 1");
            return;
        }

        // --- MVP query support ---
        if (lower.equals("select 1") || lower.equals("select 1;") || lower.equals("select 1 as one") || lower.equals("select 1 as one;")) {
            writeRowDescription(out, new String[]{"?column?"});
            PgRowWriter.writeDataRow(out, new String[]{"1"});
            writeCommandComplete(out, "SELECT 1");
            return;
        }

        // If JDBC execution is configured, try to run the query and stream results.
        SqlExecutorProvider executorProvider = SqlExecutorProviderHolder.get();
        if (executorProvider != null) {
            try {
                streamJdbcQuery(out, executorProvider, s);
                return;
            } catch (Exception e) {
                writeError(out, "XX000", "JDBC execution failed: " + e.getMessage());
                return;
            }
        }

        writeError(out, "0A000", "Query not supported yet (MVP): " + s);
    }

    private static void writeRowSet(DataOutputStream out, MetadataRowSet rs) throws IOException {
        writeRowDescription(out, rs.columns().toArray(new String[0]));
        for (List<String> row : rs.rows()) {
            PgRowWriter.writeDataRow(out, row.toArray(new String[0]));
        }
        writeCommandComplete(out, rs.commandTag());
    }

    private void streamJdbcQuery(DataOutputStream out, SqlExecutorProvider executorProvider, String sql) throws Exception {
        Integer fetchSize = props.fetchSize();
        Integer maxRows = props.maxRows();

        try (Connection conn = executorProvider.getConnection();
             Statement st = conn.createStatement()) {

            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (Exception ignored) {
                }
            }

            if (maxRows != null && maxRows > 0) {
                try {
                    st.setMaxRows(maxRows);
                } catch (Exception ignored) {
                }
            }

            boolean hasResult = st.execute(sql);
            if (!hasResult) {
                writeCommandComplete(out, "OK");
                return;
            }

            try (ResultSet rs = st.getResultSet()) {
                ResultSetMetaData md = rs.getMetaData();
                writeRowDescription(out, md);

                final int colCount = md.getColumnCount();
                long rows = 0;

                // Chunking: flush every N rows to avoid buffering huge payloads in memory.
                final int flushEvery = 256;
                while (rs.next()) {
                    String[] row = new String[colCount];
                    for (int i = 1; i <= colCount; i++) {
                        Object v = rs.getObject(i);
                        if (v == null) {
                            row[i - 1] = null;
                        } else {
                            // Text format: let driver/JVM format; clients parse based on OID.
                            row[i - 1] = v.toString();
                        }
                    }

                    PgRowWriter.writeDataRow(out, row);
                    rows++;

                    if (rows % flushEvery == 0) {
                        out.flush();
                    }
                }

                writeCommandComplete(out, "SELECT " + rows);
            }
        }
    }

    private boolean requiresPassword() {
        SqlGatewayProperties.PgWire.Auth auth = props.auth();
        if (auth == null) return false;
        String mode = auth.mode();
        return mode != null && mode.equalsIgnoreCase("password");
    }

    private boolean isPasswordValid(String user, String password) {
        SqlGatewayProperties.PgWire.Auth auth = props.auth();
        if (auth == null || auth.users() == null) return false;
        String expected = auth.users().get(user);
        return expected != null && expected.equals(password);
    }

    private static Map<String, String> readStartupParams(ByteBuffer buf) {
        Map<String, String> params = new HashMap<>();
        while (buf.hasRemaining()) {
            String k = readCString(buf);
            if (k.isEmpty()) break;
            String v = readCString(buf);
            params.put(k, v);
        }
        return params;
    }

    private static String readCString(ByteBuffer buf) {
        int start = buf.position();
        while (buf.hasRemaining()) {
            if (buf.get() == 0) {
                int end = buf.position() - 1;
                int len = end - start;
                byte[] b = new byte[len];
                buf.position(start);
                buf.get(b);
                buf.get(); // consume null
                return new String(b, StandardCharsets.UTF_8);
            }
        }
        // malformed: no terminator
        int end = buf.position();
        int len = end - start;
        byte[] b = new byte[len];
        buf.position(start);
        buf.get(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static String cstring(byte[] bytes, int offset) {
        int i = offset;
        while (i < bytes.length && bytes[i] != 0) i++;
        return new String(bytes, offset, i - offset, StandardCharsets.UTF_8);
    }

    // --- Server message writers ---

    private static void writeAuthOk(DataOutputStream out) throws IOException {
        out.writeByte('R');
        out.writeInt(8);
        out.writeInt(0);
    }

    private static void writeAuthCleartext(DataOutputStream out) throws IOException {
        out.writeByte('R');
        out.writeInt(8);
        out.writeInt(3);
    }

    private static void writeParameterStatus(DataOutputStream out, String key, String value) throws IOException {
        byte[] k = (key + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] v = (value + "\0").getBytes(StandardCharsets.UTF_8);
        out.writeByte('S');
        out.writeInt(4 + k.length + v.length);
        out.write(k);
        out.write(v);
    }

    private static void writeBackendKeyData(DataOutputStream out, int pid, int secretKey) throws IOException {
        out.writeByte('K');
        out.writeInt(12);
        out.writeInt(pid);
        out.writeInt(secretKey);
    }

    private static void writeReady(DataOutputStream out) throws IOException {
        out.writeByte('Z');
        out.writeInt(5);
        out.writeByte('I'); // idle
    }

    private static void writeEmptyQueryResponse(DataOutputStream out) throws IOException {
        out.writeByte('I');
        out.writeInt(4);
    }

    private static void writeParseComplete(DataOutputStream out) throws IOException {
        out.writeByte('1');
        out.writeInt(4);
    }

    private static void writeBindComplete(DataOutputStream out) throws IOException {
        out.writeByte('2');
        out.writeInt(4);
    }

    private static void writeCloseComplete(DataOutputStream out) throws IOException {
        out.writeByte('3');
        out.writeInt(4);
    }

    private static void writeNoData(DataOutputStream out) throws IOException {
        out.writeByte('n');
        out.writeInt(4);
    }

    private static void writeCommandComplete(DataOutputStream out, String tag) throws IOException {
        byte[] t = (tag + "\0").getBytes(StandardCharsets.UTF_8);
        out.writeByte('C');
        out.writeInt(4 + t.length);
        out.write(t);
    }

    private static void writeRowDescription(DataOutputStream out, String[] columns) throws IOException {
        int fieldCount = columns.length;
        ByteBuffer b = ByteBuffer.allocate(1024).order(ByteOrder.BIG_ENDIAN);
        b.putShort((short) fieldCount);
        for (String col : columns) {
            putCString(b, col);
            b.putInt(0); // table oid
            b.putShort((short) 0); // attr #
            b.putInt(PgType.TEXT); // type oid TEXT
            b.putShort((short) -1); // size
            b.putInt(0); // type modifier
            b.putShort((short) 0); // format code 0=text
        }
        int msgLen = b.position();
        out.writeByte('T');
        out.writeInt(4 + msgLen);
        out.write(b.array(), 0, msgLen);
    }

    private static void writeRowDescription(DataOutputStream out, ResultSetMetaData md) throws IOException {
        try {
            int fieldCount = md.getColumnCount();
            ByteBuffer b = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN);
            b.putShort((short) fieldCount);
            for (int i = 1; i <= fieldCount; i++) {
                String col = md.getColumnLabel(i);
                putCString(b, col == null ? ("col_" + i) : col);
                b.putInt(0); // table oid
                b.putShort((short) 0); // attr #

                int oid = JdbcToPgTypeMapper.toPgOid(md, i);
                b.putInt(oid);

                b.putShort((short) -1); // size (unknown / variable)

                int typmod = 0;
                if (oid == PgType.NUMERIC) {
                    typmod = JdbcToPgTypeMapper.numericTypmod(md.getPrecision(i), md.getScale(i));
                }
                b.putInt(typmod);

                b.putShort((short) 0); // format code 0=text

                if (b.remaining() < 256) {
                    // grow buffer if needed
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
        } catch (Exception e) {
            throw new IOException("Failed to write RowDescription", e);
        }
    }

    private static void writeError(DataOutputStream out, String sqlState, String message) throws IOException {
        byte[] severity = ("ERROR\0").getBytes(StandardCharsets.UTF_8);
        byte[] code = (sqlState + "\0").getBytes(StandardCharsets.UTF_8);
        byte[] msg = (message + "\0").getBytes(StandardCharsets.UTF_8);

        int payloadLen = 1 + severity.length + 1 + code.length + 1 + msg.length + 1;
        out.writeByte('E');
        out.writeInt(4 + payloadLen);
        out.writeByte('S');
        out.write(severity);
        out.writeByte('C');
        out.write(code);
        out.writeByte('M');
        out.write(msg);
        out.writeByte(0);
    }

    private static void putCString(ByteBuffer b, String s) {
        b.put(s.getBytes(StandardCharsets.UTF_8));
        b.put((byte) 0);
    }

    private static void writeParameterDescription(DataOutputStream out, int[] paramTypeOids) throws IOException {
        out.writeByte('t');
        out.writeInt(4 + 2 + (paramTypeOids == null ? 0 : (4 * paramTypeOids.length)));
        short count = (short) (paramTypeOids == null ? 0 : paramTypeOids.length);
        out.writeShort(count);
        if (paramTypeOids != null) {
            for (int oid : paramTypeOids) {
                out.writeInt(oid);
            }
        }
    }
}
