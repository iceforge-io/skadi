package org.iceforge.skadi.sqlgateway.mysqwire;

import org.iceforge.skadi.sqlgateway.auth.AuthProvider;
import org.iceforge.skadi.sqlgateway.auth.AuthProviderFactory;
import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridge;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridgeOptions;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridgeResult;
import org.iceforge.skadi.sqlgateway.metadata.MetadataCache;
import org.iceforge.skadi.sqlgateway.metadata.MetadataQueryRouter;
import org.iceforge.skadi.sqlgateway.metadata.MetadataRowSet;
import org.iceforge.skadi.sqlgateway.pgwire.SqlExecutorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handles one MySQL wire-protocol client connection.
 *
 * <p>Protocol scope: Handshake v10, mysql_native_password auth, COM_QUERY text protocol.
 * Prepared statements (COM_STMT_*) are not implemented — this covers the DBeaver / Workbench
 * "simple" connection mode and any client that uses the text protocol.
 */
final class MySqlWireSession implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MySqlWireSession.class);

    private static final MetadataCache METADATA_CACHE = new MetadataCache(Clock.systemUTC());

    // MySQL capability flags
    private static final int CAP_LONG_PASSWORD       = 0x00000001;
    private static final int CAP_LONG_FLAG           = 0x00000004;
    private static final int CAP_CONNECT_WITH_DB     = 0x00000008;
    private static final int CAP_PROTOCOL_41         = 0x00000200;
    private static final int CAP_TRANSACTIONS        = 0x00002000;
    private static final int CAP_SECURE_CONNECTION   = 0x00008000;
    private static final int CAP_MULTI_RESULTS       = 0x00020000;
    private static final int CAP_PLUGIN_AUTH         = 0x00080000;
    private static final int SERVER_CAPS =
            CAP_LONG_PASSWORD | CAP_LONG_FLAG | CAP_CONNECT_WITH_DB |
            CAP_PROTOCOL_41 | CAP_TRANSACTIONS | CAP_SECURE_CONNECTION |
            CAP_MULTI_RESULTS | CAP_PLUGIN_AUTH;

    private static final int STATUS_AUTOCOMMIT       = 0x0002;
    private static final byte CHARSET_UTF8           = 0x21;

    // MySQL command bytes
    private static final int COM_QUIT    = 0x01;
    private static final int COM_INIT_DB = 0x02;
    private static final int COM_QUERY   = 0x03;
    private static final int COM_PING    = 0x0e;

    private static final String AUTH_PLUGIN = "mysql_native_password";
    private static final String SERVER_VERSION = "8.0.32-skadi";

    private static final AtomicInteger ID_SEQ = new AtomicInteger(1);

    private final Socket socket;
    private final SqlGatewayProperties.MySqlWire props;
    private final SqlGatewayProperties.Metadata metadataProps;
    private final AuthProvider authProvider;
    private final String sessionId;
    private final int connectionId;
    private final MetadataQueryRouter metadata;

    MySqlWireSession(Socket socket,
                     SqlGatewayProperties.MySqlWire props,
                     SqlGatewayProperties.Metadata metadataProps) {
        this.socket = socket;
        this.props = props;
        this.metadataProps = metadataProps;
        this.authProvider = AuthProviderFactory.create(props.auth());
        this.sessionId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        this.connectionId = ID_SEQ.getAndIncrement();

        Duration metaTtl = (metadataProps != null && metadataProps.ttl() != null)
                ? metadataProps.ttl() : Duration.ofMinutes(2);
        String pgDb = nonBlank(metadataProps != null ? metadataProps.pgDatabase() : null, "mysql");
        String catalog = nonBlank(metadataProps != null ? metadataProps.dbxCatalog() : null, "main");
        String schema = nonBlank(metadataProps != null ? metadataProps.dbxSchema() : null, "public");
        this.metadata = new MetadataQueryRouter(METADATA_CACHE, metaTtl, pgDb, catalog, schema);
    }

    @Override
    public void run() {
        MDC.put("session_id", sessionId);
        MDC.put("client", socket.getRemoteSocketAddress().toString());
        try (socket;
             DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {

            byte[] challenge = randomBytes(20);
            sendGreeting(out, challenge);

            String user = authenticate(in, out, challenge);
            if (user == null) return; // auth failed; error already sent

            log.info("mysql session authenticated user={}", user);
            commandLoop(in, out, user);

        } catch (EOFException e) {
            log.debug("mysql client disconnected");
        } catch (IOException e) {
            log.debug("mysql session I/O error: {}", e.getMessage());
        } catch (Exception e) {
            log.warn("mysql session error", e);
        } finally {
            MDC.clear();
        }
    }

    // ── Handshake ─────────────────────────────────────────────────────────────

    private void sendGreeting(DataOutputStream out, byte[] challenge) throws IOException {
        ByteArrayOutputStream body = new ByteArrayOutputStream();

        // Protocol version 10
        body.write(10);
        // Server version
        writeNullString(body, SERVER_VERSION);
        // Connection ID (4 bytes LE)
        writeInt4LE(body, connectionId);
        // Auth plugin data part 1 (first 8 bytes of challenge)
        body.write(challenge, 0, 8);
        // Filler
        body.write(0);
        // Capability flags (lower 2 bytes)
        writeInt2LE(body, SERVER_CAPS & 0xFFFF);
        // Character set
        body.write(CHARSET_UTF8);
        // Status flags
        writeInt2LE(body, STATUS_AUTOCOMMIT);
        // Capability flags (upper 2 bytes)
        writeInt2LE(body, (SERVER_CAPS >> 16) & 0xFFFF);
        // Auth plugin data length = 21 (total challenge length + 1)
        body.write(21);
        // Reserved (10 zeros)
        body.write(new byte[10]);
        // Auth plugin data part 2 (remaining 12 bytes + 1 null)
        body.write(challenge, 8, 12);
        body.write(0);
        // Auth plugin name
        writeNullString(body, AUTH_PLUGIN);

        writePacket(out, 0, body.toByteArray());
        out.flush();
    }

    /** Returns the authenticated username, or null if auth failed (error already written). */
    private String authenticate(DataInputStream in, DataOutputStream out, byte[] challenge)
            throws Exception {
        byte[] pktData = readPacket(in); // client handshake response, seq=1

        int offset = 0;
        int clientCaps = readInt4LE(pktData, offset); offset += 4;
        /* maxPacketSize = */ readInt4LE(pktData, offset); offset += 4;
        @SuppressWarnings("unused") int charset = pktData[offset++] & 0xFF;
        offset += 23; // reserved

        String username = readNullString(pktData, offset);
        offset += username.length() + 1;

        // Auth response: length-encoded
        int authLen = pktData[offset++] & 0xFF;
        byte[] authResponse = Arrays.copyOfRange(pktData, offset, offset + authLen);
        offset += authLen;

        boolean trusted = "trust".equalsIgnoreCase(props.auth() == null ? "trust" : props.auth().mode());
        boolean ok;
        if (trusted) {
            ok = true;
        } else {
            String stored = (props.auth().users() == null) ? null : props.auth().users().get(username);
            if (stored == null) {
                ok = false;
            } else {
                ok = verifyNativePassword(challenge, stored.getBytes(StandardCharsets.UTF_8), authResponse);
            }
        }

        if (!ok) {
            log.warn("mysql auth failed for user={}", username);
            sendError(out, 2, 1045, "28000", "Access denied for user '" + username + "'");
            return null;
        }

        sendOk(out, 2);
        return username;
    }

    // ── Command loop ──────────────────────────────────────────────────────────

    private void commandLoop(DataInputStream in, DataOutputStream out, String user) throws Exception {
        while (true) {
            byte[] pktData = readPacket(in);
            if (pktData.length == 0) {
                sendOk(out, 1);
                continue;
            }

            int cmd = pktData[0] & 0xFF;
            switch (cmd) {
                case COM_QUIT -> { return; }
                case COM_PING -> sendOk(out, 1);
                case COM_INIT_DB -> {
                    // USE <db> — acknowledge, don't change routing
                    sendOk(out, 1);
                }
                case COM_QUERY -> {
                    String sql = new String(pktData, 1, pktData.length - 1, StandardCharsets.UTF_8).trim();
                    handleQuery(out, sql, user);
                }
                default -> sendError(out, 1, 1047, "08S01",
                        "Command 0x%02x not supported".formatted(cmd));
            }
        }
    }

    private void handleQuery(DataOutputStream out, String sql, String user) throws Exception {
        log.debug("mysql COM_QUERY user={} sql_len={}", user, sql.length());

        // Metadata facade intercept
        var metaOpt = metadata.tryAnswer(sql);
        if (metaOpt.isPresent()) {
            sendMetadataResultSet(out, metaOpt.get());
            return;
        }

        // Dialect bridge (MySQL → Databricks)
        SqlDialectBridge bridge = new SqlDialectBridge(SqlDialectBridgeOptions.defaultForMySqlWire());
        SqlDialectBridgeResult bridged = bridge.bridge(sql, List.of());

        SqlExecutorProvider provider = MySqlExecutorProviderHolder.get();
        if (provider == null) {
            sendError(out, 1, 1105, "HY000", "No backend executor configured");
            return;
        }

        try (Connection conn = provider.getConnection();
             Statement stmt = conn.createStatement()) {

            Duration timeout = props.effectiveQueryTimeout();
            if (timeout != null) stmt.setQueryTimeout((int) timeout.toSeconds());

            boolean hasResultSet = stmt.execute(bridged.translatedSql());
            if (hasResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    sendResultSet(out, rs);
                }
            } else {
                sendOk(out, 1);
            }
        } catch (Exception e) {
            log.warn("mysql query error: {}", e.getMessage());
            sendError(out, 1, 1105, "HY000", e.getMessage() != null ? e.getMessage() : "Query error");
        }
    }

    // ── Result set serialisation ──────────────────────────────────────────────

    private void sendMetadataResultSet(DataOutputStream out, MetadataRowSet rs) throws IOException {
        List<String> cols = rs.columns();
        List<List<String>> rows = rs.rows();

        int seq = 1;

        // Column count
        writePacket(out, seq++, encodeLengthInt(cols.size()));

        // Column definitions
        for (String col : cols) {
            writePacket(out, seq++, encodeColumnDef(col, JdbcToMySqlTypeMapper.MYSQL_TYPE_VAR_STRING, 0, 255));
        }

        // EOF after column defs
        writePacket(out, seq++, eofPacket());

        // Rows
        for (List<String> row : rows) {
            ByteArrayOutputStream rowBuf = new ByteArrayOutputStream();
            for (String val : row) {
                if (val == null) {
                    rowBuf.write(0xfb); // NULL marker
                } else {
                    byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
                    writeLengthEncodedBytes(rowBuf, bytes);
                }
            }
            writePacket(out, seq++, rowBuf.toByteArray());
        }

        // EOF at end
        writePacket(out, seq, eofPacket());
        out.flush();
    }

    private void sendResultSet(DataOutputStream out, ResultSet rs) throws Exception {
        ResultSetMetaData md = rs.getMetaData();
        int colCount = md.getColumnCount();

        int seq = 1;

        // Column count
        writePacket(out, seq++, encodeLengthInt(colCount));

        // Column definitions
        for (int i = 1; i <= colCount; i++) {
            int jdbcType = md.getColumnType(i);
            int mysqlType = JdbcToMySqlTypeMapper.toMySqlType(jdbcType);
            int flags = JdbcToMySqlTypeMapper.flagsFor(jdbcType);
            int precision = md.getPrecision(i);
            int displayLen = JdbcToMySqlTypeMapper.displayLen(jdbcType, precision);
            writePacket(out, seq++, encodeColumnDef(md.getColumnLabel(i), mysqlType, flags, displayLen));
        }

        // EOF after column defs
        writePacket(out, seq++, eofPacket());

        // Rows
        while (rs.next()) {
            ByteArrayOutputStream rowBuf = new ByteArrayOutputStream();
            for (int i = 1; i <= colCount; i++) {
                Object val = rs.getObject(i);
                if (val == null || rs.wasNull()) {
                    rowBuf.write(0xfb);
                } else {
                    byte[] bytes = renderValue(val, md.getColumnType(i)).getBytes(StandardCharsets.UTF_8);
                    writeLengthEncodedBytes(rowBuf, bytes);
                }
            }
            writePacket(out, seq++, rowBuf.toByteArray());
        }

        // EOF at end
        writePacket(out, seq, eofPacket());
        out.flush();
    }

    private static String renderValue(Object val, int jdbcType) {
        if (val instanceof java.sql.Timestamp ts) {
            return ts.toLocalDateTime().toString().replace('T', ' ');
        }
        if (val instanceof java.sql.Date d) {
            return d.toLocalDate().toString();
        }
        return val.toString();
    }

    // ── Packet encoding helpers ───────────────────────────────────────────────

    private static byte[] encodeColumnDef(String name, int mysqlType, int flags, int displayLen) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        try {
            writeNullString(b, "def");   // catalog
            writeNullString(b, "");      // schema
            writeNullString(b, "");      // table
            writeNullString(b, "");      // org_table
            writeNullString(b, name);    // name
            writeNullString(b, name);    // org_name
            b.write(0x0c);               // fixed-length fields marker
            writeInt2LE(b, CHARSET_UTF8 & 0xFF); // character set
            writeInt4LE(b, displayLen);  // column length
            b.write(mysqlType);          // type
            writeInt2LE(b, flags);       // flags
            b.write(0);                  // decimals
            b.write(0); b.write(0);      // filler
        } catch (IOException ignored) { /* ByteArrayOutputStream never throws */ }
        return b.toByteArray();
    }

    private static byte[] eofPacket() {
        // 0xfe + 2 bytes warnings + 2 bytes status
        return new byte[] { (byte) 0xfe, 0x00, 0x00, 0x02, 0x00 };
    }

    private static byte[] encodeLengthInt(int val) {
        if (val < 251) return new byte[] { (byte) val };
        if (val < 65536) return new byte[] { (byte) 0xfc, (byte) val, (byte) (val >> 8) };
        return new byte[] { (byte) 0xfd, (byte) val, (byte) (val >> 8), (byte) (val >> 16) };
    }

    private static void writeLengthEncodedBytes(OutputStream out, byte[] bytes) throws IOException {
        byte[] lenPfx = encodeLengthInt(bytes.length);
        out.write(lenPfx);
        out.write(bytes);
    }

    private void sendOk(DataOutputStream out, int seq) throws IOException {
        // OK: 0x00, affected_rows=0, last_insert_id=0, status, warnings
        writePacket(out, seq, new byte[] { 0x00, 0x00, 0x00, 0x02, 0x00 });
        out.flush();
    }

    private void sendError(DataOutputStream out, int seq, int code, String sqlState, String msg) throws IOException {
        byte[] msgBytes = msg.getBytes(StandardCharsets.UTF_8);
        byte[] stateBytes = sqlState.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        b.write(0xff);
        writeInt2LE(b, code);
        b.write('#');
        b.write(stateBytes, 0, Math.min(5, stateBytes.length));
        b.write(msgBytes);
        writePacket(out, seq, b.toByteArray());
        out.flush();
    }

    // ── Packet framing ────────────────────────────────────────────────────────

    private static void writePacket(DataOutputStream out, int seq, byte[] payload) throws IOException {
        int len = payload.length;
        out.write(len & 0xFF);
        out.write((len >> 8) & 0xFF);
        out.write((len >> 16) & 0xFF);
        out.write(seq & 0xFF);
        out.write(payload);
    }

    /** Reads one MySQL packet, returns the payload bytes. */
    private static byte[] readPacket(DataInputStream in) throws IOException {
        int b0 = in.read();
        if (b0 < 0) throw new EOFException();
        int b1 = in.read();
        int b2 = in.read();
        /* seq = */ in.read();
        int len = b0 | (b1 << 8) | (b2 << 16);
        if (len == 0) return new byte[0];
        return in.readNBytes(len);
    }

    // ── Auth helpers ──────────────────────────────────────────────────────────

    /**
     * Verifies a mysql_native_password auth token.
     *
     * <p>Client sends: SHA1(password) XOR SHA1(challenge + SHA1(SHA1(password))).
     * We reconstruct the expected token from the stored plaintext password.
     */
    static boolean verifyNativePassword(byte[] challenge, byte[] storedPasswordBytes, byte[] token) {
        if (token == null || token.length == 0) return false;
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1pwd = sha1.digest(storedPasswordBytes);
            sha1.reset();
            byte[] sha1sha1pwd = sha1.digest(sha1pwd);

            sha1.reset();
            sha1.update(challenge);
            sha1.update(sha1sha1pwd);
            byte[] xorMask = sha1.digest();

            byte[] expected = new byte[20];
            for (int i = 0; i < 20; i++) {
                expected[i] = (byte) (sha1pwd[i] ^ xorMask[i]);
            }
            return Arrays.equals(expected, Arrays.copyOf(token, 20));
        } catch (Exception e) {
            return false;
        }
    }

    // ── Low-level I/O helpers ─────────────────────────────────────────────────

    private static byte[] randomBytes(int len) {
        byte[] b = new byte[len];
        ThreadLocalRandom.current().nextBytes(b);
        // Avoid null bytes in the challenge (mysql_native_password uses null-term semantics)
        for (int i = 0; i < len; i++) {
            if (b[i] == 0) b[i] = 1;
        }
        return b;
    }

    private static void writeNullString(OutputStream out, String s) throws IOException {
        out.write(s.getBytes(StandardCharsets.UTF_8));
        out.write(0);
    }

    private static void writeInt2LE(OutputStream out, int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >> 8) & 0xFF);
    }

    private static void writeInt4LE(OutputStream out, int v) throws IOException {
        out.write(v & 0xFF);
        out.write((v >> 8) & 0xFF);
        out.write((v >> 16) & 0xFF);
        out.write((v >> 24) & 0xFF);
    }

    private static int readInt4LE(byte[] b, int offset) {
        return (b[offset] & 0xFF) |
               ((b[offset + 1] & 0xFF) << 8) |
               ((b[offset + 2] & 0xFF) << 16) |
               ((b[offset + 3] & 0xFF) << 24);
    }

    private static String readNullString(byte[] b, int offset) {
        int end = offset;
        while (end < b.length && b[end] != 0) end++;
        return new String(b, offset, end - offset, StandardCharsets.UTF_8);
    }

    private static String nonBlank(String value, String defaultValue) {
        return (value == null || value.isBlank()) ? defaultValue : value;
    }
}
