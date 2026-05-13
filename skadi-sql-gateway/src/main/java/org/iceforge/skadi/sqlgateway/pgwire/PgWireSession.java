package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.auth.AuthProvider;
import org.iceforge.skadi.sqlgateway.auth.AuthProviderFactory;
import org.iceforge.skadi.sqlgateway.auth.PrincipalPolicy;
import org.iceforge.skadi.sqlgateway.auth.PrincipalPolicyRegistry;
import org.iceforge.skadi.sqlgateway.cache.QueryCacheKey;
import org.iceforge.skadi.sqlgateway.cache.QueryCacheMetrics;
import org.iceforge.skadi.sqlgateway.cache.QueryResultCache;
import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridge;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridgeOptions;
import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.iceforge.skadi.sqlgateway.metrics.GatewayMetricsHolder;
import org.iceforge.skadi.sqlgateway.metadata.MetadataCache;
import org.iceforge.skadi.sqlgateway.metadata.MetadataQueryRouter;
import org.iceforge.skadi.sqlgateway.metadata.MetadataRowSet;
import org.iceforge.skadi.sqlgateway.trace.TableauTraceLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

final class PgWireSession implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(PgWireSession.class);

    private static final MetadataCache METADATA_CACHE = new MetadataCache(Clock.systemUTC());
    private static final QueryResultCache QUERY_CACHE = new QueryResultCache(Clock.systemUTC(), 500);
    private static final QueryCacheMetrics CACHE_METRICS = new QueryCacheMetrics();

    private final Socket socket;
    private final SqlGatewayProperties.PgWire props;
    private final SqlGatewayProperties.Cache cacheProps;
    private final SqlGatewayProperties.Trace traceProps;
    private final AuthProvider authProvider;
    private final PrincipalPolicyRegistry policyRegistry;
    private final String sessionId;
    private final TableauTraceLogger tracer;
    private final MetadataQueryRouter metadata;
    private final SessionRegistry registry; // nullable
    private final QueryGovernor governor;   // nullable
    private final int pid;
    private final int secretKey;
    private final String dbxSchema; // for SHOW search_path

    // Per-query cancel signal: set by cancelActiveQuery(), reset before each execution.
    private final AtomicBoolean cancelFlag = new AtomicBoolean(false);

    // Authenticated username and resolved policy, captured at startup.
    private String user = "";
    private PrincipalPolicy policy = PrincipalPolicy.UNRESTRICTED;

    // Minimal extended-query state.
    private String lastPreparedSql;
    private String lastStatementName;
    private String lastPortalName;
    private boolean portalHasResult;
    // Param type OIDs declared in Parse ('P'); 0 = unspecified.
    private int[] lastParamTypeOids = new int[0];
    // Bound parameter values from the most recent Bind ('B') message.
    private List<SqlParam> lastBoundParams = List.of();

    // Active JDBC Statement; stored so a CancelRequest from another connection can interrupt it.
    private volatile Statement activeStatement;

    PgWireSession(Socket socket, SqlGatewayProperties.PgWire props, SqlGatewayProperties.Cache cacheProps,
                  SqlGatewayProperties.Trace traceProps) {
        this(socket, props, cacheProps, traceProps, null, null, null);
    }

    PgWireSession(Socket socket, SqlGatewayProperties.PgWire props, SqlGatewayProperties.Cache cacheProps,
                  SqlGatewayProperties.Trace traceProps,
                  SqlGatewayProperties.Metadata metadataProps,
                  SessionRegistry registry) {
        this(socket, props, cacheProps, traceProps, metadataProps, registry, null);
    }

    PgWireSession(Socket socket, SqlGatewayProperties.PgWire props, SqlGatewayProperties.Cache cacheProps,
                  SqlGatewayProperties.Trace traceProps,
                  SqlGatewayProperties.Metadata metadataProps,
                  SessionRegistry registry,
                  QueryGovernor governor) {
        this.socket = Objects.requireNonNull(socket);
        this.props = Objects.requireNonNull(props);
        this.cacheProps = cacheProps;   // nullable
        this.traceProps = traceProps;   // nullable
        this.registry = registry;       // nullable
        this.governor = governor;       // nullable
        this.authProvider = AuthProviderFactory.create(props.auth());
        this.policyRegistry = new PrincipalPolicyRegistry(props.auth());
        this.sessionId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        this.pid = SessionRegistry.allocatePid();
        this.secretKey = ThreadLocalRandom.current().nextInt();
        boolean te = traceProps != null && traceProps.isEnabled();
        String tdPath = (traceProps != null) ? traceProps.testdataPath() : null;
        this.tracer = new TableauTraceLogger(sessionId, te, tdPath);

        // Metadata facade: use config values if available, otherwise fall back to defaults.
        Duration metaTtl = (metadataProps != null && metadataProps.ttl() != null)
                ? metadataProps.ttl() : Duration.ofMinutes(2);
        String pgDb = nonBlank(metadataProps != null ? metadataProps.pgDatabase() : null, "postgres");
        String catalog = nonBlank(metadataProps != null ? metadataProps.dbxCatalog() : null, "main");
        String schema = nonBlank(metadataProps != null ? metadataProps.dbxSchema() : null, "public");
        this.dbxSchema = schema;
        this.metadata = new MetadataQueryRouter(METADATA_CACHE, metaTtl, pgDb, catalog, schema);
    }

    private static String nonBlank(String value, String defaultValue) {
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    /** Called from a different thread when a CancelRequest targets this session. */
    void cancelActiveQuery() {
        cancelFlag.set(true);
        Statement s = activeStatement;
        if (s != null) {
            try {
                s.cancel();
            } catch (SQLException ignored) {
                // Best-effort; driver may not support Statement.cancel()
            }
        }
    }

    @Override
    public void run() {
        MDC.put("session_id", sessionId);
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

            if (code == 80877102) { // CancelRequest: int32(pid) + int32(secretKey)
                int cancelPid = buf.getInt();
                int cancelKey = buf.getInt();
                if (registry != null) registry.cancel(cancelPid, cancelKey);
                return; // close connection; per protocol no response is sent
            }

            if (code != 196608) { // protocol 3.0
                writeError(out, "08000", "Unsupported protocol");
                writeReady(out);
                out.flush();
                return;
            }

            Map<String, String> params = readStartupParams(buf);
            this.user = params.getOrDefault("user", "");

            if (authProvider.requiresPassword()) {
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

                if (!authProvider.authenticate(user, password)) {
                    writeError(out, "28P01", "password authentication failed");
                    writeReady(out);
                    out.flush();
                    return;
                }
            }

            // Resolve authorization policy for this principal.
            this.policy = policyRegistry.policyFor(user);

            String client = deriveClient(params);
            MDC.put("client", client);
            tracer.sessionStart(params, client);

            GatewayMetricsHolder.sessionOpened();
            writeAuthOk(out);
            writeParameterStatus(out, "server_version", "15.0");
            writeParameterStatus(out, "client_encoding", "UTF8");
            writeParameterStatus(out, "DateStyle", "ISO, MDY");
            writeParameterStatus(out, "standard_conforming_strings", "on");
            writeParameterStatus(out, "TimeZone", "UTC");
            writeParameterStatus(out, "integer_datetimes", "on");
            writeParameterStatus(out, "IntervalStyle", "postgres");
            writeBackendKeyData(out, pid, secretKey);
            writeReady(out);
            out.flush();

            if (registry != null) registry.register(pid, secretKey, this);

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
                    this.lastBoundParams = List.of(); // reset on new statement
                    // Read declared parameter type OIDs: int16 count + int32[] OIDs (0 = unspecified).
                    if (mb.remaining() >= 2) {
                        int count = mb.getShort() & 0xFFFF;
                        int[] oids = new int[count];
                        for (int i = 0; i < count && mb.remaining() >= 4; i++) oids[i] = mb.getInt();
                        this.lastParamTypeOids = oids;
                    } else {
                        this.lastParamTypeOids = new int[0];
                    }
                    tracer.statementParsed(sql);
                    writeParseComplete(out);
                    out.flush();
                    continue;
                }

                if (type == 'B') { // Bind
                    ByteBuffer mb = ByteBuffer.wrap(msg).order(ByteOrder.BIG_ENDIAN);
                    this.lastPortalName = readCString(mb);
                    readCString(mb); // statement name (ignored, we only keep last)
                    this.portalHasResult = guessHasResult(this.lastPreparedSql);
                    this.lastBoundParams = parseBindParams(mb);
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
                        // Emit accurate param count (OIDs reported as 0 = unspecified).
                        writeParameterDescription(out, new int[lastParamTypeOids.length]);
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
        } finally {
            if (registry != null) registry.deregister(pid, secretKey);
            GatewayMetricsHolder.sessionClosed();
            tracer.sessionEnd();
            MDC.clear();
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

        // Forward bound parameters from the most recent Bind message.
        handleSimpleQuery(out, s, this.lastBoundParams);
    }

    private static boolean guessHasResult(String sql) {
        if (sql == null) return false;
        String lower = sql.trim().toLowerCase(Locale.ROOT);
        return lower.startsWith("select") || lower.startsWith("show") || lower.startsWith("with") || lower.startsWith("values");
    }

    /** Simple Query ('Q') path — always zero parameters. */
    private void handleSimpleQuery(DataOutputStream out, String sql) throws IOException {
        handleSimpleQuery(out, sql, List.of());
    }

    private void handleSimpleQuery(DataOutputStream out, String sql, List<SqlParam> params) throws IOException {
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
                case "client_encoding"             -> "UTF8";
                case "datestyle"                   -> "ISO, MDY";
                case "timezone"                    -> "UTC";
                case "server_version"              -> "15.0";
                case "server_version_num"          -> "150000";
                case "transaction_isolation"       -> "read committed";
                case "search_path"                 -> dbxSchema;
                case "max_connections"             -> "100";
                case "integer_datetimes"           -> "on";
                case "intervalstyle"               -> "postgres";
                case "application_name"            -> "";
                case "in_hot_standby"              -> "off";
                default                            -> "";
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
            long queryT0 = System.currentTimeMillis();
            try {
                streamJdbcQueryWithCaching(out, executorProvider, s, params);
                return;
            } catch (Exception e) {
                long errorLatency = System.currentTimeMillis() - queryT0;
                tracer.queryError(s, "XX000", e.getMessage());
                GatewayMetricsHolder.recordQueryError(errorLatency, "XX000");
                writeError(out, "XX000", "JDBC execution failed: " + e.getMessage());
                return;
            }
        }

        writeError(out, "0A000", "Query not supported yet (MVP): " + s);
    }

    private void writeRowSet(DataOutputStream out, MetadataRowSet rs) throws IOException {
        List<List<String>> rows = applyPolicyFilter(rs);
        writeRowDescription(out, rs.columns().toArray(new String[0]));
        for (List<String> row : rows) {
            PgRowWriter.writeDataRow(out, row.toArray(new String[0]));
        }
        writeCommandComplete(out, "SELECT " + rows.size());
    }

    /**
     * Filters metadata rowset rows to those permitted by the session's principal policy.
     * Looks for a column named "table_schema" or "schema_name" and removes rows whose
     * schema value is not in the allowed set.
     */
    private List<List<String>> applyPolicyFilter(MetadataRowSet rs) {
        if (policy.isUnrestricted()) return rs.rows();
        List<String> cols = rs.columns();
        int schemaIdx = -1;
        for (int i = 0; i < cols.size(); i++) {
            String c = cols.get(i).toLowerCase(Locale.ROOT);
            if (c.equals("table_schema") || c.equals("schema_name")) {
                schemaIdx = i;
                break;
            }
        }
        if (schemaIdx < 0) return rs.rows(); // no schema column — can't filter
        final int idx = schemaIdx;
        return rs.rows().stream()
                .filter(row -> row.size() > idx && policy.permitsSchema(row.get(idx)))
                .toList();
    }

    private void streamJdbcQueryWithCaching(DataOutputStream out, SqlExecutorProvider executorProvider,
                                            String sql, List<SqlParam> params) throws Exception {
        // Enforce per-user concurrency limit.
        if (governor != null && !governor.tryAcquire(user)) {
            writeError(out, "53300", "too many concurrent queries for user: " + user);
            GatewayMetricsHolder.recordQueryError(0, "53300");
            return;
        }
        // Assign a short query-scoped correlation ID; written into MDC and forwarded to Databricks.
        String queryId = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        MDC.put("query_id", queryId);
        try {
            cancelFlag.set(false);
            streamJdbcQueryWithCachingInner(out, executorProvider, sql, params);
        } finally {
            if (governor != null) governor.release(user);
            MDC.remove("query_id");
        }
    }

    private void streamJdbcQueryWithCachingInner(DataOutputStream out, SqlExecutorProvider executorProvider,
                                                  String sql, List<SqlParam> params) throws Exception {
        // Path 3: Arrow-based caching with SQL dialect translation (preferred when configured).
        PgWireQueryCachingExecutorProvider cachingProvider = PgWireSessionCachingBridge.get();
        if (cachingProvider != null && guessHasResult(sql)) {
            long t0 = System.currentTimeMillis();
            tracer.queryStart(sql);
            var arrowBuf = QueryResultCache.newBuffer();
            BooleanSupplier cancelRequested = cancelFlag::get;
            Duration queryTimeout = props.effectiveQueryTimeout();
            // Pass bound params (L1 fix: was always List.of() before).
            String cacheTag = cachingProvider.executeToStream(sql, params, user, arrowBuf, queryTimeout, cancelRequested);
            long rows = ArrowIpcRowWriter.writeRows(arrowBuf.toByteArray(), out);
            writeCommandComplete(out, "SELECT " + rows);
            long latencyMs = System.currentTimeMillis() - t0;
            tracer.queryEnd(sql, rows, latencyMs, cacheTag);
            GatewayMetricsHolder.recordQuery(latencyMs, cacheTag);
            return;
        }

        // Path 1a: parameterised query — rewrite $n→? via dialect bridge and use PreparedStatement.
        // Caching is skipped here because path-1 cache key does not include parameters.
        if (!params.isEmpty()) {
            tracer.queryStart(sql);
            long t0 = System.currentTimeMillis();
            var bridged = new SqlDialectBridge(SqlDialectBridgeOptions.defaultForPgWire()).bridge(sql, params);
            String preparedSql = bridged.translatedSql();
            List<SqlParam> jdbcParams = bridged.translatedParams();

            try (Connection conn = executorProvider.getConnection();
                 PreparedStatement ps = conn.prepareStatement(preparedSql)) {
                this.activeStatement = ps;
                try {
                    applyConnectionContext(conn);
                    applyStatementLimits(ps);
                    bindParams(ps, jdbcParams);
                    boolean hasResult = ps.execute();
                    if (!hasResult) {
                        long latencyMs = System.currentTimeMillis() - t0;
                        writeCommandComplete(out, "OK");
                        tracer.queryEnd(sql, 0, latencyMs, "SKIP");
                        GatewayMetricsHolder.recordQuery(latencyMs, "SKIP");
                        return;
                    }
                    try (ResultSet rs = ps.getResultSet()) {
                        ResultSetMetaData md = rs.getMetaData();
                        writeRowDescription(out, md);
                        long rows = streamRows(out, rs, md.getColumnCount());
                        writeCommandComplete(out, "SELECT " + rows);
                        long latencyMs = System.currentTimeMillis() - t0;
                        tracer.queryEnd(sql, rows, latencyMs, "SKIP");
                        GatewayMetricsHolder.recordQuery(latencyMs, "SKIP");
                    }
                } finally {
                    this.activeStatement = null;
                }
            }
            return;
        }

        // Path 1b fallback: direct JDBC with text-row caching (no dialect translation, zero params).
        boolean cacheEnabled = cacheProps != null && cacheProps.isEnabled() && guessHasResult(sql);

        if (cacheEnabled) {
            String key = QueryCacheKey.of(sql, user);
            Optional<QueryResultCache.CacheEntry> hit = QUERY_CACHE.get(key);
            if (hit.isPresent()) {
                long t0 = System.currentTimeMillis();
                QueryResultCache.CacheEntry entry = hit.get();
                CACHE_METRICS.recordHit();
                replayFromCache(out, entry);
                long latencyMs = System.currentTimeMillis() - t0;
                tracer.queryEnd(sql, entry.rows().size(), latencyMs, "HIT");
                GatewayMetricsHolder.recordQuery(latencyMs, "HIT");
                return;
            }
            CACHE_METRICS.recordMiss();
        }
        tracer.queryStart(sql);

        long t0 = System.currentTimeMillis();

        try (Connection conn = executorProvider.getConnection();
             Statement st = conn.createStatement()) {

            // Register active statement for cancellation support.
            this.activeStatement = st;

            try {
                applyConnectionContext(conn);
                applyStatementLimits(st);

                boolean hasResult = st.execute(sql);
                if (!hasResult) {
                    long latencyMs = System.currentTimeMillis() - t0;
                    writeCommandComplete(out, "OK");
                    tracer.queryEnd(sql, 0, latencyMs, "MISS");
                    GatewayMetricsHolder.recordQuery(latencyMs, "MISS");
                    return;
                }

                try (ResultSet rs = st.getResultSet()) {
                    ResultSetMetaData md = rs.getMetaData();
                    writeRowDescription(out, md);

                    final int colCount = md.getColumnCount();

                    List<QueryResultCache.ColumnMeta> colMetas = null;
                    List<String[]> bufferedRows = null;
                    if (cacheEnabled) {
                        colMetas = new ArrayList<>(colCount);
                        for (int i = 1; i <= colCount; i++) {
                            String colName = md.getColumnLabel(i);
                            colMetas.add(new QueryResultCache.ColumnMeta(
                                    colName == null ? ("col_" + i) : colName,
                                    JdbcToPgTypeMapper.toPgOid(md, i)));
                        }
                        bufferedRows = new ArrayList<>();
                    }

                    long rows = 0;
                    final int flushEvery = 256;
                    while (rs.next()) {
                        String[] row = new String[colCount];
                        for (int i = 1; i <= colCount; i++) {
                            row[i - 1] = renderJdbcValue(rs.getObject(i));
                        }
                        PgRowWriter.writeDataRow(out, row);
                        rows++;
                        if (bufferedRows != null) bufferedRows.add(row);
                        if (rows % flushEvery == 0) out.flush();
                    }

                    writeCommandComplete(out, "SELECT " + rows);

                    if (cacheEnabled && bufferedRows != null) {
                        String key = QueryCacheKey.of(sql, user);
                        QUERY_CACHE.put(key, colMetas, bufferedRows, cacheProps.effectiveTtl());
                    }
                    long latencyMs = System.currentTimeMillis() - t0;
                    String cacheTag1b = cacheEnabled ? "MISS" : "SKIP";
                    tracer.queryEnd(sql, rows, latencyMs, cacheTag1b);
                    GatewayMetricsHolder.recordQuery(latencyMs, cacheTag1b);
                }
            } finally {
                this.activeStatement = null;
            }
        }
    }

    /** Streams ResultSet rows to the client; returns the row count. */
    private long streamRows(DataOutputStream out, ResultSet rs, int colCount) throws Exception {
        long rows = 0;
        final int flushEvery = 256;
        while (rs.next()) {
            String[] row = new String[colCount];
            for (int i = 1; i <= colCount; i++) {
                row[i - 1] = renderJdbcValue(rs.getObject(i));
            }
            PgRowWriter.writeDataRow(out, row);
            rows++;
            if (rows % flushEvery == 0) out.flush();
        }
        return rows;
    }

    private static final DateTimeFormatter TIMESTAMP_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Renders a JDBC value as a pgwire text-format string.
     *
     * <p>{@link Timestamp#toString()} appends trailing ".0" for whole-second values;
     * rendering via LocalDateTime avoids that and produces a consistent ISO-like format.
     */
    static String renderJdbcValue(Object v) {
        if (v == null) return null;
        if (v instanceof Timestamp ts) {
            return ts.toLocalDateTime().format(TIMESTAMP_FMT);
        }
        return v.toString();
    }

    private void applyConnectionContext(Connection conn) {
        // Include session_id and query_id in the Databricks query history for correlation.
        String queryId = MDC.get("query_id");
        String appName = queryId != null
                ? "skadi/" + sessionId + "/" + queryId
                : "skadi-sql-gateway";
        try { conn.setClientInfo("ApplicationName", appName); } catch (Exception ignored) {}
        try { conn.setClientInfo("User", user); } catch (Exception ignored) {}
    }

    private void applyStatementLimits(Statement st) {
        Integer fetchSize = props.fetchSize();
        Integer maxRows = props.maxRows();
        if (fetchSize != null && fetchSize > 0) {
            try { st.setFetchSize(fetchSize); } catch (Exception ignored) {}
        }
        if (maxRows != null && maxRows > 0) {
            try { st.setMaxRows(maxRows); } catch (Exception ignored) {}
        }
        Duration timeout = props.effectiveQueryTimeout();
        if (timeout != null) {
            try { st.setQueryTimeout((int) Math.max(1, timeout.getSeconds())); } catch (Exception ignored) {}
        }
    }

    private static void bindParams(PreparedStatement ps, List<SqlParam> params) throws Exception {
        for (SqlParam p : params) {
            if (p.value() == null) {
                if (p.jdbcType() != null) ps.setNull(p.index(), p.jdbcType());
                else ps.setObject(p.index(), null);
            } else {
                ps.setObject(p.index(), p.value());
            }
        }
    }

    /**
     * Parses bind parameter values from the remainder of a Bind ('B') message buffer.
     *
     * <p>Format codes: 0 = text (default), 1 = binary. Binary params are decoded as UTF-8
     * text for now — full binary-format decoding is a TODO per protocol spec.
     */
    private static List<SqlParam> parseBindParams(ByteBuffer mb) {
        try {
            if (!mb.hasRemaining()) return List.of();

            int numFormatCodes = mb.getShort() & 0xFFFF;
            int[] formatCodes = new int[numFormatCodes];
            for (int i = 0; i < numFormatCodes; i++) formatCodes[i] = mb.getShort() & 0xFFFF;

            if (!mb.hasRemaining()) return List.of();
            int numParams = mb.getShort() & 0xFFFF;
            if (numParams == 0) return List.of();

            List<SqlParam> result = new ArrayList<>(numParams);
            for (int i = 0; i < numParams; i++) {
                if (mb.remaining() < 4) break;
                int paramLen = mb.getInt();
                if (paramLen == -1) {
                    result.add(new SqlParam(i + 1, null, null)); // SQL NULL
                } else {
                    if (mb.remaining() < paramLen) break;
                    byte[] bytes = new byte[paramLen];
                    mb.get(bytes);
                    // Resolve format: global (1 code), per-param, or default text.
                    int fmt = numFormatCodes == 0 ? 0
                            : numFormatCodes == 1 ? formatCodes[0]
                            : (i < formatCodes.length ? formatCodes[i] : 0);
                    if (fmt == 1) {
                        // TODO: binary-format decoding per type OID; treat as text for now
                        log.warn("Binary-format bind param {} received; falling back to text decode", i + 1);
                    }
                    result.add(new SqlParam(i + 1, null, new String(bytes, StandardCharsets.UTF_8)));
                }
            }
            return result;
        } catch (Exception e) {
            log.warn("Failed to parse Bind message parameters: {}", e.getMessage());
            return List.of();
        }
    }

    private static void replayFromCache(DataOutputStream out, QueryResultCache.CacheEntry entry) throws IOException {
        // Write RowDescription from cached column metadata.
        List<QueryResultCache.ColumnMeta> cols = entry.columns();
        int fieldCount = cols.size();
        ByteBuffer b = ByteBuffer.allocate(4096).order(ByteOrder.BIG_ENDIAN);
        b.putShort((short) fieldCount);
        for (QueryResultCache.ColumnMeta col : cols) {
            putCString(b, col.name());
            b.putInt(0);          // table oid
            b.putShort((short) 0); // attr #
            b.putInt(col.pgOid());
            b.putShort((short) -1); // size
            b.putInt(0);           // type modifier
            b.putShort((short) 0); // format code 0=text
            if (b.remaining() < 256) {
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

        // Write DataRows.
        long rows = 0;
        final int flushEvery = 256;
        for (String[] row : entry.rows()) {
            PgRowWriter.writeDataRow(out, row);
            rows++;
            if (rows % flushEvery == 0) {
                out.flush();
            }
        }

        writeCommandComplete(out, "SELECT " + rows);
    }

    private static String deriveClient(Map<String, String> params) {
        if (params == null) return "unknown";
        String appName = params.getOrDefault("application_name", "");
        if (appName.toLowerCase(Locale.ROOT).contains("tableau")) return "tableau";
        return appName.isBlank() ? "unknown" : appName;
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
