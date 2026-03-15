package org.iceforge.skadi.sqlgateway.trace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Map;

/**
 * Encapsulates structured trace logging for pgwire sessions.
 *
 * <p>In trace mode OFF: only {@code queryEnd} is emitted (at INFO level).
 * In trace mode ON: all lifecycle events are emitted with full SQL text and startup params.
 * When {@code testdataPath} is configured, each {@code queryEnd} is also appended as a
 * JSON-lines record to {@code <testdataPath>/<sessionId>.jsonl} for corpus building.
 */
public final class TableauTraceLogger {

    private static final Logger log = LoggerFactory.getLogger(TableauTraceLogger.class);

    private final String sessionId;
    private final boolean traceEnabled;
    private final Path corpusFile; // null if no testdata path configured

    public TableauTraceLogger(String sessionId, boolean traceEnabled, String testdataPath) {
        this.sessionId = sessionId;
        this.traceEnabled = traceEnabled;
        if (traceEnabled && testdataPath != null && !testdataPath.isBlank()) {
            Path dir = Path.of(testdataPath);
            try {
                Files.createDirectories(dir);
                this.corpusFile = dir.resolve(sessionId + ".jsonl");
            } catch (IOException e) {
                log.warn("trace: could not create testdata directory {}: {}", testdataPath, e.getMessage());
                throw new IllegalStateException("Cannot create trace corpus directory: " + testdataPath, e);
            }
        } else {
            this.corpusFile = null;
        }
    }

    /** Called after authentication succeeds. */
    public void sessionStart(Map<String, String> startupParams, String client) {
        if (!traceEnabled) return;
        if (startupParams != null && !startupParams.isEmpty()) {
            log.info("event=session_start session_id={} client={} params=\"{}\"",
                    sessionId, client, formatParams(startupParams));
        } else {
            log.info("event=session_start session_id={} client={}", sessionId, client);
        }
    }

    /** Called when a Parse ('P') message is received. */
    public void statementParsed(String sql) {
        if (!traceEnabled) return;
        String fp = QueryFingerprint.of(sql);
        log.info("event=parse session_id={} fingerprint={} sql_len={} sql=\"{}\"",
                sessionId, fp, sql == null ? 0 : sql.length(), escape(sql));
    }

    /** Called just before a real query is executed (cache miss or non-cacheable). */
    public void queryStart(String sql) {
        if (!traceEnabled) return;
        String fp = QueryFingerprint.of(sql);
        log.info("event=query_start session_id={} fingerprint={} sql_len={} sql=\"{}\"",
                sessionId, fp, sql == null ? 0 : sql.length(), escape(sql));
    }

    /**
     * Called after a query completes (hit or miss). Always emitted in both trace ON and OFF.
     */
    public void queryEnd(String sql, long rows, long latencyMs, String cacheLocal) {
        String fp = QueryFingerprint.of(sql);
        log.info("event=query_end session_id={} fingerprint={} rows={} latency_ms={} cache_local={} cache_s3=SKIP",
                sessionId, fp, rows, latencyMs, cacheLocal);
        appendCorpus(sql, fp, rows, latencyMs, cacheLocal);
    }

    /** Called when a query fails. */
    public void queryError(String sql, String sqlState, String message) {
        String fp = QueryFingerprint.of(sql);
        if (traceEnabled) {
            log.info("event=query_error session_id={} fingerprint={} error_code={} error_msg=\"{}\" sql=\"{}\"",
                    sessionId, fp, sqlState, escape(message), escape(sql));
        } else {
            log.info("event=query_error session_id={} fingerprint={} error_code={}", sessionId, fp, sqlState);
        }
    }

    /** Called when the session ends (normal or error). */
    public void sessionEnd() {
        if (!traceEnabled) return;
        log.info("event=session_end session_id={}", sessionId);
    }

    // --- corpus writer ---

    private void appendCorpus(String sql, String fingerprint, long rows, long latencyMs, String cacheLocal) {
        if (corpusFile == null) return;
        String line = "{\"ts\":\"" + Instant.now() + "\""
                + ",\"session_id\":\"" + sessionId + "\""
                + ",\"fingerprint\":\"" + fingerprint + "\""
                + ",\"sql\":" + jsonString(sql)
                + ",\"rows\":" + rows
                + ",\"latency_ms\":" + latencyMs
                + ",\"cache_local\":\"" + cacheLocal + "\""
                + "}\n";
        try {
            Files.writeString(corpusFile, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.warn("trace: failed to write corpus record: {}", e.getMessage());
        }
    }

    // --- helpers ---

    private static String formatParams(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        params.forEach((k, v) -> {
            if (sb.length() > 0) sb.append(", ");
            sb.append(k).append('=').append(v);
        });
        return sb.toString();
    }

    private static String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "");
    }

    private static String jsonString(String s) {
        if (s == null) return "null";
        return "\"" + escape(s) + "\"";
    }
}
