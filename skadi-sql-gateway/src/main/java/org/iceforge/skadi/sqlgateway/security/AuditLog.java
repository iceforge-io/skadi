package org.iceforge.skadi.sqlgateway.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Always-on structured audit event emitter.
 *
 * <p>Audit events are emitted regardless of {@code trace.enabled}.  They use a dedicated
 * logger ({@code skadi.audit}) so operators can route them to a separate file or SIEM
 * without touching the main application log.
 *
 * <p><b>Audit schema:</b>
 * <pre>
 *   event=audit_connect  user=... client=... session_id=... outcome=SUCCESS|DENIED
 *   event=audit_query    user=... schema=... fingerprint=... cache=... rows=... latency_ms=...
 *                        session_id=... query_id=...
 *   event=audit_error    user=... schema=... fingerprint=... sqlstate=... session_id=... query_id=...
 * </pre>
 *
 * <p><b>Redaction guarantee:</b> none of these methods accept SQL text, bind-parameter
 * values, or passwords.  Fingerprints are opaque SHA-256 prefixes.
 */
public final class AuditLog {

    private static final Logger AUDIT = LoggerFactory.getLogger("skadi.audit");

    private AuditLog() {}

    /**
     * Records a connection attempt outcome.
     *
     * @param sessionId  pgwire session identifier
     * @param user       authenticated username (empty string if unknown before auth failure)
     * @param client     client application name (e.g. "tableau", "dbeaver")
     * @param success    true when authentication succeeded
     */
    public static void connect(String sessionId, String user, String client, boolean success) {
        AUDIT.info("event=audit_connect user={} client={} session_id={} outcome={}",
                sanitise(user), sanitise(client), sessionId, success ? "SUCCESS" : "DENIED");
    }

    /**
     * Records a successfully completed query.
     *
     * @param sessionId  pgwire session identifier
     * @param queryId    per-query correlation ID (from MDC)
     * @param user       authenticated username
     * @param schema     default schema for this session (from config)
     * @param fingerprint 8-char SHA-256 prefix of the normalised SQL
     * @param cacheTier  cache disposition tag (e.g. "HIT", "MISS", "arrow_hit")
     * @param rows       number of rows returned
     * @param latencyMs  wall-clock query duration in milliseconds
     */
    public static void query(String sessionId, String queryId, String user, String schema,
                             String fingerprint, String cacheTier, long rows, long latencyMs) {
        AUDIT.info("event=audit_query user={} schema={} fingerprint={} cache={} rows={} latency_ms={} session_id={} query_id={}",
                sanitise(user), sanitise(schema), fingerprint,
                cacheTier, rows, latencyMs, sessionId, nullSafe(queryId));
    }

    /**
     * Records a query that produced an error response.
     *
     * @param sessionId   pgwire session identifier
     * @param queryId     per-query correlation ID (from MDC)
     * @param user        authenticated username
     * @param schema      default schema for this session
     * @param fingerprint 8-char SHA-256 prefix of the normalised SQL
     * @param sqlState    five-character SQLSTATE code
     */
    public static void queryError(String sessionId, String queryId, String user, String schema,
                                  String fingerprint, String sqlState) {
        AUDIT.info("event=audit_error user={} schema={} fingerprint={} sqlstate={} session_id={} query_id={}",
                sanitise(user), sanitise(schema), fingerprint,
                nullSafe(sqlState), sessionId, nullSafe(queryId));
    }

    // --- helpers ---

    /** Removes any log-forging characters from a value that came from the client. */
    private static String sanitise(String s) {
        if (s == null || s.isBlank()) return "-";
        // Strip newlines and tabs to prevent log injection.
        return s.replace('\n', '_').replace('\r', '_').replace('\t', '_');
    }

    private static String nullSafe(String s) {
        return s == null ? "-" : s;
    }
}
