package org.iceforge.skadi.sqlgateway.security;

import java.nio.charset.StandardCharsets;

/**
 * Basic, fast security checks applied to every query before execution.
 *
 * <p>This is not a web-application firewall and does not attempt to parse SQL.
 * Its purpose is to close the cheapest abuse paths:
 * <ul>
 *   <li>OOM via pathologically large queries</li>
 *   <li>Protocol-injection via embedded null bytes</li>
 * </ul>
 *
 * <p>SQL injection prevention for parameterised queries is handled by
 * {@code PreparedStatement} binding (see B3).  For zero-param queries the user
 * IS the SQL author (authenticated principal), so no further injection defence is needed.
 */
public final class SqlSecurityValidator {

    /** Maximum allowed query size in bytes (UTF-8 encoded). 1 MiB is generous for any BI tool. */
    static final int MAX_QUERY_BYTES = 1_048_576;

    private SqlSecurityValidator() {}

    /**
     * Validates {@code sql} for basic security invariants.
     *
     * @throws SqlSecurityException if the query fails validation; the message is safe to
     *                              surface to the client (no internal detail is leaked)
     */
    public static void validate(String sql) throws SqlSecurityException {
        if (sql == null || sql.isBlank()) return; // empty queries are handled upstream

        int byteLen = sql.getBytes(StandardCharsets.UTF_8).length;
        if (byteLen > MAX_QUERY_BYTES) {
            throw new SqlSecurityException(
                    "Query exceeds maximum allowed size (" + MAX_QUERY_BYTES + " bytes)");
        }

        if (sql.indexOf('\0') >= 0) {
            throw new SqlSecurityException("Query contains illegal null byte");
        }
    }

    /** Thrown when a query fails a security check. Maps to SQLSTATE {@code 42501}. */
    public static final class SqlSecurityException extends Exception {
        SqlSecurityException(String message) {
            super(message);
        }
    }
}
