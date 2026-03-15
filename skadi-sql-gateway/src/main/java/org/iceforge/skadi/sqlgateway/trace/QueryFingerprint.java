package org.iceforge.skadi.sqlgateway.trace;

import org.iceforge.skadi.sqlgateway.dialect.SqlNormalizer;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Produces a stable, short hex fingerprint of a SQL statement for log correlation. */
public final class QueryFingerprint {

    private QueryFingerprint() {}

    /**
     * Returns the first 8 hex characters of the SHA-256 of the normalized SQL.
     * Identical queries (modulo whitespace/comments/casing) produce the same fingerprint.
     */
    public static String of(String sql) {
        if (sql == null || sql.isBlank()) return "00000000";
        String normalized = SqlNormalizer.normalizeForKey(sql);
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(normalized.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(8);
            for (int i = 0; i < 4; i++) {
                sb.append(String.format("%02x", digest[i] & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed by the JVM spec
            throw new IllegalStateException(e);
        }
    }
}
