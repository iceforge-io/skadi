package org.iceforge.skadi.semantic.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

/**
 * The logical identity of a cached query result.
 *
 * <p>A {@code CacheIdentity} captures the minimum information needed to
 * determine whether two queries share a cache entry:
 * <ul>
 *   <li>{@link #normalizedSql()} — the normalised SQL string after dialect
 *       translation and whitespace collapsing; must not be blank</li>
 *   <li>{@link #principalName()} — the user or service account that issued
 *       the query; included because row-level security may return different
 *       results for different principals; empty string denotes anonymous</li>
 *   <li>{@link #datasetVersionToken()} — an optional version token that
 *       binds the cache entry to a specific Delta Lake snapshot or Skadi-managed
 *       version; {@code null} in Lane C (gap L3, DQR-001)</li>
 * </ul>
 *
 * <p>{@link #fingerprint()} produces a deterministic SHA-256 hex string over
 * these three fields. It is suitable for use as a physical cache key and
 * matches the approach taken by the existing {@code QueryResultCacheKey} and
 * {@code QueryKeyUtil} utilities in the gateway and server modules.
 *
 * <p>This record does not perform any cache lookup or write.
 *
 * <pre>{@code
 * var id = new CacheIdentity("SELECT SUM(pnl) FROM risk.gold_risk", "alice", null);
 * String key = id.fingerprint();  // deterministic SHA-256 hex
 * }</pre>
 */
public record CacheIdentity(
        String normalizedSql,
        String principalName,
        String datasetVersionToken) {

    /**
     * @param normalizedSql        normalised SQL (post-dialect-translation,
     *                             whitespace-collapsed); must not be null or blank
     * @param principalName        principal identifier; null is treated as empty string
     * @param datasetVersionToken  optional Delta snapshot / version token;
     *                             null means no version pinning (Lane C default)
     */
    public CacheIdentity {
        Objects.requireNonNull(normalizedSql, "normalizedSql must not be null");
        if (normalizedSql.isBlank()) {
            throw new IllegalArgumentException("normalizedSql must not be blank");
        }
        // Normalise null principal to empty string so fingerprint is stable.
        if (principalName == null) principalName = "";
    }

    /**
     * Returns a deterministic SHA-256 hex fingerprint over the three identity fields.
     *
     * <p>The fingerprint is suitable for use as a physical cache key. The same
     * input always produces the same output; different inputs produce different
     * outputs with overwhelming probability.
     *
     * <p>Format of the hashed string (newline-delimited):
     * <pre>
     * sql={normalizedSql}\n
     * principal={principalName}\n
     * version={datasetVersionToken or ""}\n
     * </pre>
     */
    public String fingerprint() {
        String input = "sql=" + normalizedSql + "\n"
                + "principal=" + principalName + "\n"
                + "version=" + (datasetVersionToken != null ? datasetVersionToken : "") + "\n";
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 unavailable", e);
        }
    }
}
