package org.iceforge.skadi.sqlgateway.cache;

import org.iceforge.skadi.sqlgateway.executor.SqlParam;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * Deterministic cache key for query-result caching.
 *
 * Contract (POC): exact match on
 * - user scope (e.g. username / credential id)
 * - normalized SQL
 * - parameter values
 */
public final class QueryResultCacheKey {

    private QueryResultCacheKey() {
    }

    public static String cacheId(String userScope, String normalizedSql, List<SqlParam> params) {
        Objects.requireNonNull(normalizedSql, "normalizedSql");

        StringBuilder sb = new StringBuilder();
        sb.append("user=").append(nullSafe(userScope)).append('\n');
        sb.append("sql=").append(normalizedSql).append('\n');
        sb.append("params=").append(serializeParams(params)).append('\n');

        return sha256Hex(sb.toString());
    }

    private static String serializeParams(List<SqlParam> params) {
        if (params == null || params.isEmpty()) return "";
        // params are already sequential and ordered by index in our bridge/executors.
        StringBuilder sb = new StringBuilder();
        for (SqlParam p : params) {
            if (sb.length() > 0) sb.append('|');
            sb.append(p.index()).append('=');
            // Keep it simple and stable for POC.
            Object v = p.value();
            sb.append(v == null ? "null" : v.toString());
        }
        return sb.toString();
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s.trim();
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(s.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

