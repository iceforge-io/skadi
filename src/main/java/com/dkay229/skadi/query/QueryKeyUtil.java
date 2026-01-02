package com.dkay229.skadi.query;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Objects;

/**
 * Canonicalizes query inputs into a deterministic SHA-256 id.
 */
public final class QueryKeyUtil {
    private QueryKeyUtil() {}

    public static String queryId(QueryModels.QueryRequest req) {
        Objects.requireNonNull(req, "req");
        StringBuilder sb = new StringBuilder();
        // Prefer an explicit cacheKey if supplied (user controls staleness/semantics).
        if (req.cacheKey() != null && !req.cacheKey().isBlank()) {
            sb.append("cacheKey=").append(req.cacheKey().trim()).append("\n");
        }

        QueryModels.QueryRequest.Jdbc jdbc = Objects.requireNonNull(req.jdbc(), "jdbc");
        sb.append("jdbcUrl=").append(nullSafe(jdbc.jdbcUrl())).append("\n");
        sb.append("user=").append(nullSafe(jdbc.username())).append("\n");
        sb.append("sql=").append(normalizeSql(jdbc.sql())).append("\n");
        sb.append("params=").append(join(jdbc.params())).append("\n");

        QueryModels.QueryRequest.Format fmt = req.format();
        sb.append("fmt=").append(fmt != null ? nullSafe(fmt.type()) : "ndjson").append("\n");
        sb.append("gzip=").append(fmt != null && Boolean.TRUE.equals(fmt.gzip())).append("\n");

        QueryModels.QueryRequest.Chunking ch = req.chunking();
        sb.append("targetChunkBytes=").append(ch != null ? nullSafe(ch.targetChunkBytes()) : "").append("\n");

        QueryModels.QueryRequest.Cache cache = req.cache();
        sb.append("ttlSeconds=").append(cache != null && cache.ttlSeconds() != null ? cache.ttlSeconds() : "").append("\n");

        return sha256Hex(sb.toString());
    }

    static String normalizeSql(String sql) {
        if (sql == null) return "";
        // Minimal normalization: trim + collapse whitespace.
        return sql.trim().replaceAll("\\s+", " ");
    }

    private static String join(List<String> vals) {
        if (vals == null || vals.isEmpty()) return "";
        return String.join("|", vals);
    }

    private static String nullSafe(String s) {
        return s == null ? "" : s;
    }

    private static String sha256Hex(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dig = md.digest(s.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(dig.length * 2);
            for (byte b : dig) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 unavailable", e);
        }
    }
}
