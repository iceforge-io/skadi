package org.iceforge.skadi.api.v1;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

/**
 * Canonicalizes Query V1 submit inputs into a deterministic SHA-256 id.
 * Intended use:
 *   String queryId = QueryV1KeyUtil.queryId(req);
 * Notes:
 * - Normalizes SQL (trim + collapse whitespace)
 * - Sorts JDBC properties by key
 * - Canonicalizes "parameters" deterministically (sorted map keys, stable list ordering, stable number/boolean/null formatting)
 */
public final class QueryV1KeyUtil {
    private QueryV1KeyUtil() {}

    public static String queryId(QueryV1Models.SubmitQueryRequest req) {
        Objects.requireNonNull(req, "req");

        StringBuilder sb = new StringBuilder(512);

        QueryV1Models.Jdbc jdbc = req.jdbc();
        if (jdbc != null) {
            sb.append("datasourceId=").append(nullSafe(jdbc.datasourceId())).append('\n');
            sb.append("jdbcUrl=").append(nullSafe(jdbc.jdbcUrl())).append('\n');
            sb.append("user=").append(nullSafe(jdbc.username())).append('\n');
            sb.append("jdbcProps=").append(canonicalizeStringMap(jdbc.properties())).append('\n');
        } else {
            sb.append("datasourceId=\n");
            sb.append("jdbcUrl=\n");
            sb.append("user=\n");
            sb.append("jdbcProps=\n");
        }

        // Prefer top-level sql field (V1 request carries it here)
        sb.append("sql=").append(normalizeSql(req.sql())).append('\n');

        // Parameters can affect semantics; include them in canonical form
        sb.append("params=").append(canonicalizeObject(req.parameters())).append('\n');

        // Result formatting affects bytes, schema, etc. Include it.
        sb.append("resultFormat=").append(nullSafe(req.resultFormat())).append('\n');

        // Chunking can affect output layout/streaming boundaries; include it for safety.
        sb.append("preferredChunkBytes=").append(req.preferredChunkBytes() == null ? "" : req.preferredChunkBytes()).append('\n');

        // Timeout should not affect semantics, but it can change whether a query completes; include it only if you want
        // separate cache keys per timeout. If you prefer cache reuse regardless of timeout, delete this line.
        sb.append("timeoutMs=").append(req.timeoutMs() == null ? "" : req.timeoutMs()).append('\n');

        // Cache mode changes behavior (e.g., BYPASS/READ_THROUGH etc). Include it to avoid mixing semantics.
        sb.append("cacheMode=").append(nullSafe(req.cacheMode())).append('\n');

        return sha256Hex(sb.toString());
    }

    static String normalizeSql(String sql) {
        if (sql == null) return "";
        // Minimal normalization: trim + collapse whitespace
        return sql.trim().replaceAll("\\s+", " ");
    }

    private static String canonicalizeStringMap(Map<String, String> m) {
        if (m == null || m.isEmpty()) return "";
        // Sort keys for determinism
        Map<String, String> sorted = new TreeMap<>(m);
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Map.Entry<String, String> e : sorted.entrySet()) {
            if (!first) sb.append('&');
            first = false;
            sb.append(escape(e.getKey())).append('=').append(escape(e.getValue()));
        }
        return sb.toString();
    }

    /**
     * Deterministic serialization for parameters: supports nested maps/lists/primitives.
     * Maps are sorted by key; lists preserve order; unknown objects fall back to toString().
     */
    @SuppressWarnings("unchecked")
    private static String canonicalizeObject(Object v) {
        if (v == null) return "null";

        if (v instanceof CharSequence s) {
            return '"' + escape(s.toString()) + '"';
        }
        if (v instanceof Boolean b) {
            return b ? "true" : "false";
        }
        if (v instanceof Number n) {
            // Stable numeric string: use toString (JSON-like)
            // (Avoid locale formatting; Number.toString is locale-independent)
            return n.toString();
        }
        if (v instanceof UUID u) {
            return '"' + u.toString() + '"';
        }
        if (v instanceof Date d) {
            // Avoid timezone surprises; Date.toInstant() is UTC-based.
            return '"' + d.toInstant().toString() + '"';
        }
        if (v instanceof Map<?, ?> map) {
            // Sort keys lexicographically by their string form
            TreeMap<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String k = e.getKey() == null ? "null" : e.getKey().toString();
                sorted.put(k, e.getValue());
            }
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            boolean first = true;
            for (Map.Entry<String, Object> e : sorted.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                sb.append('"').append(escape(e.getKey())).append('"').append(':');
                sb.append(canonicalizeObject(e.getValue()));
            }
            sb.append('}');
            return sb.toString();
        }
        if (v instanceof Iterable<?> it) {
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) sb.append(',');
                first = false;
                sb.append(canonicalizeObject(o));
            }
            sb.append(']');
            return sb.toString();
        }
        if (v.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(v);
            StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = 0; i < len; i++) {
                if (i > 0) sb.append(',');
                Object o = java.lang.reflect.Array.get(v, i);
                sb.append(canonicalizeObject(o));
            }
            sb.append(']');
            return sb.toString();
        }

        // Fallback: stable-ish string
        return '"' + escape(v.toString()) + '"';
    }

    private static String escape(String s) {
        if (s == null) return "";
        // Minimal escaping to keep canonical string unambiguous
        return s
                .replace("\\", "\\\\")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\"", "\\\"");
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
