package org.iceforge.skadi.sqlgateway.cache;

import org.iceforge.skadi.sqlgateway.dialect.SqlNormalizer;

/** Builds stable cache keys for query result caching. */
public final class QueryCacheKey {

    private QueryCacheKey() {}

    /**
     * Returns a stable cache key combining normalized SQL and the authenticated username.
     *
     * <p>Normalization strips comments, collapses whitespace, and upper-cases keywords so that
     * cosmetically different but semantically identical queries share a cache entry.
     */
    public static String of(String sql, String user) {
        String normalized = SqlNormalizer.normalizeForKey(sql);
        String u = (user == null || user.isBlank()) ? "" : user.trim();
        return normalized + "|" + u;
    }
}
