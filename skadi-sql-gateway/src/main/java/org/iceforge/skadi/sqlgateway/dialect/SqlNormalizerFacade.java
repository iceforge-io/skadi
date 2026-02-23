package org.iceforge.skadi.sqlgateway.dialect;

/** Public facade for generating stable cache keys from SQL. */
public final class SqlNormalizerFacade {
    private SqlNormalizerFacade() {
    }

    public static String normalizeForKey(String sql) {
        return SqlNormalizer.normalizeForKey(sql);
    }
}

