package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.DataSizeExpressionEvaluator;

/**
 * Parses human-friendly sizes like "64MiB", "10GB", "4*1024*1024".
 * <p>
 * Internally reuses {@link DataSizeExpressionEvaluator} which understands KB/MB/GB/TB and '*'.
 */
public final class DataSizeParser {
    private DataSizeParser() {}

    public static int parseBytes(String expr) {
        if (expr == null || expr.isBlank()) {
            throw new IllegalArgumentException("size expression is blank");
        }
        String s = expr.trim();
        // Accept common IEC spellings by mapping to the existing evaluator's units.
        s = s.replace("KiB", "KB").replace("MiB", "MB").replace("GiB", "GB").replace("TiB", "TB");
        long v = DataSizeExpressionEvaluator.evaluate(s);
        if (v <= 0L || v > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size out of range: " + expr);
        }
        return (int) v;
    }
}
