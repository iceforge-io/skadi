package org.iceforge.skadi.cache;

public interface CacheMetrics {
    long hits();
    long misses();
    long bytesUsed();
}
