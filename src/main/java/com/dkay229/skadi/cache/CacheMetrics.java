package com.dkay229.skadi.cache;

public interface CacheMetrics {
    long hits();
    long misses();
    long bytesUsed();
}
