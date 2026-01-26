package org.iceforge.skadi.api;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "skadi.dashboard")
public class DashboardProperties {

    /**
     * How often cache storage size may be recomputed (S3 LIST).
     */
    private long storageTtlSeconds = 60; // safe default

    public long getStorageTtlSeconds() {
        return storageTtlSeconds;
    }

    public void setStorageTtlSeconds(long storageTtlSeconds) {
        this.storageTtlSeconds = storageTtlSeconds;
    }
}
