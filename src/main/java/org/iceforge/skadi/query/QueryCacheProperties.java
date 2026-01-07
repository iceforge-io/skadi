package org.iceforge.skadi.query;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for the query-result cache stored in S3.
 * <p>
 * Defaults are intentionally small and safe for local dev.
 */
@ConfigurationProperties(prefix = "skadi.query-cache")
public class QueryCacheProperties {

    /** S3 bucket where manifests/chunks are written. */
    private String bucket = "skadi-cache";

    /** Prefix inside the bucket. */
    private String prefix = "results";

    /** How many background threads can run query->S3 materialization. */
    private int maxConcurrentWrites = 2;

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public int getMaxConcurrentWrites() {
        return maxConcurrentWrites;
    }

    public void setMaxConcurrentWrites(int maxConcurrentWrites) {
        this.maxConcurrentWrites = maxConcurrentWrites;
    }
}
