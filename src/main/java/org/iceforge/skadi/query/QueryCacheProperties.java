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

    /** Sub-prefix for Arrow IPC artifacts written by /api/v1/queries (Option A). */
    private String arrowPrefix = "arrow";

    /** Switch to multipart upload above this many bytes (Option A). */
    private long arrowMultipartAboveBytes = 128L * 1024L * 1024L;

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

    public String getArrowPrefix() {
        return arrowPrefix;
    }

    public void setArrowPrefix(String arrowPrefix) {
        this.arrowPrefix = arrowPrefix;
    }

    public long getArrowMultipartAboveBytes() {
        return arrowMultipartAboveBytes;
    }

    public void setArrowMultipartAboveBytes(long arrowMultipartAboveBytes) {
        this.arrowMultipartAboveBytes = arrowMultipartAboveBytes;
    }
}
