package org.iceforge.skadi.aws.s3;

public class S3CacheMissException extends RuntimeException {
    private final String bucket;
    private final String key;

    public S3CacheMissException(String bucket, String key, Throwable cause) {
        super("S3 cache miss: s3://" + bucket + "/" + key, cause);
        this.bucket = bucket;
        this.key = key;
    }

    public String bucket() { return bucket; }
    public String key() { return key; }
}
