package com.dkay229.skadi.aws.s3;

import java.time.Instant;
import java.util.Map;

public final class S3Models {

    private S3Models() {}

    public record ObjectRef(String bucket, String key) {}

    public record ObjectMetadata(
            String bucket,
            String key,
            long contentLength,
            String eTag,
            String contentType,
            Instant lastModified,
            Map<String, String> userMetadata
    ) {}

    public record ListItem(
            String key,
            long size,
            String eTag,
            Instant lastModified
    ) {}
}

