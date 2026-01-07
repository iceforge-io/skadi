package org.iceforge.skadi.aws.s3;

import java.time.Instant;

public record CacheEntryMeta(
        String bucket,
        String key,
        long sizeBytes,
        Instant cachedAt,
        String source // "S3" or "PEER:<baseUrl>"
) {}
