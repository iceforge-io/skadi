package com.dkay229.skadi.query;

import com.dkay229.skadi.aws.s3.ResultSetToS3ChunkWriter;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Public API models for the query cache endpoints.
 */
public final class QueryModels {
    private QueryModels() {}

    public enum Status {
        HIT,
        RUNNING,
        DONE,
        FAILED
    }

    /**
     * MVP request: provide a fully rendered SQL string (params are only used for cache key canonicalization).
     * <p>
     * You can evolve this to prepared-statement parameter binding later.
     */
    public record QueryRequest(
            String cacheKey,
            Jdbc jdbc,
            Format format,
            Chunking chunking,
            Cache cache
    ) {
        public record Jdbc(
                String jdbcUrl,
                String username,
                String password,
                String sql,
                List<String> params
        ) {}

        public record Format(
                String type, // "ndjson" only for now
                Boolean gzip
        ) {}

        public record Chunking(
                String targetChunkBytes
        ) {}

        public record Cache(
                Long ttlSeconds
        ) {}
    }

    public record QueryResponse(
            Status status,
            String queryId,
            ResultSetToS3ChunkWriter.S3ResultSetRef ref,
            Map<String, Object> meta
    ) {}

    public record QueryStatusResponse(
            Status status,
            String queryId,
            ResultSetToS3ChunkWriter.S3ResultSetRef ref,
            String error,
            Instant updatedAt
    ) {}
}
