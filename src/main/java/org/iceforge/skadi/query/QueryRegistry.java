package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.ResultSetToS3ChunkWriter;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory registry of query status. This is intentionally MVP; later you can persist.
 */
@Service
public class QueryRegistry {

    public record Entry(QueryModels.Status status,
                        ResultSetToS3ChunkWriter.S3ResultSetRef ref,
                        String error,
                        Instant updatedAt) {}

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();

    public Optional<Entry> get(String queryId) {
        return Optional.ofNullable(map.get(queryId));
    }

    public void put(String queryId, QueryModels.Status status,
                    ResultSetToS3ChunkWriter.S3ResultSetRef ref,
                    String error) {
        map.put(queryId, new Entry(status, ref, error, Instant.now()));
    }
}
