package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.ResultSetToS3ChunkWriter;
import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.aws.s3.S3Models;
import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

@Service
public class QueryService {
    private static final Logger logger = LoggerFactory.getLogger(QueryService.class);

    private final QueryCacheProperties props;
    private final ResultSetToS3ChunkWriter writer;
    private final S3AccessLayer s3;
    private final LockService lockService;
    private final QueryRegistry registry;
    private final ManifestReader manifestReader;
    private final ExecutorService queryExecutor;
    private final JdbcClientFactory jdbcClientFactory;

    public QueryService(QueryCacheProperties props,
                        ResultSetToS3ChunkWriter writer,
                        S3AccessLayer s3,
                        LockService lockService,
                        QueryRegistry registry,
                        ManifestReader manifestReader,
                        ExecutorService queryExecutor,
                        JdbcClientFactory jdbcClientFactory) {
        this.props = Objects.requireNonNull(props);
        this.writer = Objects.requireNonNull(writer);
        this.s3 = Objects.requireNonNull(s3);
        this.lockService = Objects.requireNonNull(lockService);
        this.registry = Objects.requireNonNull(registry);
        this.manifestReader = Objects.requireNonNull(manifestReader);
        this.queryExecutor = Objects.requireNonNull(queryExecutor);
        this.jdbcClientFactory = Objects.requireNonNull(jdbcClientFactory);
    }

    public QueryModels.QueryResponse submit(QueryModels.QueryRequest req) throws Exception {
        String queryId = QueryKeyUtil.queryId(req);

        // Deterministic location for this queryId
        String bucket = props.getBucket();
        String prefix = props.getPrefix();
        String runId = queryId;

        ResultSetToS3ChunkWriter.S3WritePlan plan = new ResultSetToS3ChunkWriter.S3WritePlan(bucket, prefix, runId);

        // Fast HIT check
        if (s3.exists(plan.manifestRef())) {
            ResultSetToS3ChunkWriter.S3ResultSetRef ref = loadRefFromManifestOrFallback(plan);
            registry.put(queryId, QueryModels.Status.HIT, ref, null);
            return new QueryModels.QueryResponse(QueryModels.Status.HIT, queryId, ref,
                    Map.of("manifestKey", plan.manifestRef().key()));
        }

        // Try to acquire a cross-instance lock
        String lockKey = prefix + "/" + runId + "/.lock";
        String owner = "skadi@" + ProcessHandle.current().pid();
        long ttl = req.cache() != null && req.cache().ttlSeconds() != null ? req.cache().ttlSeconds() : 3600L;

        boolean acquired = lockService.tryAcquire(bucket, lockKey, owner, ttl);

        ResultSetToS3ChunkWriter.S3ResultSetRef ref = new ResultSetToS3ChunkWriter.S3ResultSetRef(
                bucket, prefix, runId, plan.manifestRef().key(), 0L, 0
        );

        if (!acquired) {
            registry.put(queryId, QueryModels.Status.RUNNING, ref, null);
            return new QueryModels.QueryResponse(QueryModels.Status.RUNNING, queryId, ref, Map.of());
        }

        // We are the writer. Execute asynchronously.
        registry.put(queryId, QueryModels.Status.RUNNING, ref, null);
        queryExecutor.submit(() -> {
            try {
                materialize(req, plan, queryId);
            } catch (Exception e) {
                registry.put(queryId, QueryModels.Status.FAILED, ref, e.getMessage());
                logger.warn("Query materialization failed queryId={}: {}", queryId, e.toString());
            } finally {
                lockService.release(bucket, lockKey);
            }
        });

        return new QueryModels.QueryResponse(QueryModels.Status.RUNNING, queryId, ref, Map.of("startedAt", Instant.now().toString()));
    }

    public QueryModels.QueryStatusResponse status(String queryId) {
        QueryRegistry.Entry entry = registry.get(queryId).orElse(null);
        if (entry == null) {
            return new QueryModels.QueryStatusResponse(QueryModels.Status.FAILED, queryId, null,
                    "Unknown queryId", Instant.now());
        }

        // Upgrade RUNNING -> DONE if manifest now exists
        if (entry.status() == QueryModels.Status.RUNNING && entry.ref() != null) {
            S3Models.ObjectRef manifestRef = new S3Models.ObjectRef(entry.ref().bucket(), entry.ref().manifestKey());
            if (s3.exists(manifestRef)) {
                ResultSetToS3ChunkWriter.S3WritePlan plan =
                        new ResultSetToS3ChunkWriter.S3WritePlan(entry.ref().bucket(), entry.ref().prefix(), entry.ref().runId());
                ResultSetToS3ChunkWriter.S3ResultSetRef upgraded = loadRefFromManifestOrFallback(plan);
                registry.put(queryId, QueryModels.Status.DONE, upgraded, null);
                entry = registry.get(queryId).orElse(entry);
            }
        }

        return new QueryModels.QueryStatusResponse(entry.status(), queryId, entry.ref(), entry.error(), entry.updatedAt());
    }

    private void materialize(QueryModels.QueryRequest req, ResultSetToS3ChunkWriter.S3WritePlan plan, String queryId) throws Exception {
        QueryModels.QueryRequest.Jdbc jdbc = Objects.requireNonNull(req.jdbc(), "jdbc");
        String sql = Objects.requireNonNull(jdbc.sql(), "sql");

        ResultSetToS3ChunkWriter.StreamOptions opt = ResultSetToS3ChunkWriter.StreamOptions.defaults();

        // gzip override
        boolean gzip = req.format() != null && Boolean.TRUE.equals(req.format().gzip());
        if (!gzip) {
            opt = new ResultSetToS3ChunkWriter.StreamOptions(
                    opt.jdbcFetchSize(),
                    opt.uploadThreads(),
                    opt.maxInFlightChunks(),
                    opt.maxInFlightBytes(),
                    opt.targetChunkBytes(),
                    false,
                    opt.rowEncoder(),
                    opt.manifestSerializer(),
                    opt.uploadRetries(),
                    opt.useMultipartAboveBytes()
            );
        }

        // chunk size override
        if (req.chunking() != null && req.chunking().targetChunkBytes() != null && !req.chunking().targetChunkBytes().isBlank()) {
            int target = DataSizeParser.parseBytes(req.chunking().targetChunkBytes());
            opt = new ResultSetToS3ChunkWriter.StreamOptions(
                    opt.jdbcFetchSize(),
                    opt.uploadThreads(),
                    opt.maxInFlightChunks(),
                    Math.max(opt.maxInFlightBytes(), target),
                    target,
                    opt.compress(),
                    opt.rowEncoder(),
                    opt.manifestSerializer(),
                    opt.uploadRetries(),
                    opt.useMultipartAboveBytes()
            );
        }

        try (Connection conn = jdbcClientFactory.openConnection(jdbc)) {
            ResultSetToS3ChunkWriter.S3ResultSetRef ref = writer.write(conn, sql, plan, opt);
            registry.put(queryId, QueryModels.Status.DONE, ref, null);
        }
    }

    private ResultSetToS3ChunkWriter.S3ResultSetRef loadRefFromManifestOrFallback(ResultSetToS3ChunkWriter.S3WritePlan plan) {
        try {
            ResultSetToS3ChunkWriter.Manifest m = manifestReader.read(plan.bucket(), plan.manifestRef().key());
            int chunkCount = m.chunks() != null ? m.chunks().size() : 0;
            return new ResultSetToS3ChunkWriter.S3ResultSetRef(
                    plan.bucket(), plan.prefix(), plan.runId(), plan.manifestRef().key(),
                    m.totalRows(), chunkCount
            );
        } catch (Exception e) {
            int chunkCount = s3.list(plan.bucket(), plan.prefix() + "/" + plan.runId() + "/part-", 10_000).size();
            return new ResultSetToS3ChunkWriter.S3ResultSetRef(
                    plan.bucket(), plan.prefix(), plan.runId(), plan.manifestRef().key(),
                    0L, chunkCount
            );
        }
    }
}
