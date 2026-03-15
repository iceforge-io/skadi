package org.iceforge.skadi.sqlgateway.pgwire;

import org.iceforge.skadi.sqlgateway.cache.QueryResultCache;
import org.iceforge.skadi.sqlgateway.cache.QueryResultCacheKey;
import org.iceforge.skadi.sqlgateway.cache.QueryResultCacheMetrics;
import org.iceforge.skadi.sqlgateway.dialect.ClientCompatibility;
import org.iceforge.skadi.sqlgateway.dialect.SourceDialect;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridge;
import org.iceforge.skadi.sqlgateway.dialect.SqlDialectBridgeOptions;
import org.iceforge.skadi.sqlgateway.executor.SqlParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * POC query-result cache for pgwire simple-query execution.
 *
 * <p>Contract: conservative exact-match caching for deterministic queries only.
 * For now we define "deterministic" as SQL that begins with SELECT/WITH/VALUES and does not
 * contain obvious non-deterministic functions.
 */
public final class PgWireQueryCachingExecutorProvider {

    private static final Logger log = LoggerFactory.getLogger(PgWireQueryCachingExecutorProvider.class);

    private final DataSource dataSource;
    private final QueryResultCache cache;
    private final QueryResultCacheMetrics metrics;
    private final Duration ttl;

    public PgWireQueryCachingExecutorProvider(DataSource dataSource,
                                              QueryResultCache cache,
                                              QueryResultCacheMetrics metrics,
                                              Duration ttl) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.ttl = ttl == null ? Duration.ofMinutes(2) : ttl;
    }

    /**
     * Executes a query to an OutputStream, using cache when possible.
     *
     * @return cache source tag: "cache_local" or "miss" (for now)
     */
    public String executeToStream(String sql, List<SqlParam> params, String userScope, OutputStream out) throws Exception {
        SqlDialectBridge bridge = new SqlDialectBridge(new SqlDialectBridgeOptions(
                SourceDialect.POSTGRES,
                ClientCompatibility.GENERIC_JDBC,
                true,
                true,
                true,
                true,
                true,
                true
        ));

        var bridged = bridge.bridge(sql, params);
        String translatedSql = bridged.translatedSql();
        List<SqlParam> translatedParams = bridged.translatedParams();

        // Conservative: only cache likely-deterministic queries.
        if (!isDeterministic(translatedSql)) {
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(translatedSql)) {
                bind(ps, translatedParams);
                try (var allocator = new org.apache.arrow.memory.RootAllocator()) {
                    org.iceforge.skadi.arrow.JdbcArrowStreamer.stream(ps, 1024, allocator, out, () -> false);
                }
            }
            return "miss";
        }

        String cacheId = QueryResultCacheKey.cacheId(userScope, bridged.normalizedSqlForKey(), bridged.normalizedParamsForKey());

        Optional<QueryResultCache.ArrowEntry> hit = cache.getArrow(cacheId);
        if (hit.isPresent()) {
            metrics.recordHit();
            QueryResultCache.ArrowEntry e = hit.get();
            new ByteArrayInputStream(e.arrowBytes()).transferTo(out);
            log.info("CACHE_HIT cache_local=true cache_s3=false cacheId={} sizeBytes={}", cacheId, e.sizeBytes());
            return "cache_local";
        }

        metrics.recordMiss();
        Instant start = Instant.now();
        var buffer = QueryResultCache.newBuffer();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(translatedSql)) {
            bind(ps, translatedParams);
            try (var allocator = new org.apache.arrow.memory.RootAllocator()) {
                org.iceforge.skadi.arrow.JdbcArrowStreamer.stream(ps, 1024, allocator, buffer, () -> false);
            }
        }

        byte[] bytes = buffer.toByteArray();
        cache.putArrow(cacheId, bytes, ttl);
        new ByteArrayInputStream(bytes).transferTo(out);

        log.info("CACHE_MISS cache_local=false cache_s3=false cacheId={} sizeBytes={} durationMs={} normalizedSql='{}'",
                cacheId,
                bytes.length,
                Duration.between(start, Instant.now()).toMillis(),
                bridged.normalizedSqlForKey());

        return "miss";
    }

    private static boolean isDeterministic(String sql) {
        if (sql == null) return false;
        String s = sql.trim().toLowerCase();
        if (!(s.startsWith("select") || s.startsWith("with") || s.startsWith("values"))) return false;
        // POC blacklist.
        return !(s.contains("current_timestamp")
                || s.contains("current_date")
                || s.contains("now()")
                || s.contains("rand(")
                || s.contains("random(")
                || s.contains("uuid"));
    }

    private static void bind(PreparedStatement ps, List<SqlParam> params) throws Exception {
        if (params == null) return;
        for (SqlParam p : params) {
            if (p.value() == null) {
                if (p.jdbcType() != null) {
                    ps.setNull(p.index(), p.jdbcType());
                } else {
                    ps.setObject(p.index(), null);
                }
            } else {
                ps.setObject(p.index(), p.value());
            }
        }
    }
}

