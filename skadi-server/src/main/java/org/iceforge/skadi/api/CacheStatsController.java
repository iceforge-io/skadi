package org.iceforge.skadi.api;

import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.query.QueryCacheProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

@RestController
@RequestMapping("/api")
public class CacheStatsController {

    private final CacheMetricsRegistry metrics;
    private final S3AccessLayer s3;
    private final QueryCacheProperties props;
    private final Duration storageTtl;

    // Cache the last computed storage snapshot to avoid frequent S3 LIST calls.
    private final AtomicReference<StorageSnapshot> lastStorage = new AtomicReference<>(StorageSnapshot.empty());

    public CacheStatsController(
            CacheMetricsRegistry metrics,
            S3AccessLayer s3,
            QueryCacheProperties props,
            DashboardProperties dashboardProps
    ) {
        this.metrics = Objects.requireNonNull(metrics);
        this.s3 = Objects.requireNonNull(s3);
        this.props = Objects.requireNonNull(props);

        long ttlSeconds = dashboardProps.getStorageTtlSeconds();
        this.storageTtl = Duration.ofSeconds(Math.max(15, ttlSeconds)); // guardrail
    }


    @GetMapping("/cache/stats")
    public Map<String, Object> stats() {
        long hits = metrics.hits();
        long misses = metrics.misses();

        String bucket = props.getBucket();
        String prefix = props.getPrefix();

        StorageSnapshot snap = storageSnapshot(bucket, prefix);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("hits", hits);
        out.put("misses", misses);

        // Keep same keys the UI expects
        out.put("bytesUsed", snap.bytesUsed);
        out.put("objectCount", snap.objectCount);
        out.put("truncated", snap.truncated);

        // Optional: expose freshness/debug
        out.put("storageAsOfEpochMs", snap.asOfEpochMs);

        out.put("store", props.getStore());
        out.put("bucket", bucket);
        out.put("prefix", prefix);
        return out;
    }

    private StorageSnapshot storageSnapshot(String bucket, String prefix) {
        long now = System.currentTimeMillis();
        StorageSnapshot current = lastStorage.get();

        // If cached snapshot is still fresh AND bucket/prefix matches, return it.
        if (current.isFresh(now, storageTtl) && current.matches(bucket, prefix)) {
            return current;
        }

        // Recompute once; if multiple threads race, we accept a small burst at TTL boundary.
        // (If you want strict single-flight, we can add a lock, but this is usually enough.)
        int maxKeys = 10_000;
        long bytesUsed = 0L;
        int objects = 0;
        boolean truncated = false;

        try {
            var items = s3.list(bucket, prefix, maxKeys);
            if (items != null) {
                for (var it : items) {
                    bytesUsed += Math.max(0L, it.size());
                    objects++;
                }
                truncated = (items.size() >= maxKeys);
            }
        } catch (Exception ignored) {
            // On failure, keep previous snapshot if it matches; otherwise return zeros.
            if (current.matches(bucket, prefix)) {
                return current;
            }
            return new StorageSnapshot(bucket, prefix, 0L, 0, false, now);
        }

        StorageSnapshot updated = new StorageSnapshot(bucket, prefix, bytesUsed, objects, truncated, now);
        lastStorage.set(updated);
        return updated;
    }

    private static final class StorageSnapshot {
        final String bucket;
        final String prefix;
        final long bytesUsed;
        final int objectCount;
        final boolean truncated;
        final long asOfEpochMs;

        StorageSnapshot(String bucket, String prefix, long bytesUsed, int objectCount, boolean truncated, long asOfEpochMs) {
            this.bucket = bucket;
            this.prefix = prefix;
            this.bytesUsed = bytesUsed;
            this.objectCount = objectCount;
            this.truncated = truncated;
            this.asOfEpochMs = asOfEpochMs;
        }

        static StorageSnapshot empty() {
            return new StorageSnapshot(null, null, 0L, 0, false, 0L);
        }

        boolean matches(String b, String p) {
            return Objects.equals(bucket, b) && Objects.equals(prefix, p);
        }

        boolean isFresh(long now, Duration ttl) {
            return asOfEpochMs > 0L && (now - asOfEpochMs) < ttl.toMillis();
        }
    }
}
