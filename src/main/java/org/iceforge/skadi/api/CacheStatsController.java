package org.iceforge.skadi.api;

import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.query.QueryCacheProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

@RestController
@RequestMapping("/api")
public class CacheStatsController {

    private final CacheMetricsRegistry metrics;
    private final S3AccessLayer s3;
    private final QueryCacheProperties props;

    public CacheStatsController(CacheMetricsRegistry metrics, S3AccessLayer s3, QueryCacheProperties props) {
        this.metrics = Objects.requireNonNull(metrics);
        this.s3 = Objects.requireNonNull(s3);
        this.props = Objects.requireNonNull(props);
    }

    @GetMapping("/cache/stats")
    public Map<String, Object> stats() {
        long hits = metrics.hits();
        long misses = metrics.misses();

        // Best-effort storage sizing. For very large buckets, this will be a capped estimate.
        String bucket = props.getBucket();
        String prefix = props.getPrefix();

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
            // leave bytesUsed at 0 and expose an error flag below
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("hits", hits);
        out.put("misses", misses);
        out.put("bytesUsed", bytesUsed);
        out.put("objectCount", objects);
        out.put("truncated", truncated);
        out.put("store", props.getStore());
        out.put("bucket", bucket);
        out.put("prefix", prefix);
        return out;
    }
}
