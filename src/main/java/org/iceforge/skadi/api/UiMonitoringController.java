package org.iceforge.skadi.api;

import org.iceforge.skadi.api.v1.QueryV1Models;
import org.iceforge.skadi.api.v1.QueryV1Registry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * UI-friendly monitoring endpoints for the static dashboard.
 *
 * <p>These are intentionally small, aggregated payloads so the browser can poll cheaply.
 */
@RestController
@RequestMapping("/api")
public class UiMonitoringController {

    private final QueryV1Registry v1Registry;

    public UiMonitoringController(QueryV1Registry v1Registry) {
        this.v1Registry = Objects.requireNonNull(v1Registry);
    }

    /**
     * Live KPIs for the top strip.
     *
     * <ul>
     *   <li>runningUncached: number of v1 queries currently in RUNNING</li>
     *   <li>runningCached: best-effort "active" cache hits, approximated as CACHE_HIT entries updated recently</li>
     *   <li>clusterNodes: UI will usually compute this from /api/peers; we default to 1 here</li>
     * </ul>
     */
    @GetMapping("/metrics/live")
    public Map<String, Object> live(@RequestParam(defaultValue = "30") long cacheHitActiveSeconds) {
        Instant now = Instant.now();

        long runningUncached = v1Registry.recent(200).stream()
                .filter(e -> e.state() == QueryV1Models.State.RUNNING)
                .count();

        long runningCached = v1Registry.recent(500).stream()
                .filter(e -> isCacheSource(e.lastSource()))
                .filter(e -> e.updatedAt() != null && Duration.between(e.updatedAt(), now).abs().getSeconds() <= cacheHitActiveSeconds)
                .count();

        return Map.of(
                "runningUncached", runningUncached,
                "runningCached", runningCached,
                "clusterNodes", 1,
                "updatedAtIso", now.toString()
        );
    }

    /**
     * Time series of cached vs uncached durations.
     *
     * <p>We bucket by a fixed step (minute-ish) and return average durations per bucket.
     */
    @GetMapping("/metrics/timeseries")
    public List<Map<String, Object>> timeseries(@RequestParam(defaultValue = "1h") String window) {
        Window w = Window.parse(window);

        Instant now = Instant.now();
        Instant start = now.minus(w.duration);

        // Choose a reasonable step.
        Duration step = switch (w.kind) {
            case M15, H1 -> Duration.ofMinutes(1);
            case H6 -> Duration.ofMinutes(5);
            case H24 -> Duration.ofMinutes(15);
        };

        // Align start to step boundaries for stable x-axis ticks.
        Instant alignedStart = alignDown(start, step);
        Instant alignedEnd = alignUp(now, step);

        int buckets = (int) Math.max(1, Duration.between(alignedStart, alignedEnd).toSeconds() / step.toSeconds());

        long[] cachedSum = new long[buckets];
        long[] cachedCnt = new long[buckets];
        long[] uncachedSum = new long[buckets];
        long[] uncachedCnt = new long[buckets];

        for (QueryV1Registry.Entry e : v1Registry.recent(2000)) {
            Instant upd = e.updatedAt();
            Instant st = e.startedAt();
            if (upd == null || st == null) continue;
            if (upd.isBefore(alignedStart) || upd.isAfter(alignedEnd)) continue;

            long durMs = Math.max(0L, Duration.between(st, upd).toMillis());
            int idx = (int) (Duration.between(alignedStart, upd).toSeconds() / step.toSeconds());
            if (idx < 0 || idx >= buckets) continue;

            boolean isCached = isCacheSource(e.lastSource());
            if (isCached) {
                cachedSum[idx] += durMs;
                cachedCnt[idx] += 1;
            } else {
                uncachedSum[idx] += durMs;
                uncachedCnt[idx] += 1;
            }
        }

        List<Map<String, Object>> out = new ArrayList<>(buckets);
        for (int i = 0; i < buckets; i++) {
            Instant ts = alignedStart.plus(step.multipliedBy(i));

            Long cachedMs = cachedCnt[i] == 0 ? null : (cachedSum[i] / cachedCnt[i]);
            Long uncachedMs = uncachedCnt[i] == 0 ? null : (uncachedSum[i] / uncachedCnt[i]);

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("tsIso", ts.toString());
            row.put("cachedMs", cachedMs);
            row.put("uncachedMs", uncachedMs);
            out.add(row);
        }

        return out;
    }

    /**
     * Query history for the bottom table.
     */
    @GetMapping("/queries/history")
    public List<Map<String, Object>> history(@RequestParam(defaultValue = "200") int limit) {
        int lim = Math.max(1, Math.min(limit, 500));
        List<Map<String, Object>> out = new ArrayList<>(lim);

        for (QueryV1Registry.Entry e : v1Registry.recent(lim)) {
            Instant st = e.startedAt();
            Instant upd = e.updatedAt();

            Long durationMs = null;
            if (st != null && upd != null) {
                durationMs = Math.max(0L, Duration.between(st, upd).toMillis());
            }

            boolean cached = isCacheSource(e.lastSource());

            String source = normalizeSource(e.lastSource(), cached);
            String cacheKind = cached ? source : null;

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("startedAtIso", st == null ? null : st.toString());
            row.put("queryId", e.queryId());
            row.put("source", source);
            row.put("cache", cacheKind);
            row.put("cached", cached);
            row.put("durationMs", durationMs);
            long rp = e.rowsProduced();
            // Rows are best-effort; show null instead of a misleading 0.
            row.put("rows", rp <= 0 ? null : rp);
            row.put("sql", e.request() == null ? null : e.request().sql());
            row.put("status", toStatus(e.state()));
            out.add(row);
        }

        return out;
    }

    private static String toStatus(QueryV1Models.State s) {
        if (s == null) return "UNKNOWN";
        return switch (s) {
            case QUEUED, RUNNING -> "RUNNING";
            case SUCCEEDED -> "OK";
            case FAILED -> "FAILED";
            case CANCELED -> "CANCELED";
        };
    }

    private static boolean isCacheSource(String lastSource) {
        if (lastSource == null) return false;
        String v = lastSource.trim().toUpperCase();
        return v.startsWith("CACHE_");
    }

    private static String normalizeSource(String lastSource, boolean cached) {
        if (cached) {
            if (lastSource == null) return "cache_s3";
            String v = lastSource.trim().toLowerCase();
            // Expect CACHE_LOCAL or CACHE_S3
            if (v.equals("cache_local") || v.equals("cache_s3")) return v;
            if (v.startsWith("cache_")) return v;
            return "cache_s3";
        }
        if (lastSource == null) return "db";
        String v = lastSource.trim().toLowerCase();
        if (v.equals("db")) return "db";
        // Older variants (CACHE_HIT) should not reach here if cached=true, but keep safe.
        if (v.equals("cache_hit")) return "cache_s3";
        return v;
    }

    private static Instant alignDown(Instant t, Duration step) {
        long sec = t.getEpochSecond();
        long k = step.getSeconds();
        long aligned = (sec / k) * k;
        return Instant.ofEpochSecond(aligned).truncatedTo(ChronoUnit.SECONDS);
    }

    private static Instant alignUp(Instant t, Duration step) {
        long sec = t.getEpochSecond();
        long k = step.getSeconds();
        long aligned = ((sec + k - 1) / k) * k;
        return Instant.ofEpochSecond(aligned).atOffset(ZoneOffset.UTC).toInstant();
    }

    private enum WindowKind { M15, H1, H6, H24 }

    private record Window(WindowKind kind, Duration duration) {
        static Window parse(String s) {
            if (s == null) return new Window(WindowKind.H1, Duration.ofHours(1));
            String v = s.trim().toLowerCase();
            return switch (v) {
                case "15m", "15min", "15mins" -> new Window(WindowKind.M15, Duration.ofMinutes(15));
                case "6h" -> new Window(WindowKind.H6, Duration.ofHours(6));
                case "24h", "1d" -> new Window(WindowKind.H24, Duration.ofHours(24));
                case "1h" -> new Window(WindowKind.H1, Duration.ofHours(1));
                default -> new Window(WindowKind.H1, Duration.ofHours(1));
            };
        }
    }
}
