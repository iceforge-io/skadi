package org.iceforge.skadi.dashboard;

import org.iceforge.skadi.query.QueryStatsRegistry;
import org.springframework.web.bind.annotation.*;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/internal/dashboard")
public class DashboardController {

    private final QueryStatsRegistry stats;

    public DashboardController(QueryStatsRegistry stats) {
        this.stats = stats;
    }

    @GetMapping("/node")
    public Map<String, Object> node() {
        long uptimeMs = ManagementFactory.getRuntimeMXBean().getUptime();
        return Map.of(
                "now", Instant.now().toString(),
                "uptimeMs", uptimeMs
        );
    }

    @GetMapping("/queries/top")
    public List<Map<String, Object>> top(@RequestParam(defaultValue = "50") int limit) {
        return stats.topByBytes(limit).stream()
                .map(e -> {
                    var s = e.getValue();
                    return Map.<String, Object>of(
                            "queryId", e.getKey(),
                            "bytesServed", s.bytesServed.sum(),
                            "localHits", s.localHits.sum(),
                            "peerHits", s.peerHits.sum(),
                            "s3Hits", s.s3Hits.sum(),
                            "lastAccess", s.lastAccess.toString()
                    );
                })
                .toList();
    }

    @GetMapping("/queries/{queryId}")
    public Map<String, Object> query(@PathVariable String queryId) {
        var s = stats.get(queryId);
        if (s == null) {
            return Map.of("queryId", queryId, "found", false);
        }
        return Map.of(
                "queryId", queryId,
                "found", true,
                "bytesServed", s.bytesServed.sum(),
                "localHits", s.localHits.sum(),
                "peerHits", s.peerHits.sum(),
                "s3Hits", s.s3Hits.sum(),
                "lastAccess", s.lastAccess.toString()
        );
    }
}
