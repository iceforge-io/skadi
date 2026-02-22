package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.CacheFetchContext;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

@Service
public class QueryStatsRegistry {

    public static final class Stats {
        public final LongAdder bytesServed = new LongAdder();
        public final LongAdder localHits = new LongAdder();
        public final LongAdder peerHits  = new LongAdder();
        public final LongAdder s3Hits    = new LongAdder();
        public volatile Instant lastAccess = Instant.now();

        void touch() { lastAccess = Instant.now(); }
    }

    private final ConcurrentHashMap<String, Stats> map = new ConcurrentHashMap<>();

    public void recordServe(String queryId, long bytes, CacheFetchContext.Source src) {
        if (queryId == null || queryId.isBlank()) return;
        Stats s = map.computeIfAbsent(queryId, k -> new Stats());
        if (bytes > 0) s.bytesServed.add(bytes);

        if (src != null) {
            switch (src) {
                case LOCAL -> s.localHits.increment();
                case PEER  -> s.peerHits.increment();
                case S3    -> s.s3Hits.increment();
                default -> {}
            }
        }
        s.touch();
    }

    public Stats get(String queryId) {
        return map.get(queryId);
    }

    public List<Map.Entry<String, Stats>> topByBytes(int limit) {
        int lim = Math.max(1, Math.min(limit, 500));
        return map.entrySet().stream()
                .sorted(Comparator.comparingLong(
                        (Map.Entry<String, Stats> e) -> e.getValue().bytesServed.sum()
                ).reversed())
                .limit(lim)
                .toList();
    }
}
