// java
package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.CacheFetchContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class QueryStatsRegistryTest {

    private QueryStatsRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new QueryStatsRegistry();
    }

    @Test
    void recordServe_updatesBytesAndHits_andTouch() {
        String qid = "q1";

        registry.recordServe(qid, 100, CacheFetchContext.Source.LOCAL);
        registry.recordServe(qid, 200, CacheFetchContext.Source.PEER);
        registry.recordServe(qid, 0, CacheFetchContext.Source.S3); // bytes not added
        registry.recordServe(qid, -50, null); // ignored bytes, no hit type

        QueryStatsRegistry.Stats stats = registry.get(qid);
        assertNotNull(stats, "Stats should exist after recordServe");
        assertEquals(300L, stats.bytesServed.sum());
        assertEquals(1L, stats.localHits.sum());
        assertEquals(1L, stats.peerHits.sum());
        assertEquals(1L, stats.s3Hits.sum());

        // lastAccess should be updated to a time close to 'now'
        var before = stats.lastAccess;
        registry.recordServe(qid, 1, CacheFetchContext.Source.LOCAL);
        assertTrue(stats.lastAccess.isAfter(before) || stats.lastAccess.equals(before));
    }


    @Test
    void get_returnsNullForUnknownId() {
        assertNull(registry.get("missing"));
    }

    @Test
    void topByBytes_returnsSortedLimitedList() {
        registry.recordServe("a", 10, CacheFetchContext.Source.LOCAL);
        registry.recordServe("b", 50, CacheFetchContext.Source.LOCAL);
        registry.recordServe("c", 30, CacheFetchContext.Source.LOCAL);
        registry.recordServe("b", 20, CacheFetchContext.Source.LOCAL); // total 70

        List<Map.Entry<String, QueryStatsRegistry.Stats>> top2 = registry.topByBytes(2);
        assertEquals(2, top2.size());
        assertEquals("b", top2.get(0).getKey());
        assertEquals(70L, top2.get(0).getValue().bytesServed.sum());
        assertEquals("c", top2.get(1).getKey());
        assertEquals(30L, top2.get(1).getValue().bytesServed.sum());

        // limit bounds: min 1, max 500
        assertEquals(1, registry.topByBytes(0).size());
        assertTrue(registry.topByBytes(999).size() <= 3);
    }

    @Test
    void concurrentUpdates_areAggregated() throws InterruptedException {
        String qid = "concurrent";
        int threads = 8;
        int loops = 500;
        Thread[] ts = new Thread[threads];

        for (int i = 0; i < threads; i++) {
            ts[i] = new Thread(() -> {
                for (int j = 0; j < loops; j++) {
                    registry.recordServe(qid, 1, CacheFetchContext.Source.PEER);
                }
            });
        }
        for (Thread t : ts) t.start();
        for (Thread t : ts) t.join();

        QueryStatsRegistry.Stats stats = registry.get(qid);
        assertNotNull(stats);
        assertEquals(threads * loops, stats.bytesServed.sum());
        assertEquals(0L, stats.localHits.sum());
        assertEquals(threads * loops, stats.peerHits.sum());
        assertEquals(0L, stats.s3Hits.sum());
    }
}