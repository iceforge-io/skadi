package org.iceforge.skadi.dashboard;

import org.iceforge.skadi.query.QueryStatsRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class DashboardControllerTest {

    private MockMvc mockMvc;
    private QueryStatsRegistry stats;

    static class SampleStats {
        LongAdder bytesServed = new LongAdder();
        LongAdder localHits = new LongAdder();
        LongAdder peerHits = new LongAdder();
        LongAdder s3Hits = new LongAdder();
        Instant lastAccess;
    }

    @BeforeEach
    void setup() {
        stats = Mockito.mock(QueryStatsRegistry.class);
        DashboardController controller = new DashboardController(stats);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    void node_returnsUptimeAndNow() throws Exception {
        mockMvc.perform(get("/internal/dashboard/node"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.now").exists())
                .andExpect(jsonPath("$.uptimeMs").isNumber());
    }

    @Test
    void top_returnsTransformedEntries() throws Exception {
        QueryStatsRegistry.Stats s1 = new QueryStatsRegistry.Stats();
        s1.bytesServed.add(1000);
        s1.localHits.add(2);
        s1.peerHits.add(3);
        s1.s3Hits.add(4);
        s1.lastAccess = Instant.parse("2024-01-01T00:00:00Z");

        Map.Entry<String, QueryStatsRegistry.Stats> e1 = Map.entry("q1", s1);

        // noinspection unchecked
        Mockito.when(stats.topByBytes(anyInt()))
                .thenReturn((List<Map.Entry<String, QueryStatsRegistry.Stats>>) (List<?>) List.of(e1));

        mockMvc.perform(get("/internal/dashboard/queries/top").param("limit", "1"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$[0].queryId").value("q1"))
                .andExpect(jsonPath("$[0].bytesServed").value(1000))
                .andExpect(jsonPath("$[0].localHits").value(2))
                .andExpect(jsonPath("$[0].peerHits").value(3))
                .andExpect(jsonPath("$[0].s3Hits").value(4))
                .andExpect(jsonPath("$[0].lastAccess").value("2024-01-01T00:00:00Z"));
    }

    @Test
    void query_notFound_returnsFoundFalse() throws Exception {
        Mockito.when(stats.get(eq("missing"))).thenReturn(null);

        mockMvc.perform(get("/internal/dashboard/queries/missing"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.queryId").value("missing"))
                .andExpect(jsonPath("$.found").value(false));
    }

    @Test
    void query_found_returnsStats() throws Exception {

        QueryStatsRegistry.Stats realStats = new QueryStatsRegistry.Stats();
        realStats.bytesServed.add(42);
        realStats.localHits.add(1);
        realStats.peerHits.add(0);
        realStats.s3Hits.add(2);
        realStats.lastAccess = Instant.parse("2024-06-01T12:00:00Z");

        Mockito.when(stats.get(eq("abc"))).thenReturn(realStats);

        mockMvc.perform(get("/internal/dashboard/queries/abc"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.queryId").value("abc"))
                .andExpect(jsonPath("$.found").value(true))
                .andExpect(jsonPath("$.bytesServed").value(42))
                .andExpect(jsonPath("$.localHits").value(1))
                .andExpect(jsonPath("$.peerHits").value(0))
                .andExpect(jsonPath("$.s3Hits").value(2))
                .andExpect(jsonPath("$.lastAccess").value("2024-06-01T12:00:00Z"));
    }
}