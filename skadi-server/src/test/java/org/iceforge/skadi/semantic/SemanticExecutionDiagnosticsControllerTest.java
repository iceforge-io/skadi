package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.service.SemanticExecutionCircuitBreaker;
import org.iceforge.skadi.semantic.service.SemanticExecutionHealthStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for {@link SemanticExecutionDiagnosticsController}.
 * Uses MockMvc standalone — no Spring context, no Databricks, no execution.
 */
class SemanticExecutionDiagnosticsControllerTest {

    private static final String URL = "/api/semantic/v1/execution/status";

    private static final String SERVER_URL = "http://skadi-server:8080";

    private MockMvc mockMvc;
    private SemanticExecutionMetricsRegistry metricsRegistry;
    private SemanticExecutionCircuitBreaker circuitBreaker;

    @BeforeEach
    void setUp() {
        metricsRegistry = new SemanticExecutionMetricsRegistry();
        circuitBreaker = SemanticExecutionCircuitBreaker.alwaysAllow(SERVER_URL);

        var execProps = new SemanticExecutionProperties();
        execProps.setServerUrl(SERVER_URL);
        execProps.setDatasourceId("prod-databricks");

        var controller = new SemanticExecutionDiagnosticsController(execProps, metricsRegistry, circuitBreaker);
        mockMvc = MockMvcBuilders
                .standaloneSetup(controller)
                .build();
    }

    // ── Status shape ──────────────────────────────────────────────────────────

    @Test
    void status_returnsHttp200() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(status().isOk());
    }

    @Test
    void status_activeIsTrue() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void status_exposesConfiguredServerUrl() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.serverUrl").value("http://skadi-server:8080"));
    }

    @Test
    void status_exposesConfiguredDatasourceId() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.datasourceId").value("prod-databricks"));
    }

    // ── Metrics shape ─────────────────────────────────────────────────────────

    @Test
    void status_metricsStartAtZero() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.metrics.attempts").value(0))
                .andExpect(jsonPath("$.metrics.cacheHits").value(0))
                .andExpect(jsonPath("$.metrics.asyncAccepted").value(0))
                .andExpect(jsonPath("$.metrics.successes").value(0))
                .andExpect(jsonPath("$.metrics.failures").value(0))
                .andExpect(jsonPath("$.metrics.timeouts").value(0))
                .andExpect(jsonPath("$.metrics.errors").value(0));
    }

    @Test
    void status_metricsReflectRecordedActivity() throws Exception {
        metricsRegistry.recordAttempt();
        metricsRegistry.recordAttempt();
        metricsRegistry.recordCacheHit();
        metricsRegistry.recordAttempt();
        metricsRegistry.recordAsyncAccepted();
        metricsRegistry.recordSuccess();

        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.metrics.attempts").value(3))
                .andExpect(jsonPath("$.metrics.cacheHits").value(1))
                .andExpect(jsonPath("$.metrics.asyncAccepted").value(1))
                .andExpect(jsonPath("$.metrics.successes").value(1))
                .andExpect(jsonPath("$.metrics.failures").value(0));
    }

    @Test
    void status_failureCounterInResponse() throws Exception {
        metricsRegistry.recordAttempt();
        metricsRegistry.recordFailure();

        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.metrics.failures").value(1))
                .andExpect(jsonPath("$.metrics.successes").value(0));
    }

    @Test
    void status_timeoutAndErrorCounters() throws Exception {
        metricsRegistry.recordTimeout();
        metricsRegistry.recordError();

        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.metrics.timeouts").value(1))
                .andExpect(jsonPath("$.metrics.errors").value(1));
    }

    // ── Health snapshot shape ─────────────────────────────────────────────────

    @Test
    void status_healthStatusIsHealthy() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.health.status").value("HEALTHY"));
    }

    @Test
    void status_healthFailureCountStartsAtZero() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.health.failureCount").value(0));
    }

    @Test
    void status_healthExposesFailureThreshold() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.health.failureThreshold").isNumber());
    }

    @Test
    void status_healthNullTimestampsOmitted() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.health.lastSuccessAt").doesNotExist())
                .andExpect(jsonPath("$.health.lastFailureAt").doesNotExist())
                .andExpect(jsonPath("$.health.circuitOpenUntil").doesNotExist());
    }

    @Test
    void status_disabled_activeIsFalse() throws Exception {
        var execProps = new SemanticExecutionProperties();
        execProps.setServerUrl(SERVER_URL);
        execProps.setDatasourceId("prod-databricks");

        var disabledCb = SemanticExecutionCircuitBreaker.disabled(SERVER_URL);
        var controller = new SemanticExecutionDiagnosticsController(
                execProps, new SemanticExecutionMetricsRegistry(), disabledCb);
        var mvc = MockMvcBuilders.standaloneSetup(controller).build();

        mvc.perform(get(URL))
                .andExpect(jsonPath("$.active").value(false))
                .andExpect(jsonPath("$.health.status").value(
                        SemanticExecutionHealthStatus.DISABLED.name()));
    }

    @Test
    void status_noSecretsExposed() throws Exception {
        mockMvc.perform(get(URL))
                .andExpect(jsonPath("$.password").doesNotExist())
                .andExpect(jsonPath("$.jdbcPassword").doesNotExist())
                .andExpect(jsonPath("$.token").doesNotExist());
    }
}
