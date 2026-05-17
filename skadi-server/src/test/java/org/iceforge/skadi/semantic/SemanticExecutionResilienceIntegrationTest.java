package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.service.SemanticExecutionCircuitBreaker;
import org.iceforge.skadi.semantic.service.SemanticExecutionHealthStatus;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Integration tests for Lane F semantic execution resilience, readiness, and diagnostics.
 *
 * <p>Covers the full diagnostics surface — property binding, bean creation, and
 * end-to-end controller responses across healthy, failure, circuit-open, and
 * recovery states.
 *
 * <p>No Spring context, no real network calls, no Databricks. Uses MockMvc standalone
 * and injectable {@link Clock} for deterministic circuit-breaker state control.
 */
class SemanticExecutionResilienceIntegrationTest {

    private static final String STATUS_URL = "/api/semantic/v1/execution/status";
    private static final String SERVER_URL = "http://skadi-test:8080";
    private static final int THRESHOLD = 3;
    private static final long OPEN_MS = 1_000L;

    // ── Property binding ──────────────────────────────────────────────────────

    @Test
    void properties_defaultValues_bindCorrectly() {
        // Verify defaults without any YAML override
        var props = new SemanticExecutionProperties();
        assertTrue(props.isEnabled());
        assertTrue(props.getCircuitBreaker().isEnabled());
        assertEquals(3, props.getCircuitBreaker().getFailureThreshold());
        assertEquals(30_000L, props.getCircuitBreaker().getOpenDurationMs());
    }

    @Test
    void properties_customCircuitBreakerValues_bindFromYamlStyle() {
        var source = new MapConfigurationPropertySource(Map.of(
                "skadi.semantic.execution.enabled", "true",
                "skadi.semantic.execution.server-url", "http://custom-server:9090",
                "skadi.semantic.execution.datasource-id", "analytics-ds",
                "skadi.semantic.execution.circuit-breaker.enabled", "true",
                "skadi.semantic.execution.circuit-breaker.failure-threshold", "7",
                "skadi.semantic.execution.circuit-breaker.open-duration-ms", "45000"
        ));
        var props = new Binder(source).bindOrCreate(
                "skadi.semantic.execution", SemanticExecutionProperties.class);

        assertTrue(props.isEnabled());
        assertEquals("http://custom-server:9090", props.getServerUrl());
        assertEquals("analytics-ds", props.getDatasourceId());
        assertTrue(props.getCircuitBreaker().isEnabled());
        assertEquals(7, props.getCircuitBreaker().getFailureThreshold());
        assertEquals(45_000L, props.getCircuitBreaker().getOpenDurationMs());
    }

    @Test
    void properties_executionDisabled_bindsCorrectly() {
        var source = new MapConfigurationPropertySource(Map.of(
                "skadi.semantic.execution.enabled", "false"
        ));
        var props = new Binder(source).bindOrCreate(
                "skadi.semantic.execution", SemanticExecutionProperties.class);

        assertFalse(props.isEnabled());
    }

    @Test
    void properties_circuitBreakerDisabled_bindsCorrectly() {
        var source = new MapConfigurationPropertySource(Map.of(
                "skadi.semantic.execution.circuit-breaker.enabled", "false",
                "skadi.semantic.execution.circuit-breaker.failure-threshold", "1",
                "skadi.semantic.execution.circuit-breaker.open-duration-ms", "500"
        ));
        var props = new Binder(source).bindOrCreate(
                "skadi.semantic.execution", SemanticExecutionProperties.class);

        assertFalse(props.getCircuitBreaker().isEnabled());
        assertEquals(1, props.getCircuitBreaker().getFailureThreshold());
        assertEquals(500L, props.getCircuitBreaker().getOpenDurationMs());
    }

    // ── Bean creation from SemanticContractConfiguration ─────────────────────

    @Test
    void circuitBreakerBean_enabled_isHealthyByDefault() {
        var config = new SemanticContractConfiguration();
        var execProps = enabledExecProps();

        var cb = config.semanticExecutionCircuitBreaker(execProps);

        assertTrue(cb.isCallAllowed());
        assertEquals(SemanticExecutionHealthStatus.HEALTHY, cb.snapshot().status());
    }

    @Test
    void circuitBreakerBean_usesConfiguredThreshold() {
        var config = new SemanticContractConfiguration();
        var execProps = enabledExecProps();
        execProps.getCircuitBreaker().setFailureThreshold(5);

        var cb = config.semanticExecutionCircuitBreaker(execProps);

        assertEquals(5, cb.snapshot().failureThreshold());
    }

    @Test
    void circuitBreakerBean_executionDisabled_createsDisabledBreaker() {
        var config = new SemanticContractConfiguration();
        var execProps = new SemanticExecutionProperties();
        execProps.setEnabled(false);
        execProps.setServerUrl(SERVER_URL);

        var cb = config.semanticExecutionCircuitBreaker(execProps);

        assertFalse(cb.isCallAllowed());
        assertEquals(SemanticExecutionHealthStatus.DISABLED, cb.snapshot().status());
    }

    @Test
    void circuitBreakerBean_serverUrlExposedInSnapshot() {
        var config = new SemanticContractConfiguration();
        var execProps = enabledExecProps();
        execProps.setServerUrl("http://specific-server:9999");

        var cb = config.semanticExecutionCircuitBreaker(execProps);

        assertEquals("http://specific-server:9999", cb.snapshot().skadiServerBaseUrl());
    }

    // ── Diagnostics: failure and circuit-open states ──────────────────────────

    @Test
    void diagnostics_afterSingleFailure_readinessIsUnavailable() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));
        cb.recordFailure("server error", SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.health.status").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.health.failureCount").value(1))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void diagnostics_timeout_readinessIsUnavailable() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));
        cb.recordFailure("query timed out after 300000ms", SemanticExecutionHealthStatus.TIMEOUT);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("TIMEOUT"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void diagnostics_serverFailedState_readinessIsUnavailable() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));
        cb.recordFailure("query failed on server", SemanticExecutionHealthStatus.FAILED);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("FAILED"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"));
    }

    @Test
    void diagnostics_afterThresholdFailures_circuitOpenAndDegraded() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("connection refused", SemanticExecutionHealthStatus.UNAVAILABLE);
        }

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.health.status").value("CIRCUIT_OPEN"))
                .andExpect(jsonPath("$.health.readiness").value("DEGRADED"))
                .andExpect(jsonPath("$.health.failureCount").value(THRESHOLD))
                .andExpect(jsonPath("$.health.failureThreshold").value(THRESHOLD))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void diagnostics_circuitOpen_exposesOpenUntilTimestamp() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.circuitOpenUntil").isNotEmpty());
    }

    @Test
    void diagnostics_failureCount_incrementsInResponse() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.failureCount").value(0));

        cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.failureCount").value(2));
    }

    @Test
    void diagnostics_lastFailureReasonExposed() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));
        cb.recordFailure("Connection refused to http://skadi-test:8080", SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.lastFailureReason")
                        .value("Connection refused to http://skadi-test:8080"));
    }

    // ── Diagnostics: full lifecycle (failures → circuit open → probe → recovery) ──

    @Test
    void diagnostics_probeAfterWindowExpiry_resetsToHealthyAndReady() throws Exception {
        Instant base = Instant.parse("2026-05-17T10:00:00Z");
        Instant[] now = {base};
        Clock clock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now[0]; }
        };

        var cb = new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, SERVER_URL, clock);

        // Trip the circuit
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("server down", SemanticExecutionHealthStatus.UNAVAILABLE);
        }
        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.readiness").value("DEGRADED"));

        // Advance past open window and allow probe
        now[0] = base.plusMillis(OPEN_MS + 1);
        cb.isCallAllowed(); // probe call — transitions to HEALTHY
        cb.recordSuccess(); // probe succeeds

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.health.status").value("HEALTHY"))
                .andExpect(jsonPath("$.health.readiness").value("READY"))
                .andExpect(jsonPath("$.health.failureCount").value(0))
                .andExpect(jsonPath("$.health.circuitOpenUntil").doesNotExist());
    }

    @Test
    void diagnostics_probeFailure_reOpensCircuit() throws Exception {
        Instant base = Instant.parse("2026-05-17T10:00:00Z");
        Instant[] now = {base};
        Clock clock = new Clock() {
            @Override public ZoneOffset getZone() { return ZoneOffset.UTC; }
            @Override public Clock withZone(java.time.ZoneId zone) { return this; }
            @Override public Instant instant() { return now[0]; }
        };

        var cb = new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, SERVER_URL, clock);
        for (int i = 0; i < THRESHOLD; i++) {
            cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE);
        }

        // Probe window expires and probe fails
        now[0] = base.plusMillis(OPEN_MS + 1);
        cb.isCallAllowed(); // probe → HEALTHY temporarily
        cb.recordFailure("still down", SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("CIRCUIT_OPEN"))
                .andExpect(jsonPath("$.health.readiness").value("DEGRADED"))
                .andExpect(jsonPath("$.health.circuitOpenUntil").isNotEmpty());
    }

    // ── Diagnostics: secret safety ────────────────────────────────────────────

    @Test
    void diagnostics_noSecretsInResponse() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.password").doesNotExist())
                .andExpect(jsonPath("$.jdbcPassword").doesNotExist())
                .andExpect(jsonPath("$.secret").doesNotExist())
                .andExpect(jsonPath("$.token").doesNotExist())
                .andExpect(jsonPath("$.credentials").doesNotExist());
    }

    @Test
    void diagnostics_serverUrlExposed_datasourceIdExposed_noCredentials() throws Exception {
        var cb = breaker(Clock.fixed(Instant.now(), ZoneOffset.UTC));

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.serverUrl").value(SERVER_URL))
                .andExpect(jsonPath("$.datasourceId").value("test-ds"))
                .andExpect(jsonPath("$.jdbcUrl").doesNotExist())
                .andExpect(jsonPath("$.username").doesNotExist());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static SemanticExecutionCircuitBreaker breaker(Clock clock) {
        return new SemanticExecutionCircuitBreaker(true, THRESHOLD, OPEN_MS, SERVER_URL, clock);
    }

    private static SemanticExecutionProperties enabledExecProps() {
        var props = new SemanticExecutionProperties();
        props.setEnabled(true);
        props.setServerUrl(SERVER_URL);
        return props;
    }

    private static MockMvc mvc(SemanticExecutionCircuitBreaker cb) {
        var execProps = new SemanticExecutionProperties();
        execProps.setServerUrl(SERVER_URL);
        execProps.setDatasourceId("test-ds");

        var controller = new SemanticExecutionDiagnosticsController(
                execProps, new SemanticExecutionMetricsRegistry(), cb);
        return MockMvcBuilders.standaloneSetup(controller).build();
    }
}
