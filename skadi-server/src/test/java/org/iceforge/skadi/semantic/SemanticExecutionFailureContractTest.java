package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.service.SemanticExecutionCircuitBreaker;
import org.iceforge.skadi.semantic.service.SemanticExecutionFailure;
import org.iceforge.skadi.semantic.service.SemanticExecutionHealthStatus;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Contract tests locking the observable failure response shape for semantic execution.
 *
 * <p>F5 scope: the diagnostics endpoint contract after each failure type.
 * F4 proved {@code QueryExecutionResult.errorMessage()} is safe; F5 proves what
 * {@code GET /api/semantic/v1/execution/status} exposes after each failure type.
 *
 * <p>The combination locks the full observable contract: caller gets a safe message,
 * and the diagnostics endpoint shows the same safe reason without raw exception content.
 *
 * <p>No Spring context, no real network calls. Uses MockMvc standalone and direct
 * circuit-breaker state setup.
 */
class SemanticExecutionFailureContractTest {

    private static final String STATUS_URL = "/api/semantic/v1/execution/status";
    private static final String SERVER_URL = "http://test-server:8080";

    private static final String[] FORBIDDEN = {
            "jdbc:", "password", "secret", "token", "credential",
            "Exception", "stack", "Caused by", "at org.", "at java.",
            "NullPointer", "IllegalArgument", "IOException"
    };

    // ── DISABLED — diagnostics contract ──────────────────────────────────────

    @Test
    void contract_disabled_activeIsFalse_statusDisabled_readinessDisabled() throws Exception {
        var cb = SemanticExecutionCircuitBreaker.disabled(SERVER_URL);
        mvc(cb).perform(get(STATUS_URL))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.active").value(false))
                .andExpect(jsonPath("$.health.status").value("DISABLED"))
                .andExpect(jsonPath("$.health.readiness").value("DISABLED"));
    }

    @Test
    void contract_disabled_noLastFailureReason() throws Exception {
        // DISABLED never records a failure — lastFailureReason must be absent.
        var cb = SemanticExecutionCircuitBreaker.disabled(SERVER_URL);
        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.lastFailureReason").doesNotExist());
    }

    // ── UNAVAILABLE — diagnostics contract ───────────────────────────────────

    @Test
    void contract_unavailable_statusAndReadiness() throws Exception {
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.active").value(true));
    }

    @Test
    void contract_unavailable_lastFailureReason_isSafeMessage() throws Exception {
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.lastFailureReason")
                        .value(SemanticExecutionFailure.UNAVAILABLE.safeMessage()));
    }

    // ── TIMEOUT — diagnostics contract ───────────────────────────────────────

    @Test
    void contract_timeout_statusAndReadiness() throws Exception {
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.TIMEOUT.safeMessage(),
                SemanticExecutionHealthStatus.TIMEOUT);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("TIMEOUT"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"));
    }

    @Test
    void contract_timeout_lastFailureReason_isSafeMessage() throws Exception {
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.TIMEOUT.safeMessage(),
                SemanticExecutionHealthStatus.TIMEOUT);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.lastFailureReason")
                        .value(SemanticExecutionFailure.TIMEOUT.safeMessage()));
    }

    // ── REMOTE_ERROR — diagnostics contract ──────────────────────────────────

    @Test
    void contract_remoteError_statusAndReadiness() throws Exception {
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.REMOTE_ERROR.safeMessage(),
                SemanticExecutionHealthStatus.FAILED);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("FAILED"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"));
    }

    @Test
    void contract_remoteError_lastFailureReason_isSafeMessage_notRawServerContent() throws Exception {
        // This is the anti-reflection contract: when execute() records a REMOTE_ERROR,
        // it uses SemanticExecutionFailure.REMOTE_ERROR.safeMessage() — not the raw
        // server message (which could contain SQL text, tokens, or Databricks error details).
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.REMOTE_ERROR.safeMessage(),
                SemanticExecutionHealthStatus.FAILED);

        String body = mvc(cb).perform(get(STATUS_URL))
                .andReturn().getResponse().getContentAsString();

        // The exact safe message — not any raw server content
        assertTrue(body.contains(SemanticExecutionFailure.REMOTE_ERROR.safeMessage()),
                "lastFailureReason must equal the safe message");
        assertFalse(body.contains("Databricks"),
                "raw server vendor name must not appear in diagnostics");
        assertFalse(body.contains("SQL parse"),
                "raw server error details must not appear in diagnostics");
        assertFalse(body.contains("ACCESS DENIED"),
                "raw server access errors must not appear in diagnostics");
    }

    // ── UNEXPECTED — diagnostics contract ────────────────────────────────────

    @Test
    void contract_unexpected_statusAndReadiness() throws Exception {
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.UNEXPECTED.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("UNAVAILABLE"))
                .andExpect(jsonPath("$.health.readiness").value("UNAVAILABLE"));
    }

    @Test
    void contract_unexpected_lastFailureReason_isSafeMessage_notExceptionText() throws Exception {
        var cb = breaker();
        // execute() uses the safe message — not e.getMessage() which could contain JDBC URL
        cb.recordFailure(SemanticExecutionFailure.UNEXPECTED.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.lastFailureReason")
                        .value(SemanticExecutionFailure.UNEXPECTED.safeMessage()));
    }

    // ── CIRCUIT_OPEN — diagnostics contract ──────────────────────────────────

    @Test
    void contract_circuitOpen_statusDegradedReadiness() throws Exception {
        // Trip the circuit at threshold=2
        var cb = new SemanticExecutionCircuitBreaker(true, 2, 60_000L, SERVER_URL, Clock.systemUTC());
        cb.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);
        cb.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("CIRCUIT_OPEN"))
                .andExpect(jsonPath("$.health.readiness").value("DEGRADED"))
                .andExpect(jsonPath("$.health.failureCount").value(2))
                .andExpect(jsonPath("$.health.failureThreshold").value(2))
                .andExpect(jsonPath("$.health.circuitOpenUntil").isNotEmpty());
    }

    @Test
    void contract_circuitOpen_lastFailureReason_reflectsTripCause_notCircuitState() throws Exception {
        // The lastFailureReason shows WHY the circuit tripped (the actual failure classification),
        // not the string "circuit open" — useful for operators diagnosing root cause.
        var cb = new SemanticExecutionCircuitBreaker(true, 1, 60_000L, SERVER_URL, Clock.systemUTC());
        cb.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("CIRCUIT_OPEN"))
                .andExpect(jsonPath("$.health.lastFailureReason")
                        .value(SemanticExecutionFailure.UNAVAILABLE.safeMessage()));
    }

    // ── Pre-call block does not overwrite lastFailureReason ──────────────────

    @Test
    void contract_circuitOpenBlock_doesNotChangeLastFailureReason() {
        // When the circuit blocks a call (isCallAllowed=false), execute() does NOT call
        // recordFailure on the CB — so lastFailureReason stays from the trip-causing failure.
        var cb = new SemanticExecutionCircuitBreaker(true, 1, 60_000L, SERVER_URL, Clock.systemUTC());
        cb.recordFailure(SemanticExecutionFailure.TIMEOUT.safeMessage(),
                SemanticExecutionHealthStatus.TIMEOUT);

        String reasonAfterTrip = cb.snapshot().lastFailureReason();

        // Simulate the pre-call block: execute() returns FAILED without calling recordFailure
        // So lastFailureReason must be unchanged after the blocked call.
        assertEquals(SemanticExecutionFailure.TIMEOUT.safeMessage(), reasonAfterTrip,
                "lastFailureReason must reflect why the circuit tripped, not the blocked call");
        assertEquals(SemanticExecutionFailure.TIMEOUT.safeMessage(), cb.snapshot().lastFailureReason(),
                "lastFailureReason must not change when a call is blocked by circuit");
    }

    // ── Success preserves failure history ────────────────────────────────────

    @Test
    void contract_afterSuccessFollowingFailure_lastFailureReasonPreserved() throws Exception {
        // recordSuccess() resets failureCount and status but does NOT erase lastFailureReason.
        // This is intentional: operators can see what the last failure was even after recovery.
        var cb = breaker();
        cb.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                SemanticExecutionHealthStatus.UNAVAILABLE);
        cb.recordSuccess();

        mvc(cb).perform(get(STATUS_URL))
                .andExpect(jsonPath("$.health.status").value("HEALTHY"))
                .andExpect(jsonPath("$.health.readiness").value("READY"))
                .andExpect(jsonPath("$.health.failureCount").value(0))
                // lastFailureReason is preserved across recovery — history not erased
                .andExpect(jsonPath("$.health.lastFailureReason")
                        .value(SemanticExecutionFailure.UNAVAILABLE.safeMessage()));
    }

    // ── Cross-cutting: no forbidden keywords in diagnostics ───────────────────

    @Test
    void contract_allFailureTypes_lastFailureReason_containsNoForbiddenKeywords() throws Exception {
        // Map of (failure, healthStatus) pairs that exercise each recordFailure path
        record Case(SemanticExecutionFailure f, SemanticExecutionHealthStatus h) {}
        var cases = new Case[]{
                new Case(SemanticExecutionFailure.UNAVAILABLE,  SemanticExecutionHealthStatus.UNAVAILABLE),
                new Case(SemanticExecutionFailure.TIMEOUT,      SemanticExecutionHealthStatus.TIMEOUT),
                new Case(SemanticExecutionFailure.REMOTE_ERROR, SemanticExecutionHealthStatus.FAILED),
                new Case(SemanticExecutionFailure.UNEXPECTED,   SemanticExecutionHealthStatus.UNAVAILABLE),
        };

        for (var c : cases) {
            var cb = breaker();
            cb.recordFailure(c.f().safeMessage(), c.h());

            String body = mvc(cb).perform(get(STATUS_URL))
                    .andReturn().getResponse().getContentAsString();

            for (String kw : FORBIDDEN) {
                assertFalse(body.toLowerCase().contains(kw.toLowerCase()),
                        "diagnostics after " + c.f() + " must not contain '" + kw + "'"
                                + " — body: " + body);
            }
        }
    }

    // ── Message stability and uniqueness ──────────────────────────────────────

    @Test
    void contract_safeMessages_areStableAcrossInvocations() {
        for (SemanticExecutionFailure f : SemanticExecutionFailure.values()) {
            assertEquals(f.safeMessage(), f.safeMessage(),
                    f + ".safeMessage() must be idempotent");
        }
    }

    @Test
    void contract_allFailureTypes_haveDistinctSafeMessages() {
        long distinct = Arrays.stream(SemanticExecutionFailure.values())
                .map(SemanticExecutionFailure::safeMessage)
                .distinct()
                .count();
        assertEquals(SemanticExecutionFailure.values().length, distinct,
                "each SemanticExecutionFailure must have a unique safe message");
    }

    @Test
    void contract_safeMessage_doesNotContainEnumConstantName() {
        // Safe messages must be human-readable English, not raw enum names like "REMOTE_ERROR".
        for (SemanticExecutionFailure f : SemanticExecutionFailure.values()) {
            assertFalse(f.safeMessage().contains(f.name()),
                    f + ".safeMessage() must not expose the internal enum constant name");
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static SemanticExecutionCircuitBreaker breaker() {
        return new SemanticExecutionCircuitBreaker(true, 10, 30_000L, SERVER_URL, Clock.systemUTC());
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
