package org.iceforge.skadi.semantic.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.cache.CacheContract;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.time.Clock;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SemanticExecutionFailure} classification and
 * the corresponding safe message behavior in {@link SkadiServerQueryExecutionService}.
 *
 * <p>Verifies that every failure path returns a stable, secret-free message
 * and that raw exception details do not propagate to callers.
 *
 * <p>No Spring context, no Databricks. Uses lightweight JDK HttpServer fakes
 * and direct circuit-breaker state setup.
 */
class SemanticExecutionFailureClassificationTest {

    private static final String SERVER_URL = "http://localhost:1"; // unreachable
    private static final ExecutionContext CTX = ExecutionContext.of("alice");
    private static final String SQL = "SELECT 1 FROM dual";

    // Credential-like keywords that must never appear in safe messages or returned results
    private static final String[] FORBIDDEN_KEYWORDS = {
            "jdbc:", "password", "secret", "token", "credential",
            "Exception", "stack", "Caused by", "at org.", "at java."
    };

    // ── SemanticExecutionFailure enum ─────────────────────────────────────────

    @Test
    void allFailures_haveNonBlankSafeMessage() {
        for (SemanticExecutionFailure f : SemanticExecutionFailure.values()) {
            String msg = f.safeMessage();
            assertNotNull(msg, f + ".safeMessage() must not be null");
            assertFalse(msg.isBlank(), f + ".safeMessage() must not be blank");
        }
    }

    @Test
    void safeMessages_containNoForbiddenKeywords() {
        for (SemanticExecutionFailure f : SemanticExecutionFailure.values()) {
            String msg = f.safeMessage().toLowerCase();
            for (String keyword : FORBIDDEN_KEYWORDS) {
                assertFalse(msg.contains(keyword.toLowerCase()),
                        f + ".safeMessage() must not contain '" + keyword + "'");
            }
        }
    }

    @Test
    void fromHealthStatus_disabled_returnsDisabled() {
        assertEquals(SemanticExecutionFailure.DISABLED,
                SemanticExecutionFailure.fromHealthStatus(SemanticExecutionHealthStatus.DISABLED));
    }

    @Test
    void fromHealthStatus_circuitOpen_returnsCircuitOpen() {
        assertEquals(SemanticExecutionFailure.CIRCUIT_OPEN,
                SemanticExecutionFailure.fromHealthStatus(SemanticExecutionHealthStatus.CIRCUIT_OPEN));
    }

    @Test
    void fromHealthStatus_healthy_returnsUnavailable() {
        assertEquals(SemanticExecutionFailure.UNAVAILABLE,
                SemanticExecutionFailure.fromHealthStatus(SemanticExecutionHealthStatus.HEALTHY));
    }

    @Test
    void fromHealthStatus_unavailable_returnsUnavailable() {
        assertEquals(SemanticExecutionFailure.UNAVAILABLE,
                SemanticExecutionFailure.fromHealthStatus(SemanticExecutionHealthStatus.UNAVAILABLE));
    }

    @Test
    void fromHealthStatus_timeout_returnsUnavailable() {
        assertEquals(SemanticExecutionFailure.UNAVAILABLE,
                SemanticExecutionFailure.fromHealthStatus(SemanticExecutionHealthStatus.TIMEOUT));
    }

    @Test
    void fromHealthStatus_failed_returnsUnavailable() {
        assertEquals(SemanticExecutionFailure.UNAVAILABLE,
                SemanticExecutionFailure.fromHealthStatus(SemanticExecutionHealthStatus.FAILED));
    }

    // ── Execute: disabled path ────────────────────────────────────────────────

    @Test
    void execute_disabled_returnsDisabledSafeMessage() {
        var cb = SemanticExecutionCircuitBreaker.disabled(SERVER_URL);
        var svc = service(SERVER_URL, cb);

        var result = svc.execute(request());

        assertEquals(ExecutionStatus.FAILED, result.status());
        assertEquals(SemanticExecutionFailure.DISABLED.safeMessage(), result.errorMessage());
        assertNoForbiddenKeywords(result.errorMessage());
    }

    // ── Execute: circuit-open path ────────────────────────────────────────────

    @Test
    void execute_circuitOpen_returnsCircuitOpenSafeMessage() {
        var cb = new SemanticExecutionCircuitBreaker(true, 1, 60_000L, SERVER_URL, Clock.systemUTC());
        cb.recordFailure("err", SemanticExecutionHealthStatus.UNAVAILABLE); // trips circuit at threshold=1

        var svc = service(SERVER_URL, cb);

        var result = svc.execute(request());

        assertEquals(ExecutionStatus.FAILED, result.status());
        assertEquals(SemanticExecutionFailure.CIRCUIT_OPEN.safeMessage(), result.errorMessage());
        assertNoForbiddenKeywords(result.errorMessage());
    }

    // ── Execute: connection refused (UNEXPECTED) ──────────────────────────────

    @Test
    void execute_connectionRefused_returnsUnexpectedSafeMessage() {
        var cb = SemanticExecutionCircuitBreaker.alwaysAllow(SERVER_URL);
        var svc = service(SERVER_URL, cb); // port 1 is unreachable

        var result = svc.execute(request());

        assertEquals(ExecutionStatus.FAILED, result.status());
        assertEquals(SemanticExecutionFailure.UNEXPECTED.safeMessage(), result.errorMessage());
        // Critical: raw exception message (which could contain JDBC URL/credentials) must NOT appear
        assertNoForbiddenKeywords(result.errorMessage());
    }

    @Test
    void execute_connectionRefused_errorMessage_containsNoServerUrl() {
        // The server URL (http://localhost:1) must not leak into the returned message
        var svc = service(SERVER_URL, SemanticExecutionCircuitBreaker.alwaysAllow(SERVER_URL));
        var result = svc.execute(request());

        assertFalse(result.errorMessage().contains("localhost"),
                "server URL must not appear in returned error message");
        assertFalse(result.errorMessage().contains(":1"),
                "server port must not appear in returned error message");
    }

    // ── Execute: unexpected HTTP status (UNAVAILABLE) ─────────────────────────

    @Test
    void execute_unexpectedHttpStatus_returnsUnavailableSafeMessage() throws Exception {
        String resp = "{\"queryId\":\"q1\",\"state\":\"UNKNOWN_STATE\"}";
        try (var fake = FakeServer.returning(503, resp)) {
            var svc = service(fake.baseUrl(), SemanticExecutionCircuitBreaker.alwaysAllow(fake.baseUrl()));
            var result = svc.execute(request());

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals(SemanticExecutionFailure.UNAVAILABLE.safeMessage(), result.errorMessage());
            assertNoForbiddenKeywords(result.errorMessage());
        }
    }

    // ── Execute: remote FAILED state (REMOTE_ERROR) ───────────────────────────

    @Test
    void execute_remoteFailedState_returnsRemoteErrorSafeMessage() throws Exception {
        // Server accepts 202 → status returns FAILED with a raw server message
        String submitResp = "{\"queryId\":\"q2\",\"state\":\"QUEUED\"}";
        String statusResp = "{\"queryId\":\"q2\",\"state\":\"FAILED\","
                + "\"message\":\"Databricks error: SQL parse error near 'FROM'\"}";

        try (var fake = FakeServer.asyncWithStatus(submitResp, statusResp)) {
            var svc = service(fake.baseUrl(), SemanticExecutionCircuitBreaker.alwaysAllow(fake.baseUrl()));
            var result = svc.execute(request());

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals(SemanticExecutionFailure.REMOTE_ERROR.safeMessage(), result.errorMessage());
            // Raw server message must NOT propagate to the caller
            assertFalse(result.errorMessage().contains("Databricks"),
                    "raw server error message must not appear in result");
            assertFalse(result.errorMessage().contains("SQL parse error"),
                    "raw server error detail must not appear in result");
        }
    }

    // ── Execute: remote CANCELED state (REMOTE_ERROR) ────────────────────────

    @Test
    void execute_remoteCanceledState_returnsRemoteErrorSafeMessage() throws Exception {
        String submitResp = "{\"queryId\":\"q3\",\"state\":\"QUEUED\"}";
        String statusResp = "{\"queryId\":\"q3\",\"state\":\"CANCELED\"}";

        try (var fake = FakeServer.asyncWithStatus(submitResp, statusResp)) {
            var svc = service(fake.baseUrl(), SemanticExecutionCircuitBreaker.alwaysAllow(fake.baseUrl()));
            var result = svc.execute(request());

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals(SemanticExecutionFailure.REMOTE_ERROR.safeMessage(), result.errorMessage());
            assertNoForbiddenKeywords(result.errorMessage());
        }
    }

    // ── Execute: poll timeout (TIMEOUT) ──────────────────────────────────────

    @Test
    void execute_pollTimeout_returnsTimeoutSafeMessage() throws Exception {
        // Submit accepted; status polls return RUNNING; maxWaitMs expires after one poll.
        String submitResp = "{\"queryId\":\"q4\",\"state\":\"QUEUED\"}";
        String statusResp = "{\"queryId\":\"q4\",\"state\":\"RUNNING\"}";

        try (var fake = FakeServer.asyncWithStatus(submitResp, statusResp)) {
            // pollIntervalMs=10 ensures deadline (5ms) is exceeded after one sleep
            var svc = new SkadiServerQueryExecutionService(
                    fake.baseUrl(), "jdbc:h2:mem:test", "sa", "",
                    new ObjectMapper(),
                    HttpClient.newHttpClient(),
                    /*pollIntervalMs=*/ 10L,
                    /*maxWaitMs=*/ 5L,
                    NoOpSemanticExecutionMetrics.INSTANCE,
                    SemanticExecutionCircuitBreaker.alwaysAllow(fake.baseUrl()));

            var result = svc.execute(request());

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertEquals(SemanticExecutionFailure.TIMEOUT.safeMessage(), result.errorMessage());
            // maxWaitMs value must not appear in returned message
            assertFalse(result.errorMessage().contains("5"),
                    "internal timeout duration must not appear in result");
            assertNoForbiddenKeywords(result.errorMessage());
        }
    }

    // ── Execute: success — unaffected ─────────────────────────────────────────

    @Test
    void execute_cacheHit_successUnchanged() throws Exception {
        String resp = "{\"queryId\":\"q5\",\"state\":\"SUCCEEDED\"}";
        try (var fake = FakeServer.returning(200, resp)) {
            var svc = service(fake.baseUrl(), SemanticExecutionCircuitBreaker.alwaysAllow(fake.baseUrl()));
            var result = svc.execute(request());

            assertEquals(ExecutionStatus.COMPLETED, result.status());
            assertNull(result.errorMessage(), "successful result must have no error message");
        }
    }

    @Test
    void execute_asyncSuccess_successUnchanged() throws Exception {
        String submitResp = "{\"queryId\":\"q6\",\"state\":\"QUEUED\"}";
        String statusResp = "{\"queryId\":\"q6\",\"state\":\"SUCCEEDED\"}";
        try (var fake = FakeServer.asyncWithStatus(submitResp, statusResp)) {
            var svc = service(fake.baseUrl(), SemanticExecutionCircuitBreaker.alwaysAllow(fake.baseUrl()));
            var result = svc.execute(request());

            assertEquals(ExecutionStatus.COMPLETED, result.status());
            assertNull(result.errorMessage());
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static QueryExecutionRequest request() {
        return QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none());
    }

    private static SkadiServerQueryExecutionService service(String baseUrl,
                                                             SemanticExecutionCircuitBreaker cb) {
        return new SkadiServerQueryExecutionService(
                baseUrl, "jdbc:h2:mem:test", "sa", "s3cr3t",
                new ObjectMapper(),
                HttpClient.newHttpClient(),
                /*pollIntervalMs=*/ 10L,
                /*maxWaitMs=*/ 5_000L,
                NoOpSemanticExecutionMetrics.INSTANCE,
                cb);
    }

    private static void assertNoForbiddenKeywords(String message) {
        if (message == null) return;
        String lower = message.toLowerCase();
        for (String kw : FORBIDDEN_KEYWORDS) {
            assertFalse(lower.contains(kw.toLowerCase()),
                    "error message must not contain '" + kw + "' — got: " + message);
        }
    }

    // ── Minimal fake HTTP server ──────────────────────────────────────────────

    static final class FakeServer implements AutoCloseable {

        private final com.sun.net.httpserver.HttpServer server;

        private FakeServer() throws Exception {
            server = com.sun.net.httpserver.HttpServer.create(
                    new java.net.InetSocketAddress(0), 0);
        }

        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() { server.stop(0); }

        static FakeServer returning(int status, String body) throws Exception {
            var fake = new FakeServer();
            fake.server.createContext("/api/v1/queries", exchange -> {
                exchange.getRequestBody().readAllBytes();
                byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }

        /** POST to /api/v1/queries returns 202 with submitResp; GET status returns statusResp. */
        static FakeServer asyncWithStatus(String submitResp, String statusResp) throws Exception {
            var fake = new FakeServer();
            fake.server.createContext("/api/v1/queries", exchange -> {
                String method = exchange.getRequestMethod();
                String payload;
                int code;
                if ("POST".equals(method)) {
                    exchange.getRequestBody().readAllBytes();
                    payload = submitResp;
                    code = 202;
                } else {
                    payload = statusResp;
                    code = 200;
                }
                byte[] bytes = payload.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(code, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }
    }
}
