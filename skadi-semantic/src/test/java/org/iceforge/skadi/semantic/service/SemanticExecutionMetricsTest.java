package org.iceforge.skadi.semantic.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.cache.CacheContract;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@link SkadiServerQueryExecutionService} calls the correct
 * {@link SemanticExecutionMetrics} counters for each execution path.
 *
 * <p>Uses hand-rolled JDK {@code HttpServer} fakes and a capturing
 * {@link SemanticExecutionMetrics} stub — no Mockito, no Databricks.
 */
class SemanticExecutionMetricsTest {

    private static final String SQL = "SELECT 1";
    private static final ExecutionContext CTX = ExecutionContext.of("alice");

    // ── NoOpSemanticExecutionMetrics ──────────────────────────────────────────

    @Test
    void noOp_allMethodsAreSilent() {
        var m = NoOpSemanticExecutionMetrics.INSTANCE;
        assertDoesNotThrow(() -> {
            m.recordAttempt();
            m.recordCacheHit();
            m.recordAsyncAccepted();
            m.recordSuccess();
            m.recordFailure();
            m.recordTimeout();
            m.recordError();
        });
    }

    // ── Cache-hit path ────────────────────────────────────────────────────────

    @Test
    void cacheHit_recordsAttemptAndCacheHit() throws Exception {
        var capture = new CapturingMetrics();
        String resp = "{\"queryId\":\"q1\",\"state\":\"SUCCEEDED\",\"resultUrl\":\"/r\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeServer.returning(200, resp)) {
            service(fake.baseUrl(), capture)
                    .execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));
        }

        assertEquals(1, capture.attempts);
        assertEquals(1, capture.cacheHits);
        assertEquals(0, capture.asyncAccepted);
        assertEquals(0, capture.successes);
        assertEquals(0, capture.failures);
        assertEquals(0, capture.timeouts);
        assertEquals(0, capture.errors);
    }

    // ── Async path ────────────────────────────────────────────────────────────

    @Test
    void asyncQuery_recordsAttemptAsyncAndSuccess() throws Exception {
        var capture = new CapturingMetrics();
        AtomicInteger pollCount = new AtomicInteger();
        String submitResp = "{\"queryId\":\"q2\",\"state\":\"QUEUED\",\"resultUrl\":\"/r\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeServer.asyncQuery(submitResp, pollCount)) {
            service(fake.baseUrl(), capture)
                    .execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));
        }

        assertEquals(1, capture.attempts);
        assertEquals(0, capture.cacheHits);
        assertEquals(1, capture.asyncAccepted);
        assertEquals(1, capture.successes);
        assertEquals(0, capture.failures);
        assertEquals(0, capture.timeouts);
        assertEquals(0, capture.errors);
    }

    // ── Failure path ──────────────────────────────────────────────────────────

    @Test
    void unexpectedSubmitResponse_recordsFailure() throws Exception {
        var capture = new CapturingMetrics();
        // HTTP 200 but state=FAILED is not the cache-hit pattern (200+SUCCEEDED);
        // falls through to the unexpected-response branch.
        String resp = "{\"queryId\":\"q3\",\"state\":\"FAILED\",\"resultUrl\":\"/r\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeServer.returning(200, resp)) {
            service(fake.baseUrl(), capture)
                    .execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));
        }

        assertEquals(1, capture.attempts);
        assertEquals(0, capture.cacheHits);
        assertEquals(1, capture.failures);
        assertEquals(0, capture.errors);
    }

    // ── Error path ────────────────────────────────────────────────────────────

    @Test
    void connectionRefused_recordsError() {
        var capture = new CapturingMetrics();
        // Port 1 is unreachable — HTTP client will throw an exception.
        var svc = service("http://localhost:1", capture);

        var result = svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

        assertEquals(1, capture.attempts);
        assertEquals(0, capture.cacheHits);
        assertEquals(1, capture.errors);
        assertEquals(ExecutionStatus.FAILED, result.status());
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static SkadiServerQueryExecutionService service(String baseUrl, SemanticExecutionMetrics m) {
        return new SkadiServerQueryExecutionService(
                baseUrl, "jdbc:h2:mem:test", "sa", "",
                new ObjectMapper(),
                HttpClient.newHttpClient(),
                /*pollIntervalMs=*/ 10L,
                /*maxWaitMs=*/ 5_000L,
                m,
                SemanticExecutionCircuitBreaker.alwaysAllow(baseUrl));
    }

    static final class CapturingMetrics implements SemanticExecutionMetrics {
        int attempts, cacheHits, asyncAccepted, successes, failures, timeouts, errors;

        @Override public void recordAttempt()       { attempts++; }
        @Override public void recordCacheHit()      { cacheHits++; }
        @Override public void recordAsyncAccepted() { asyncAccepted++; }
        @Override public void recordSuccess()       { successes++; }
        @Override public void recordFailure()       { failures++; }
        @Override public void recordTimeout()       { timeouts++; }
        @Override public void recordError()         { errors++; }
    }

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
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }

        static FakeServer asyncQuery(String submitResp, AtomicInteger pollCount) throws Exception {
            var fake = new FakeServer();
            fake.server.createContext("/api/v1/queries", exchange -> {
                String path   = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();
                String payload;
                int status;
                if ("POST".equals(method) && "/api/v1/queries".equals(path)) {
                    exchange.getRequestBody().readAllBytes();
                    payload = submitResp;
                    status  = 202;
                } else {
                    int count = pollCount.incrementAndGet();
                    String state = count >= 2 ? "SUCCEEDED" : "RUNNING";
                    payload = "{\"queryId\":\"q2\",\"state\":\"" + state + "\"}";
                    status  = 200;
                }
                byte[] bytes = payload.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(status, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }
    }
}
