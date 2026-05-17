package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.cache.CacheContract;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;
import org.iceforge.skadi.semantic.cache.CacheLookupRequest;
import org.iceforge.skadi.semantic.cache.CacheWriteRequest;
import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Structural, validation, and no-op behavior tests for service boundary types.
 * No Spring context, no external services, no JSON serialization.
 */
class ServiceBoundaryTest {

    static final String SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";
    static final ExecutionContext CTX = ExecutionContext.of("alice");
    static final CacheIdentity IDENTITY = new CacheIdentity(SQL, "alice", null);

    // ── ExecutionContext ───────────────────────────────────────────────────────

    @Test
    void context_holdsFields() {
        var ctx = new ExecutionContext("alice", "qid-001", "pgwire");
        assertEquals("alice",   ctx.principalName());
        assertEquals("qid-001", ctx.correlationId());
        assertEquals("pgwire",  ctx.sourceSystem());
    }

    @Test
    void context_anonymous_hasEmptyPrincipal() {
        var ctx = ExecutionContext.anonymous();
        assertEquals("", ctx.principalName());
        assertNull(ctx.correlationId());
        assertNull(ctx.sourceSystem());
    }

    @Test
    void context_of_setsOnlyPrincipal() {
        var ctx = ExecutionContext.of("bob");
        assertEquals("bob", ctx.principalName());
        assertNull(ctx.correlationId());
    }

    @Test
    void context_rejectsNullPrincipal() {
        assertThrows(NullPointerException.class,
                () -> new ExecutionContext(null, null, null));
    }

    @Test
    void context_allowsEmptyPrincipal() {
        assertDoesNotThrow(() -> new ExecutionContext("", null, null));
    }

    // ── QueryExecutionRequest ─────────────────────────────────────────────────

    @Test
    void executionRequest_sqlOnly_factory() {
        var req = QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none());
        assertEquals(SQL,  req.sql());
        assertNull(req.semanticContractName());
        assertEquals(CTX, req.context());
    }

    @Test
    void executionRequest_semanticFirst() {
        var req = new QueryExecutionRequest(CTX, null, "mxl_risk", CacheContract.ttl(300L));
        assertNull(req.sql());
        assertEquals("mxl_risk", req.semanticContractName());
    }

    @Test
    void executionRequest_bothProvided_isValid() {
        assertDoesNotThrow(() ->
                new QueryExecutionRequest(CTX, SQL, "mxl_risk", CacheContract.none()));
    }

    @Test
    void executionRequest_neitherProvided_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryExecutionRequest(CTX, null, null, CacheContract.none()));
        assertThrows(IllegalArgumentException.class,
                () -> new QueryExecutionRequest(CTX, "", "  ", CacheContract.none()));
    }

    @Test
    void executionRequest_rejectsNullContext() {
        assertThrows(NullPointerException.class,
                () -> new QueryExecutionRequest(null, SQL, null, CacheContract.none()));
    }

    // ── QueryExecutionResult ──────────────────────────────────────────────────

    @Test
    void executionResult_completed_factory() {
        var r = QueryExecutionResult.completed(CTX, IDENTITY, CacheEntryState.ABSENT);
        assertEquals(ExecutionStatus.COMPLETED, r.status());
        assertEquals(IDENTITY,                  r.cacheIdentity());
        assertNull(r.outputShape());
        assertNull(r.errorMessage());
    }

    @Test
    void executionResult_cacheHit_factory() {
        var r = QueryExecutionResult.cacheHit(CTX, IDENTITY);
        assertEquals(ExecutionStatus.CACHE_HIT,  r.status());
        assertEquals(CacheEntryState.FRESH,      r.cacheState());
    }

    @Test
    void executionResult_failed_factory() {
        var r = QueryExecutionResult.failed(CTX, IDENTITY, "timeout");
        assertEquals(ExecutionStatus.FAILED, r.status());
        assertEquals("timeout",              r.errorMessage());
    }

    @Test
    void executionResult_nonFailed_rejectsErrorMessage() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryExecutionResult(CTX, ExecutionStatus.COMPLETED,
                        IDENTITY, CacheEntryState.ABSENT, null, "oops"));
    }

    @Test
    void executionResult_failed_rejectsNullErrorMessage() {
        assertThrows(NullPointerException.class,
                () -> QueryExecutionResult.failed(CTX, IDENTITY, null));
    }

    // ── QueryMetadataRequest ──────────────────────────────────────────────────

    @Test
    void metadataRequest_forSql_factory() {
        var r = QueryMetadataRequest.forSql(CTX, SQL);
        assertEquals(SQL, r.sql());
        assertNull(r.semanticContractName());
    }

    @Test
    void metadataRequest_forContract_factory() {
        var r = QueryMetadataRequest.forContract(CTX, "mxl_risk");
        assertNull(r.sql());
        assertEquals("mxl_risk", r.semanticContractName());
    }

    @Test
    void metadataRequest_neitherProvided_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new QueryMetadataRequest(CTX, null, "  "));
    }

    // ── QueryMetadataResult ───────────────────────────────────────────────────

    @Test
    void metadataResult_unknown_hasNullFields() {
        var r = QueryMetadataResult.unknown();
        assertNull(r.outputShape());
        assertNull(r.contractName());
        assertTrue(r.references().isEmpty());
    }

    @Test
    void metadataResult_references_isUnmodifiable() {
        var r = QueryMetadataResult.unknown();
        assertThrows(UnsupportedOperationException.class,
                () -> r.references().add(null));
    }

    // ── SemanticResolutionRequest ─────────────────────────────────────────────

    @Test
    void resolutionRequest_holdsFields() {
        var r = new SemanticResolutionRequest("mxl_risk", CTX);
        assertEquals("mxl_risk", r.contractName());
        assertEquals(CTX,        r.context());
    }

    @Test
    void resolutionRequest_rejectsBlankContractName() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionRequest(" ", CTX));
    }

    // ── SemanticResolutionResult ──────────────────────────────────────────────

    static SemanticContract minimalContract() {
        var entity = new SemanticEntity("c", "", new SemanticEndpoint("cat", "sch", "tbl"), List.of());
        return new SemanticContract("c", new SemanticContractVersion("1.0.0"), "", entity,
                List.of(), List.of(), SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null);
    }

    @Test
    void resolutionResult_found_factory() {
        var c = minimalContract();
        var r = SemanticResolutionResult.found(c);
        assertTrue(r.resolved());
        assertSame(c, r.contract());
        assertNull(r.errorMessage());
    }

    @Test
    void resolutionResult_notFound_factory() {
        var r = SemanticResolutionResult.notFound("contract 'x' not accessible");
        assertFalse(r.resolved());
        assertNull(r.contract());
        assertEquals("contract 'x' not accessible", r.errorMessage());
    }

    @Test
    void resolutionResult_foundWithNullContract_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionResult(null, true, null));
    }

    @Test
    void resolutionResult_notFoundWithNonNullContract_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionResult(minimalContract(), false, null));
    }

    // ── NoOpCacheLookupService ────────────────────────────────────────────────

    @Test
    void noOpCache_lookup_returnsAbsent() {
        var svc = NoOpCacheLookupService.INSTANCE;
        var req = new CacheLookupRequest(IDENTITY, CacheContract.none());
        var result = svc.lookup(req);
        assertEquals(org.iceforge.skadi.semantic.cache.CacheEntryState.ABSENT, result.state());
        assertNull(result.artifactMeta());
    }

    @Test
    void noOpCache_write_isNoop() {
        var svc = NoOpCacheLookupService.INSTANCE;
        var req = new CacheWriteRequest(IDENTITY, CacheContract.none(), 0L);
        assertDoesNotThrow(() -> svc.write(req));
    }

    @Test
    void noOpCache_lookup_rejectsNullRequest() {
        assertThrows(NullPointerException.class,
                () -> NoOpCacheLookupService.INSTANCE.lookup(null));
    }

    // ── NoOpLineageContextProvider ────────────────────────────────────────────

    @Test
    void noOpLineage_returnsEmptyList() {
        var refs = NoOpLineageContextProvider.INSTANCE.referencesFor(CTX, "mxl_risk");
        assertNotNull(refs);
        assertTrue(refs.isEmpty());
    }

    @Test
    void noOpLineage_rejectsNullContext() {
        assertThrows(NullPointerException.class,
                () -> NoOpLineageContextProvider.INSTANCE.referencesFor(null, "c"));
    }

    @Test
    void noOpLineage_rejectsNullContractName() {
        assertThrows(NullPointerException.class,
                () -> NoOpLineageContextProvider.INSTANCE.referencesFor(CTX, null));
    }

    // ── SkadiServerQueryExecutionService — Lane E activation ─────────────────

    @Test
    void skadiserverActivation_cacheHit_returnsCompletedFresh() throws Exception {
        String respJson = "{\"queryId\":\"q1\",\"state\":\"SUCCEEDED\","
                + "\"resultUrl\":\"/api/v1/queries/q1/results\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryServer.succeededImmediately(respJson)) {
            var svc = activatedService(fake.baseUrl());
            var result = svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertEquals(ExecutionStatus.COMPLETED, result.status());
            assertEquals(org.iceforge.skadi.semantic.cache.CacheEntryState.FRESH, result.cacheState());
            assertEquals(SQL,   result.cacheIdentity().normalizedSql());
            assertEquals("alice", result.cacheIdentity().principalName());
        }
    }

    @Test
    void skadiserverActivation_sqlForwarded_toQueryApi() throws Exception {
        java.util.concurrent.atomic.AtomicReference<String> capturedBody =
                new java.util.concurrent.atomic.AtomicReference<>();
        String respJson = "{\"queryId\":\"q1\",\"state\":\"SUCCEEDED\","
                + "\"resultUrl\":\"/api/v1/queries/q1/results\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryServer.capturingAndReturning(capturedBody, 200, respJson)) {
            var svc = activatedService(fake.baseUrl());
            svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertNotNull(capturedBody.get(), "request body must be captured");
            assertTrue(capturedBody.get().contains(SQL), "submitted body must include the SQL");
            assertTrue(capturedBody.get().contains("jdbcUrl"), "submitted body must include JDBC config");
        }
    }

    @Test
    void skadiserverActivation_asyncQuery_pollsUntilSucceeded() throws Exception {
        // POST returns 202 QUEUED; first status poll returns RUNNING; second returns SUCCEEDED.
        java.util.concurrent.atomic.AtomicInteger pollCount =
                new java.util.concurrent.atomic.AtomicInteger();
        String submitResp = "{\"queryId\":\"q2\",\"state\":\"QUEUED\","
                + "\"resultUrl\":\"/api/v1/queries/q2/results\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryServer.asyncQuery(submitResp, pollCount)) {
            var svc = activatedService(fake.baseUrl());
            var result = svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertEquals(ExecutionStatus.COMPLETED, result.status());
            assertEquals(org.iceforge.skadi.semantic.cache.CacheEntryState.ABSENT, result.cacheState());
            assertTrue(pollCount.get() >= 2, "must have polled at least twice");
        }
    }

    @Test
    void skadiserverActivation_serverFailure_returnsFailedResult() throws Exception {
        String respJson = "{\"queryId\":\"q3\",\"state\":\"FAILED\","
                + "\"resultUrl\":\"/api/v1/queries/q3/results\",\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryServer.succeededImmediately(200, respJson)) {
            var svc = activatedService(fake.baseUrl());
            var result = svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertEquals(ExecutionStatus.FAILED, result.status());
            assertNotNull(result.errorMessage());
        }
    }

    private static SkadiServerQueryExecutionService activatedService(String baseUrl) {
        return new SkadiServerQueryExecutionService(
                baseUrl,
                "jdbc:h2:mem:test", "sa", "",
                new com.fasterxml.jackson.databind.ObjectMapper(),
                java.net.http.HttpClient.newHttpClient(),
                /*pollIntervalMs=*/ 10L,
                /*maxWaitMs=*/ 5_000L,
                NoOpSemanticExecutionMetrics.INSTANCE);
    }

    /**
     * Lightweight fake HTTP server backed by the JDK {@code com.sun.net.httpserver.HttpServer}.
     * Stands in for skadi-server's query API without any external dependencies.
     */
    static final class FakeQueryServer implements AutoCloseable {

        private final com.sun.net.httpserver.HttpServer server;

        private FakeQueryServer() throws Exception {
            server = com.sun.net.httpserver.HttpServer.create(
                    new java.net.InetSocketAddress(0), 0);
        }

        /** Returns the server base URL (e.g. {@code http://localhost:54321}). */
        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }

        /** POST /api/v1/queries → {@code httpStatus} with {@code respBody}; no polling needed. */
        static FakeQueryServer succeededImmediately(String respBody) throws Exception {
            return succeededImmediately(200, respBody);
        }

        static FakeQueryServer succeededImmediately(int httpStatus, String respBody) throws Exception {
            var fake = new FakeQueryServer();
            fake.server.createContext("/api/v1/queries", exchange -> {
                exchange.getRequestBody().readAllBytes();
                byte[] bytes = respBody.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(httpStatus, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }

        /**
         * Captures the POST body then responds; useful for asserting what was forwarded.
         */
        static FakeQueryServer capturingAndReturning(
                java.util.concurrent.atomic.AtomicReference<String> capture,
                int httpStatus,
                String respBody) throws Exception {
            var fake = new FakeQueryServer();
            fake.server.createContext("/api/v1/queries", exchange -> {
                capture.set(new String(exchange.getRequestBody().readAllBytes(),
                        java.nio.charset.StandardCharsets.UTF_8));
                byte[] bytes = respBody.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(httpStatus, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }

        /**
         * Simulates async execution: POST returns 202 QUEUED; status polls count toward
         * {@code pollCount}; on the 2nd poll, returns SUCCEEDED.
         */
        static FakeQueryServer asyncQuery(
                String submitResp,
                java.util.concurrent.atomic.AtomicInteger pollCount) throws Exception {
            var fake = new FakeQueryServer();
            fake.server.createContext("/api/v1/queries", exchange -> {
                String path = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();

                if ("POST".equals(method) && "/api/v1/queries".equals(path)) {
                    exchange.getRequestBody().readAllBytes();
                    byte[] bytes = submitResp.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(202, bytes.length);
                    exchange.getResponseBody().write(bytes);
                } else {
                    int count = pollCount.incrementAndGet();
                    String state = count >= 2 ? "SUCCEEDED" : "RUNNING";
                    String statusJson = "{\"queryId\":\"q2\",\"state\":\"" + state + "\"}";
                    byte[] bytes = statusJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
                exchange.close();
            });
            fake.server.start();
            return fake;
        }
    }

    // ── ExecutionStatus completeness ──────────────────────────────────────────

    @Test
    void executionStatus_hasExpectedValues() {
        assertEquals(4, ExecutionStatus.values().length);
    }
}
