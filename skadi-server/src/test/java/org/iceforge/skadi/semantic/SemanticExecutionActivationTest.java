package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.jdbc.SkadiJdbcProperties;
import org.iceforge.skadi.semantic.cache.CacheContract;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.service.ExecutionContext;
import org.iceforge.skadi.semantic.service.ExecutionStatus;
import org.iceforge.skadi.semantic.service.QueryExecutionRequest;
import org.iceforge.skadi.semantic.service.QueryExecutionService;
import org.iceforge.skadi.semantic.service.SemanticExecutionCircuitBreaker;
import org.iceforge.skadi.semantic.service.SkadiServerQueryExecutionService;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for Lane E semantic execution activation (skadi#97).
 *
 * <p>Verifies that {@link SemanticContractConfiguration} wires
 * {@link SkadiServerQueryExecutionService} as the {@link QueryExecutionService} bean,
 * and that the wired service correctly delegates to {@code POST /api/v1/queries}.
 *
 * <p>Uses a hand-rolled JDK {@code HttpServer} as a fake skadi-server query endpoint —
 * no Spring context, no Mockito, no Databricks.
 */
class SemanticExecutionActivationTest {

    private static final String SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";
    private static final ExecutionContext CTX = ExecutionContext.of("alice");

    // ── Bean wiring ────────────────────────────────────────────────────────────

    @Test
    void queryExecutionService_beanIsInstanceOfSkadiServerService() {
        var config = new SemanticContractConfiguration();

        var execProps = new SemanticExecutionProperties();
        execProps.setServerUrl("http://localhost:8080");
        execProps.setDatasourceId("default");

        var jdbcProps = new SkadiJdbcProperties();
        var ds = new SkadiJdbcProperties.JdbcDatasourceConfig();
        ds.setJdbcUrl("jdbc:databricks://host:443");
        ds.setUsername("token");
        ds.setPassword("secret");
        jdbcProps.setDatasources(Map.of("default", ds));

        QueryExecutionService svc = config.queryExecutionService(execProps, jdbcProps, new ObjectMapper(),
                new SemanticExecutionMetricsRegistry(),
                SemanticExecutionCircuitBreaker.alwaysAllow(execProps.getServerUrl()));

        assertNotNull(svc);
        assertInstanceOf(SkadiServerQueryExecutionService.class, svc,
                "queryExecutionService bean must be SkadiServerQueryExecutionService");
    }

    @Test
    void queryExecutionService_missingDatasource_beanStillCreated() {
        var config = new SemanticContractConfiguration();

        var execProps = new SemanticExecutionProperties();
        execProps.setServerUrl("http://localhost:8080");
        execProps.setDatasourceId("nonexistent");

        var jdbcProps = new SkadiJdbcProperties(); // no datasources configured

        QueryExecutionService svc = config.queryExecutionService(execProps, jdbcProps, new ObjectMapper(),
                new SemanticExecutionMetricsRegistry(),
                SemanticExecutionCircuitBreaker.alwaysAllow(execProps.getServerUrl()));

        assertNotNull(svc, "bean must be created even when datasource is absent");
        assertInstanceOf(SkadiServerQueryExecutionService.class, svc);
    }

    // ── Delegation behaviour ───────────────────────────────────────────────────

    @Test
    void queryExecutionService_delegatesToSubmitEndpoint_cacheHit() throws Exception {
        String respJson = "{\"queryId\":\"q1\",\"state\":\"SUCCEEDED\","
                + "\"resultUrl\":\"/api/v1/queries/q1/results\","
                + "\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryApi.succeededImmediately(200, respJson)) {
            QueryExecutionService svc = wiredService(fake.baseUrl());
            var result = svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertEquals(ExecutionStatus.COMPLETED, result.status());
            assertEquals(CacheEntryState.FRESH, result.cacheState());
        }
    }

    @Test
    void queryExecutionService_delegatesToSubmitEndpoint_forwardsSql() throws Exception {
        AtomicReference<String> capturedBody = new AtomicReference<>();
        String respJson = "{\"queryId\":\"q1\",\"state\":\"SUCCEEDED\","
                + "\"resultUrl\":\"/api/v1/queries/q1/results\","
                + "\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryApi.capturingAndReturning(capturedBody, 200, respJson)) {
            QueryExecutionService svc = wiredService(fake.baseUrl());
            svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertNotNull(capturedBody.get(), "request body must reach the server");
            assertTrue(capturedBody.get().contains(SQL),
                    "forwarded body must contain the SQL");
        }
    }

    @Test
    void queryExecutionService_asyncExecution_pollsStatusUntilDone() throws Exception {
        var pollCount = new java.util.concurrent.atomic.AtomicInteger();
        String submitResp = "{\"queryId\":\"q2\",\"state\":\"QUEUED\","
                + "\"resultUrl\":\"/api/v1/queries/q2/results\","
                + "\"expiresAt\":\"2030-01-01T00:00:00Z\"}";

        try (var fake = FakeQueryApi.asyncQuery(submitResp, pollCount)) {
            QueryExecutionService svc = wiredService(fake.baseUrl());
            var result = svc.execute(QueryExecutionRequest.sqlOnly(CTX, SQL, CacheContract.none()));

            assertEquals(ExecutionStatus.COMPLETED, result.status());
            assertEquals(CacheEntryState.ABSENT, result.cacheState());
            assertTrue(pollCount.get() >= 2, "service must poll at least twice before SUCCEEDED");
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static QueryExecutionService wiredService(String serverUrl) {
        return new SkadiServerQueryExecutionService(
                serverUrl,
                "jdbc:h2:mem:test", "sa", "",
                new ObjectMapper());
    }

    /**
     * Minimal fake HTTP server standing in for skadi-server's {@code /api/v1/queries} API.
     * Backed by the JDK's built-in {@code com.sun.net.httpserver.HttpServer}.
     */
    static final class FakeQueryApi implements AutoCloseable {

        private final com.sun.net.httpserver.HttpServer server;

        private FakeQueryApi() throws Exception {
            server = com.sun.net.httpserver.HttpServer.create(
                    new java.net.InetSocketAddress(0), 0);
        }

        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }

        static FakeQueryApi succeededImmediately(int httpStatus, String respBody) throws Exception {
            var fake = new FakeQueryApi();
            fake.server.createContext("/api/v1/queries", exchange -> {
                exchange.getRequestBody().readAllBytes();
                byte[] bytes = respBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(httpStatus, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }

        static FakeQueryApi capturingAndReturning(
                AtomicReference<String> capture, int httpStatus, String respBody) throws Exception {
            var fake = new FakeQueryApi();
            fake.server.createContext("/api/v1/queries", exchange -> {
                capture.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                byte[] bytes = respBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(httpStatus, bytes.length);
                exchange.getResponseBody().write(bytes);
                exchange.close();
            });
            fake.server.start();
            return fake;
        }

        static FakeQueryApi asyncQuery(
                String submitResp,
                java.util.concurrent.atomic.AtomicInteger pollCount) throws Exception {
            var fake = new FakeQueryApi();
            fake.server.createContext("/api/v1/queries", exchange -> {
                String path   = exchange.getRequestURI().getPath();
                String method = exchange.getRequestMethod();

                if ("POST".equals(method) && "/api/v1/queries".equals(path)) {
                    exchange.getRequestBody().readAllBytes();
                    byte[] bytes = submitResp.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(202, bytes.length);
                    exchange.getResponseBody().write(bytes);
                } else {
                    int count = pollCount.incrementAndGet();
                    String state = count >= 2 ? "SUCCEEDED" : "RUNNING";
                    String statusJson = "{\"queryId\":\"q2\",\"state\":\"" + state + "\"}";
                    byte[] bytes = statusJson.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
                exchange.close();
            });
            fake.server.start();
            return fake;
        }
    }
}
