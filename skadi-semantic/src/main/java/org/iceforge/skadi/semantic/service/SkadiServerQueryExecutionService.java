package org.iceforge.skadi.semantic.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Objects;

/**
 * {@link QueryExecutionService} that delegates to {@code POST /api/v1/queries} on skadi-server.
 *
 * <p><strong>Lane E activation — DQR-002 resolved (Option 4, partial convergence).</strong>
 * The semantic execution path delegates to skadi-server via HTTP; the SQL gateway direct
 * path is unchanged.
 *
 * <p><strong>Lane F F1 — resilience.</strong> Calls are guarded by a
 * {@link SemanticExecutionCircuitBreaker}. When the circuit is open (repeated failures),
 * calls are short-circuited and return a FAILED result without contacting skadi-server.
 * When disabled, all calls are blocked and the status is reported as DISABLED.
 *
 * <p>Observability: every execution path emits structured SLF4J log events and records
 * counters via {@link SemanticExecutionMetrics}. Wired as a Spring bean by
 * {@code SemanticContractConfiguration} in {@code skadi-server}.
 */
public final class SkadiServerQueryExecutionService implements QueryExecutionService {

    private static final Logger log = LoggerFactory.getLogger(SkadiServerQueryExecutionService.class);

    static final long DEFAULT_POLL_INTERVAL_MS = 500L;
    static final long DEFAULT_MAX_WAIT_MS = 300_000L;

    private final String baseUrl;
    private final String jdbcUrl;
    private final String jdbcUsername;
    private final String jdbcPassword;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final long pollIntervalMs;
    private final long maxWaitMs;
    private final SemanticExecutionMetrics metrics;
    private final SemanticExecutionCircuitBreaker circuitBreaker;

    /**
     * Convenience constructor. Uses {@link NoOpSemanticExecutionMetrics} and an
     * always-allow circuit breaker.
     */
    public SkadiServerQueryExecutionService(
            String baseUrl,
            String jdbcUrl,
            String jdbcUsername,
            String jdbcPassword,
            ObjectMapper mapper) {
        this(baseUrl, jdbcUrl, jdbcUsername, jdbcPassword, mapper,
                NoOpSemanticExecutionMetrics.INSTANCE);
    }

    /**
     * Constructor with explicit metrics. Uses an always-allow circuit breaker.
     */
    public SkadiServerQueryExecutionService(
            String baseUrl,
            String jdbcUrl,
            String jdbcUsername,
            String jdbcPassword,
            ObjectMapper mapper,
            SemanticExecutionMetrics metrics) {
        this(baseUrl, jdbcUrl, jdbcUsername, jdbcPassword, mapper, metrics,
                SemanticExecutionCircuitBreaker.alwaysAllow(baseUrl));
    }

    /**
     * Production constructor. Uses the default {@link HttpClient} and standard poll settings.
     *
     * @param baseUrl        skadi-server base URL (e.g. {@code http://localhost:8080})
     * @param jdbcUrl        JDBC URL forwarded to skadi-server for Databricks connections
     * @param jdbcUsername   optional JDBC username; may be null
     * @param jdbcPassword   optional JDBC password; may be null
     * @param mapper         Jackson ObjectMapper for JSON request/response handling
     * @param metrics        observability callback
     * @param circuitBreaker circuit-breaker for failure isolation; controls DISABLED/CIRCUIT_OPEN paths
     */
    public SkadiServerQueryExecutionService(
            String baseUrl,
            String jdbcUrl,
            String jdbcUsername,
            String jdbcPassword,
            ObjectMapper mapper,
            SemanticExecutionMetrics metrics,
            SemanticExecutionCircuitBreaker circuitBreaker) {
        this(baseUrl, jdbcUrl, jdbcUsername, jdbcPassword, mapper,
                HttpClient.newHttpClient(), DEFAULT_POLL_INTERVAL_MS, DEFAULT_MAX_WAIT_MS,
                metrics, circuitBreaker);
    }

    /** Package-private: allows tests to inject fake {@link HttpClient} and tunable poll timing. */
    SkadiServerQueryExecutionService(
            String baseUrl,
            String jdbcUrl,
            String jdbcUsername,
            String jdbcPassword,
            ObjectMapper mapper,
            HttpClient httpClient,
            long pollIntervalMs,
            long maxWaitMs,
            SemanticExecutionMetrics metrics,
            SemanticExecutionCircuitBreaker circuitBreaker) {
        String trimmed = Objects.requireNonNull(baseUrl, "baseUrl");
        this.baseUrl = trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.pollIntervalMs = pollIntervalMs;
        this.maxWaitMs = maxWaitMs;
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker");
    }

    /**
     * Delegates query execution to skadi-server via {@code POST /api/v1/queries}.
     *
     * <p>Returns immediately with a FAILED result (without making an HTTP call) when:
     * <ul>
     *   <li>the service is disabled ({@code skadi.semantic.execution.enabled=false})</li>
     *   <li>the circuit is open due to repeated consecutive failures</li>
     * </ul>
     *
     * @param request the execution request; must carry a non-blank {@link QueryExecutionRequest#sql()}
     * @return execution result; never null
     * @throws IllegalArgumentException if the request carries no SQL
     */
    @Override
    public QueryExecutionResult execute(QueryExecutionRequest request) {
        Objects.requireNonNull(request, "request");

        String sql = request.sql();
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException(
                    "SQL-first execution required for Lane E; request.sql() must not be blank");
        }

        String principal = request.context().principalName();
        CacheIdentity identity = new CacheIdentity(sql, principal, request.semanticContractName());

        log.debug("semantic-exec: attempt principal={} server={}", principal, baseUrl);
        metrics.recordAttempt();

        if (!circuitBreaker.isCallAllowed()) {
            SemanticExecutionHealthStatus cbStatus = circuitBreaker.snapshot().status();
            SemanticExecutionFailure failure = SemanticExecutionFailure.fromHealthStatus(cbStatus);
            if (cbStatus == SemanticExecutionHealthStatus.DISABLED) {
                log.debug("semantic-exec: disabled principal={}", principal);
            } else {
                log.warn("semantic-exec: circuit-open principal={}", principal);
            }
            metrics.recordFailure();
            return QueryExecutionResult.failed(request.context(), identity, failure.safeMessage());
        }

        try {
            String body = buildSubmitBody(sql);
            HttpRequest submitReq = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/v1/queries"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

            HttpResponse<String> response =
                    httpClient.send(submitReq, HttpResponse.BodyHandlers.ofString());

            JsonNode resp = mapper.readTree(response.body());
            String queryId = resp.path("queryId").asText(null);
            String state = resp.path("state").asText("");

            if (response.statusCode() == 200 && "SUCCEEDED".equals(state)) {
                log.info("semantic-exec: cache-hit queryId={} principal={}", queryId, principal);
                circuitBreaker.recordSuccess();
                metrics.recordCacheHit();
                return QueryExecutionResult.completed(request.context(), identity, CacheEntryState.FRESH);
            }
            if (response.statusCode() == 202 && queryId != null) {
                log.info("semantic-exec: async-accepted queryId={} principal={}", queryId, principal);
                metrics.recordAsyncAccepted();
                return pollUntilDone(request.context(), identity, queryId);
            }

            log.warn("semantic-exec: unexpected-submit-response http={} state={} principal={}",
                    response.statusCode(), state, principal);
            circuitBreaker.recordFailure(SemanticExecutionFailure.UNAVAILABLE.safeMessage(),
                    SemanticExecutionHealthStatus.UNAVAILABLE);
            metrics.recordFailure();
            return QueryExecutionResult.failed(request.context(), identity,
                    SemanticExecutionFailure.UNAVAILABLE.safeMessage());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("semantic-exec: interrupted principal={}", principal);
            circuitBreaker.recordFailure(SemanticExecutionFailure.TIMEOUT.safeMessage(),
                    SemanticExecutionHealthStatus.TIMEOUT);
            metrics.recordError();
            return QueryExecutionResult.failed(request.context(), identity,
                    SemanticExecutionFailure.TIMEOUT.safeMessage());
        } catch (Exception e) {
            // Log the raw exception for operator diagnosis; never propagate it to callers.
            log.error("semantic-exec: error principal={}", principal, e);
            circuitBreaker.recordFailure(SemanticExecutionFailure.UNEXPECTED.safeMessage(),
                    SemanticExecutionHealthStatus.UNAVAILABLE);
            metrics.recordError();
            return QueryExecutionResult.failed(request.context(), identity,
                    SemanticExecutionFailure.UNEXPECTED.safeMessage());
        }
    }

    /** Returns a point-in-time health snapshot of the circuit-breaker state. */
    public SemanticExecutionHealthSnapshot getHealthSnapshot() {
        return circuitBreaker.snapshot();
    }

    private String buildSubmitBody(String sql) throws Exception {
        ObjectNode body = mapper.createObjectNode();
        body.put("sql", sql);
        ObjectNode jdbc = body.putObject("jdbc");
        jdbc.put("jdbcUrl", jdbcUrl);
        if (jdbcUsername != null) jdbc.put("username", jdbcUsername);
        if (jdbcPassword != null) jdbc.put("password", jdbcPassword);
        return mapper.writeValueAsString(body);
    }

    private QueryExecutionResult pollUntilDone(ExecutionContext ctx, CacheIdentity identity, String queryId)
            throws Exception {
        String statusUrl = baseUrl + "/api/v1/queries/" + queryId + "/status";
        long deadline = System.currentTimeMillis() + maxWaitMs;

        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(pollIntervalMs);
            HttpRequest statusReq = HttpRequest.newBuilder()
                    .uri(URI.create(statusUrl))
                    .GET()
                    .build();
            HttpResponse<String> statusResp =
                    httpClient.send(statusReq, HttpResponse.BodyHandlers.ofString());
            JsonNode statusNode = mapper.readTree(statusResp.body());
            String state = statusNode.path("state").asText("");

            log.debug("semantic-exec: poll queryId={} state={}", queryId, state);

            if ("SUCCEEDED".equals(state)) {
                log.info("semantic-exec: succeeded queryId={} principal={}", queryId, ctx.principalName());
                circuitBreaker.recordSuccess();
                metrics.recordSuccess();
                return QueryExecutionResult.completed(ctx, identity, CacheEntryState.ABSENT);
            }
            if ("FAILED".equals(state)) {
                // Log raw server message for operator diagnosis; return stable safe message to caller.
                String rawMsg = statusNode.path("message").asText("query failed on server");
                log.warn("semantic-exec: failed queryId={} msg={}", queryId, rawMsg);
                circuitBreaker.recordFailure(SemanticExecutionFailure.REMOTE_ERROR.safeMessage(),
                        SemanticExecutionHealthStatus.FAILED);
                metrics.recordFailure();
                return QueryExecutionResult.failed(ctx, identity,
                        SemanticExecutionFailure.REMOTE_ERROR.safeMessage());
            }
            if ("CANCELED".equals(state)) {
                log.warn("semantic-exec: canceled queryId={}", queryId);
                circuitBreaker.recordFailure(SemanticExecutionFailure.REMOTE_ERROR.safeMessage(),
                        SemanticExecutionHealthStatus.FAILED);
                metrics.recordFailure();
                return QueryExecutionResult.failed(ctx, identity,
                        SemanticExecutionFailure.REMOTE_ERROR.safeMessage());
            }
            // QUEUED or RUNNING — keep polling
        }

        log.warn("semantic-exec: timeout queryId={} maxWaitMs={}", queryId, maxWaitMs);
        circuitBreaker.recordFailure(SemanticExecutionFailure.TIMEOUT.safeMessage(),
                SemanticExecutionHealthStatus.TIMEOUT);
        metrics.recordTimeout();
        return QueryExecutionResult.failed(ctx, identity, SemanticExecutionFailure.TIMEOUT.safeMessage());
    }
}
