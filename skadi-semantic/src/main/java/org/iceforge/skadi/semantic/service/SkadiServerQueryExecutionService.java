package org.iceforge.skadi.semantic.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;

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
 * path is unchanged. See {@code ai/dqr/DQR-002-semantic-execution-delegation.md}.
 *
 * <p>Execution protocol:
 * <ol>
 *   <li>POSTs the SQL and JDBC config to {@code /api/v1/queries} on the configured server.</li>
 *   <li>If the server responds {@code 200 SUCCEEDED} (cache hit), returns immediately with
 *       a {@link CacheEntryState#FRESH} result.</li>
 *   <li>If the server responds {@code 202 ACCEPTED} (cache miss, async start), polls
 *       {@code /api/v1/queries/{queryId}/status} until a terminal state is reached.</li>
 * </ol>
 *
 * <p>Wired as a Spring bean by {@code SemanticContractConfiguration} in {@code skadi-server}
 * via {@code skadi.semantic.execution.*} properties.
 */
public final class SkadiServerQueryExecutionService implements QueryExecutionService {

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

    /**
     * Production constructor. Uses the default {@link HttpClient} and standard poll settings.
     *
     * @param baseUrl      skadi-server base URL (e.g. {@code http://localhost:8080})
     * @param jdbcUrl      JDBC URL forwarded to skadi-server for Databricks connections
     * @param jdbcUsername optional JDBC username; may be null
     * @param jdbcPassword optional JDBC password; may be null
     * @param mapper       Jackson ObjectMapper for JSON request/response handling
     */
    public SkadiServerQueryExecutionService(
            String baseUrl,
            String jdbcUrl,
            String jdbcUsername,
            String jdbcPassword,
            ObjectMapper mapper) {
        this(baseUrl, jdbcUrl, jdbcUsername, jdbcPassword, mapper,
                HttpClient.newHttpClient(), DEFAULT_POLL_INTERVAL_MS, DEFAULT_MAX_WAIT_MS);
    }

    /** Package-private: allows tests to inject a fake {@link HttpClient} and tunable poll timing. */
    SkadiServerQueryExecutionService(
            String baseUrl,
            String jdbcUrl,
            String jdbcUsername,
            String jdbcPassword,
            ObjectMapper mapper,
            HttpClient httpClient,
            long pollIntervalMs,
            long maxWaitMs) {
        String trimmed = Objects.requireNonNull(baseUrl, "baseUrl");
        this.baseUrl = trimmed.endsWith("/") ? trimmed.substring(0, trimmed.length() - 1) : trimmed;
        this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
        this.jdbcUsername = jdbcUsername;
        this.jdbcPassword = jdbcPassword;
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.httpClient = Objects.requireNonNull(httpClient, "httpClient");
        this.pollIntervalMs = pollIntervalMs;
        this.maxWaitMs = maxWaitMs;
    }

    /**
     * Delegates query execution to skadi-server via {@code POST /api/v1/queries}.
     *
     * <p>Synchronous: returns only when the query reaches a terminal state (SUCCEEDED, FAILED,
     * or CANCELED) or the max-wait deadline is exceeded.
     *
     * @param request the execution request; must carry a non-blank {@link QueryExecutionRequest#sql()}
     * @return execution result; never null
     * @throws IllegalArgumentException if the request carries no SQL (semantic-first not yet supported)
     */
    @Override
    public QueryExecutionResult execute(QueryExecutionRequest request) {
        Objects.requireNonNull(request, "request");

        String sql = request.sql();
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException(
                    "SQL-first execution required for Lane E; request.sql() must not be blank");
        }

        CacheIdentity identity = new CacheIdentity(
                sql, request.context().principalName(), request.semanticContractName());

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
                return QueryExecutionResult.completed(request.context(), identity, CacheEntryState.FRESH);
            }
            if (response.statusCode() == 202 && queryId != null) {
                return pollUntilDone(request.context(), identity, queryId);
            }
            return QueryExecutionResult.failed(request.context(), identity,
                    "unexpected submit response: HTTP " + response.statusCode() + " state=" + state);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return QueryExecutionResult.failed(request.context(), identity,
                    "interrupted while waiting for query: " + e.getMessage());
        } catch (Exception e) {
            return QueryExecutionResult.failed(request.context(), identity, e.getMessage());
        }
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

            if ("SUCCEEDED".equals(state)) {
                return QueryExecutionResult.completed(ctx, identity, CacheEntryState.ABSENT);
            }
            if ("FAILED".equals(state)) {
                return QueryExecutionResult.failed(ctx, identity,
                        statusNode.path("message").asText("query failed on server"));
            }
            if ("CANCELED".equals(state)) {
                return QueryExecutionResult.failed(ctx, identity, "query was canceled on server");
            }
            // QUEUED or RUNNING — keep polling
        }
        return QueryExecutionResult.failed(ctx, identity, "query timed out after " + maxWaitMs + "ms");
    }
}
