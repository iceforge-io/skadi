package org.iceforge.skadi.semantic.service;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * Point-in-time snapshot of the semantic execution delegation path health.
 *
 * <p>All optional fields ({@link #lastSuccessAt}, {@link #lastFailureAt},
 * {@link #lastFailureReason}, {@link #circuitOpenUntil}) are null until a relevant
 * event has occurred. They are omitted from JSON when null.
 *
 * <p>Produced by {@link SemanticExecutionCircuitBreaker#snapshot()} and exposed
 * via the diagnostics endpoint — no credentials are included.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticExecutionHealthSnapshot(
        SemanticExecutionHealthStatus status,
        String skadiServerBaseUrl,
        int failureCount,
        int failureThreshold,
        Instant lastSuccessAt,
        Instant lastFailureAt,
        String lastFailureReason,
        Instant circuitOpenUntil) {}
