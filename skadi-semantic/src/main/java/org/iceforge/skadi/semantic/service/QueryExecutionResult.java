package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;
import org.iceforge.skadi.semantic.query.SemanticOutputShape;

import java.util.Objects;

/**
 * The result of a {@link QueryExecutionService#execute} call.
 *
 * <p>A {@code QueryExecutionResult} carries execution metadata — status, cache
 * identity, cache state, and optionally the output shape. It does not carry the
 * actual result rows; those are streamed separately (Arrow IPC or similar).
 *
 * <p>{@link #outputShape()} is null for SQL-first execution where the shape is
 * not declared ahead of time. It is non-null when the request was resolved via
 * a {@code SemanticQueryContract} that declares the shape.
 *
 * <p>{@link #errorMessage()} is null unless {@link #status()} is
 * {@link ExecutionStatus#FAILED}.
 */
public record QueryExecutionResult(
        ExecutionContext context,
        ExecutionStatus status,
        CacheIdentity cacheIdentity,
        CacheEntryState cacheState,
        SemanticOutputShape outputShape,
        String errorMessage) {

    /**
     * @param context       execution context echoed from the request; must not be null
     * @param status        outcome status; must not be null
     * @param cacheIdentity the logical cache identity used for this query; must not be null
     * @param cacheState    the cache lookup state for this query; must not be null
     * @param outputShape   declared output shape; null for SQL-first execution
     * @param errorMessage  error detail; must be null unless status is FAILED
     */
    public QueryExecutionResult {
        Objects.requireNonNull(context,       "context must not be null");
        Objects.requireNonNull(status,        "status must not be null");
        Objects.requireNonNull(cacheIdentity, "cacheIdentity must not be null");
        Objects.requireNonNull(cacheState,    "cacheState must not be null");
        if (status != ExecutionStatus.FAILED && errorMessage != null) {
            throw new IllegalArgumentException(
                    "errorMessage must be null when status is not FAILED, was: " + status);
        }
    }

    /** Returns a successful (non-cache-hit) result with no declared output shape. */
    public static QueryExecutionResult completed(
            ExecutionContext context, CacheIdentity identity, CacheEntryState cacheState) {
        return new QueryExecutionResult(context, ExecutionStatus.COMPLETED,
                identity, cacheState, null, null);
    }

    /** Returns a cache-hit result. */
    public static QueryExecutionResult cacheHit(
            ExecutionContext context, CacheIdentity identity) {
        return new QueryExecutionResult(context, ExecutionStatus.CACHE_HIT,
                identity, CacheEntryState.FRESH, null, null);
    }

    /** Returns a failed result with an error message. */
    public static QueryExecutionResult failed(
            ExecutionContext context, CacheIdentity identity, String errorMessage) {
        Objects.requireNonNull(errorMessage, "errorMessage must not be null for FAILED status");
        return new QueryExecutionResult(context, ExecutionStatus.FAILED,
                identity, CacheEntryState.ABSENT, null, errorMessage);
    }
}
