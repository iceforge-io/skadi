package org.iceforge.skadi.semantic.service;

/**
 * The outcome status of a {@link QueryExecutionRequest}.
 */
public enum ExecutionStatus {

    /** Query completed and rows are (or will be) available. */
    COMPLETED,

    /**
     * Query result was served from the cache.
     * No Databricks call was made for this request.
     */
    CACHE_HIT,

    /** Query failed; {@link QueryExecutionResult#errorMessage()} contains details. */
    FAILED,

    /** Query was cancelled before completion. */
    CANCELLED
}
