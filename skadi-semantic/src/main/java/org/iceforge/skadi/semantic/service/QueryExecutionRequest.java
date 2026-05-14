package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.cache.CacheContract;

import java.util.Objects;

/**
 * A request to execute a query via {@link QueryExecutionService}.
 *
 * <p>Supports two execution modes:
 * <ul>
 *   <li><strong>SQL-first</strong> (current Lane C / Lane A/B): {@link #sql()} is set;
 *       {@link #semanticContractName()} is null. The executor translates SQL and
 *       routes directly to Databricks.</li>
 *   <li><strong>Semantic-first</strong> (future lanes): {@link #semanticContractName()}
 *       is set; the semantic compiler derives the SQL from the contract. {@link #sql()}
 *       may be null in this mode.</li>
 * </ul>
 *
 * <p>At least one of {@code sql} or {@code semanticContractName} must be provided.
 *
 * <pre>{@code
 * // SQL-first (today)
 * var req = QueryExecutionRequest.sqlOnly(ctx, "SELECT SUM(pnl) FROM ...", CacheContract.ttl(300L));
 *
 * // Semantic-first (future)
 * var req2 = new QueryExecutionRequest(ctx, null, "mxl_risk", CacheContract.ttl(300L));
 * }</pre>
 */
public record QueryExecutionRequest(
        ExecutionContext context,
        String sql,
        String semanticContractName,
        CacheContract cacheContract) {

    /**
     * @param context              execution context (principal, correlation, source); must not be null
     * @param sql                  raw SQL string for SQL-first execution; may be null for
     *                             semantic-first execution
     * @param semanticContractName semantic contract name for semantic-first execution;
     *                             may be null for SQL-first execution
     * @param cacheContract        cache policy hint; must not be null
     * @throws IllegalArgumentException if both {@code sql} and {@code semanticContractName} are blank
     */
    public QueryExecutionRequest {
        Objects.requireNonNull(context,       "context must not be null");
        Objects.requireNonNull(cacheContract, "cacheContract must not be null");
        boolean hasSql      = sql != null && !sql.isBlank();
        boolean hasContract = semanticContractName != null && !semanticContractName.isBlank();
        if (!hasSql && !hasContract) {
            throw new IllegalArgumentException(
                    "at least one of sql or semanticContractName must be non-blank");
        }
    }

    /**
     * Convenience factory for SQL-first execution (the current execution mode).
     *
     * @param context       execution context; must not be null
     * @param sql           raw SQL; must not be null or blank
     * @param cacheContract cache policy hint; must not be null
     */
    public static QueryExecutionRequest sqlOnly(
            ExecutionContext context, String sql, CacheContract cacheContract) {
        return new QueryExecutionRequest(context, sql, null, cacheContract);
    }
}
