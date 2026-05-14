package org.iceforge.skadi.semantic.service;

/**
 * Service seam for query execution.
 *
 * <p>Implementations translate a {@link QueryExecutionRequest} into a
 * {@link QueryExecutionResult} by dispatching to a backend execution engine.
 * In Lane C, the only implementation is a skeleton that documents the seam
 * without activating real execution (see {@link SkadiserverQueryExecutionService}).
 *
 * <p><strong>SQL-first execution (current):</strong> the request carries a raw
 * SQL string. The implementation normalises the SQL, checks the cache, and —
 * on a miss — routes to Databricks via JDBC.
 *
 * <p><strong>Semantic-first execution (future lanes):</strong> the request carries
 * a {@link QueryExecutionRequest#semanticContractName() semantic contract name}.
 * The implementation resolves the contract, compiles it to SQL, and routes
 * through the same execution path.
 *
 * <p>This interface does not dictate the transport — it may be implemented by an
 * in-process JDBC executor, an HTTP client calling {@code skadi-server},
 * or a stub returning fixture data.
 *
 * <p><strong>This interface is not a Spring bean in Lane C.</strong>
 */
public interface QueryExecutionService {

    /**
     * Executes a query and returns execution metadata.
     *
     * <p>The actual result rows are streamed separately. This method returns
     * as soon as the execution metadata (status, cache state, correlation ID)
     * is known.
     *
     * @param request the execution request; must not be null
     * @return execution metadata; never null
     */
    QueryExecutionResult execute(QueryExecutionRequest request);
}
