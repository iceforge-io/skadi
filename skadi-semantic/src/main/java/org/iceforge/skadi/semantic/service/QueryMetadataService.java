package org.iceforge.skadi.semantic.service;

/**
 * Service seam for output-shape metadata inspection.
 *
 * <p>Allows callers to discover what columns a query will return
 * before (or without) executing it. UI bricks, AI context builders,
 * and Tableau adapters use this to understand result structure.
 *
 * <p>For SQL-first queries, shape information may not be available until
 * after the first execution (implementation-dependent).
 * For semantic-contract-based queries, the shape is declared in the
 * {@code SemanticQueryContract} and is available without execution.
 *
 * <p>Implementations must not execute SQL or call Databricks to answer
 * a metadata request — they should derive the shape from the contract
 * or from a cached result descriptor.
 *
 * <p><strong>This interface is not a Spring bean in Lane C.</strong>
 */
public interface QueryMetadataService {

    /**
     * Inspects the output-shape metadata for a query.
     *
     * @param request the metadata request; must not be null
     * @return metadata result; never null (use {@link QueryMetadataResult#unknown()} for unknown shape)
     */
    QueryMetadataResult inspect(QueryMetadataRequest request);
}
