package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.query.SemanticReference;

import java.util.List;

/**
 * Service seam for attaching lineage context to a query or execution result.
 *
 * <p>A {@code LineageContextProvider} returns lightweight
 * {@link SemanticReference} pointers (source, type, id, name) that describe
 * the lineage relationships relevant to a given contract or query. It does not:
 * <ul>
 *   <li>Connect to a BCBS239 lineage database</li>
 *   <li>Connect to Market Risk Brain or any unstructured knowledge system</li>
 *   <li>Execute SQL or call Databricks</li>
 * </ul>
 *
 * <p>In Lane C, the active implementation is {@link NoOpLineageContextProvider}
 * which returns an empty list. The BCBS239 lineage subscriber and MRB context
 * provider are post-Lane C concerns tracked by DQR-003.
 *
 * <p><strong>This interface is not a Spring bean in Lane C.</strong>
 */
public interface LineageContextProvider {

    /**
     * Returns lineage reference pointers for the given execution context and contract.
     *
     * @param context      execution context carrying principal and correlation; must not be null
     * @param contractName the name of the contract being queried; must not be null or blank
     * @return unmodifiable list of lineage references; never null; may be empty
     */
    List<SemanticReference> referencesFor(ExecutionContext context, String contractName);
}
