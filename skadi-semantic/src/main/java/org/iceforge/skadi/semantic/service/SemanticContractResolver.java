package org.iceforge.skadi.semantic.service;

/**
 * Service seam for principal-aware semantic contract resolution.
 *
 * <p>A {@code SemanticContractResolver} wraps the {@code ContractRegistry}
 * from C2 with principal context: it applies the access policy from
 * {@link SemanticResolutionRequest#context()} before returning a contract.
 *
 * <p>A contract that the principal is not permitted to access is treated the
 * same as a contract that does not exist — the result is
 * {@link SemanticResolutionResult#notFound(String)} in both cases. This
 * prevents information leakage about the existence of restricted contracts.
 *
 * <p>In Lane C, access policy enforcement is deferred. Implementations may
 * return all contracts regardless of principal (stub behaviour), with a note
 * that enforcement will be added post-Lane C.
 *
 * <p>This interface does not load files from disk, validate YAML or JSON Schema,
 * or call any external system.
 *
 * <p><strong>This interface is not a Spring bean in Lane C.</strong>
 */
public interface SemanticContractResolver {

    /**
     * Resolves a semantic contract by name for the given principal.
     *
     * @param request the resolution request; must not be null
     * @return the resolution result; never null
     */
    SemanticResolutionResult resolve(SemanticResolutionRequest request);
}
