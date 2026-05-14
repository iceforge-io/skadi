package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.util.List;
import java.util.Optional;

/**
 * Registry of {@link SemanticContract} instances.
 *
 * <p>This interface is the lookup surface that future components depend on:
 * <ul>
 *   <li>Contract loaders (post-Lane C, once DQR-001 is resolved) populate the registry
 *       by calling {@link #register}</li>
 *   <li>The semantic compiler (C3) queries {@link #findByName} to resolve a contract
 *       before compiling a semantic query</li>
 *   <li>The semantic executor (C5) and future REST layer call {@link #list}</li>
 *   <li>The AI intent resolver (Lane E) calls {@link #forPrincipal} to build the
 *       LLM context window from contracts accessible to a given principal</li>
 * </ul>
 *
 * <p><strong>This interface contains no implementation.</strong> It does not:
 * <ul>
 *   <li>Load files from disk or validate YAML/JSON Schema</li>
 *   <li>Enforce access policy or resolve role membership</li>
 *   <li>Connect to Databricks, S3, or any external system</li>
 *   <li>Contain Spring beans or runtime wiring</li>
 * </ul>
 *
 * <p>Contract name is the stable lookup key. Names are treated as
 * case-sensitive identifiers matching {@link SemanticContract#name()}.
 *
 * <p>See {@code ai/adr/ADR-009-contracts-before-planning.md} and
 * {@code ai/dqr/DQR-001-contract-definition-format.md}.
 */
public interface ContractRegistry {

    /**
     * Registers a contract under its {@link SemanticContract#name()}.
     *
     * @param contract the contract to register; must not be null
     * @throws DuplicateContractException if a contract with the same name is
     *                                    already registered
     */
    void register(SemanticContract contract);

    /**
     * Finds a contract by its unique name.
     *
     * @param name contract name; must not be null
     * @return the contract, or {@link Optional#empty()} if not found
     */
    Optional<SemanticContract> findByName(String name);

    /**
     * Returns an unmodifiable snapshot of all registered contracts.
     * The order of elements is implementation-defined but stable within a call.
     *
     * @return unmodifiable list; never null
     */
    List<SemanticContract> list();

    /**
     * Returns the contracts that the given principal is permitted to access.
     *
     * <p><strong>Lane C stub behaviour:</strong> access policy evaluation is
     * deferred to post-Lane C. Implementations may return {@link #list()} until
     * policy enforcement is added (see {@code ai/adr/ADR-009-contracts-before-planning.md}
     * and DQR-001). This method signature exists now so that Lane E code (AI intent
     * resolver) can call it without an interface change later.
     *
     * @param principalName the identity of the requesting principal; must not be null
     * @return unmodifiable list of accessible contracts; never null
     */
    List<SemanticContract> forPrincipal(String principalName);

    /**
     * Returns {@code true} if a contract with the given name is registered.
     *
     * <p>Default implementation delegates to {@link #findByName}. Implementations
     * may override for efficiency.
     *
     * @param name contract name; must not be null
     */
    default boolean contains(String name) {
        return findByName(name).isPresent();
    }

    /**
     * Removes the contract with the given name, if present.
     *
     * <p>Default implementation throws {@link UnsupportedOperationException}.
     * Read-only registries (e.g., one loaded from YAML at startup) need not
     * support mutation. Mutable registries such as the in-memory test fixture
     * should override this method.
     *
     * @param name contract name; must not be null
     * @throws UnsupportedOperationException if this registry does not support removal
     */
    default void remove(String name) {
        throw new UnsupportedOperationException(
                "remove is not supported by this ContractRegistry implementation");
    }
}
