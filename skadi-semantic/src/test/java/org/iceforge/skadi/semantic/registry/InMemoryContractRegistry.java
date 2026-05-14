package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Mutable in-memory {@link ContractRegistry} for use in unit tests.
 *
 * <p>This class exists in {@code src/test} and must not be used in production
 * code. It is not a Spring bean and carries no runtime wiring. Contract storage
 * format (YAML, JSON, etc.) is an open question tracked by DQR-001; this class
 * is intentionally independent of that decision.
 *
 * <p>Insertion order is preserved. This implementation is not thread-safe.
 */
public class InMemoryContractRegistry implements ContractRegistry {

    private final Map<String, SemanticContract> store = new LinkedHashMap<>();

    @Override
    public void register(SemanticContract contract) {
        Objects.requireNonNull(contract, "contract must not be null");
        if (store.containsKey(contract.name())) {
            throw new DuplicateContractException(contract.name());
        }
        store.put(contract.name(), contract);
    }

    @Override
    public Optional<SemanticContract> findByName(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return Optional.ofNullable(store.get(name));
    }

    @Override
    public List<SemanticContract> list() {
        return List.copyOf(store.values());
    }

    /**
     * Returns all registered contracts regardless of principal.
     *
     * <p>Access policy evaluation is deferred to post-Lane C. This stub
     * returns {@link #list()} for all principals.
     */
    @Override
    public List<SemanticContract> forPrincipal(String principalName) {
        Objects.requireNonNull(principalName, "principalName must not be null");
        return list();
    }

    /** Removes the contract with the given name; no-op if not present. */
    @Override
    public void remove(String name) {
        Objects.requireNonNull(name, "name must not be null");
        store.remove(name);
    }

    /** Returns the number of registered contracts. */
    public int size() {
        return store.size();
    }
}
