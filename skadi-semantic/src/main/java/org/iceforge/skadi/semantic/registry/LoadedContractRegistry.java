package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A read-only {@link ContractRegistry} populated from a list of {@link SemanticContract}
 * instances at construction time.
 *
 * <p>This is the runtime registry produced by {@link ContractRegistryPopulator} after
 * validation passes. Once constructed it cannot be modified — {@link #register} and
 * {@link #remove} throw {@link UnsupportedOperationException}.
 *
 * <p>Contract order is the insertion order of the source list, preserved via
 * {@link LinkedHashMap}. {@link #list()} returns an unmodifiable snapshot in that order.
 *
 * <p>This class is package-private. Callers depend on the {@link ContractRegistry}
 * interface; the specific implementation is an internal detail of the registry package.
 */
final class LoadedContractRegistry implements ContractRegistry {

    private final Map<String, SemanticContract> contracts;

    private LoadedContractRegistry(Map<String, SemanticContract> contracts) {
        this.contracts = contracts; // already unmodifiable
    }

    /**
     * Builds a {@link LoadedContractRegistry} from the given list.
     *
     * <p>Contracts are stored in the list's iteration order. Duplicate names cause a
     * {@link DuplicateContractException} — validate the list with
     * {@link org.iceforge.skadi.semantic.validation.SemanticContractValidator} first to
     * surface duplicates as typed {@link org.iceforge.skadi.semantic.validation.ContractValidationIssue}s
     * rather than exceptions.
     *
     * @param contracts ordered list of contracts to register; must not be null;
     *                  elements must not be null
     * @throws DuplicateContractException if any two contracts share a name
     */
    static LoadedContractRegistry of(List<SemanticContract> contracts) {
        Objects.requireNonNull(contracts, "contracts must not be null");
        var map = new LinkedHashMap<String, SemanticContract>(contracts.size() * 2);
        for (var c : contracts) {
            Objects.requireNonNull(c, "contracts list must not contain null elements");
            if (map.putIfAbsent(c.name(), c) != null) {
                throw new DuplicateContractException(c.name());
            }
        }
        return new LoadedContractRegistry(Collections.unmodifiableMap(map));
    }

    /** @throws UnsupportedOperationException always — this registry is read-only after population */
    @Override
    public void register(SemanticContract contract) {
        throw new UnsupportedOperationException(
                "LoadedContractRegistry is read-only; use ContractRegistryPopulator to build a new registry");
    }

    @Override
    public Optional<SemanticContract> findByName(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return Optional.ofNullable(contracts.get(name));
    }

    @Override
    public List<SemanticContract> list() {
        return List.copyOf(contracts.values());
    }

    /**
     * Returns all contracts in the registry.
     *
     * <p>Access policy enforcement is deferred to post-Lane D. This implementation
     * returns the same result as {@link #list()} regardless of the principal name.
     */
    @Override
    public List<SemanticContract> forPrincipal(String principalName) {
        Objects.requireNonNull(principalName, "principalName must not be null");
        return list();
    }

    @Override
    public boolean contains(String name) {
        Objects.requireNonNull(name, "name must not be null");
        return contracts.containsKey(name);
    }

    /** @throws UnsupportedOperationException always — this registry is read-only after population */
    @Override
    public void remove(String name) {
        throw new UnsupportedOperationException(
                "LoadedContractRegistry is read-only; use ContractRegistryPopulator to build a new registry");
    }
}
