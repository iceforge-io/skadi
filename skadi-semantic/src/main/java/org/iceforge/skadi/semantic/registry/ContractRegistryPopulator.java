package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.loader.JsonContractLoader;
import org.iceforge.skadi.semantic.validation.SemanticContractValidator;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * Validates and populates a {@link ContractRegistry} from a list of
 * {@link SemanticContract} instances.
 *
 * <p>Population follows a two-step pipeline:
 * <ol>
 *   <li><strong>Validate</strong> — {@link SemanticContractValidator} checks all contracts
 *       for structural issues and cross-contract consistency (duplicate names, empty
 *       measure/dimension lists, unsupported cache strategies, etc.).</li>
 *   <li><strong>Register</strong> — if validation produces no ERROR issues, the contracts
 *       are loaded into an immutable {@link LoadedContractRegistry} in input-list order.</li>
 * </ol>
 *
 * <p>Warnings (e.g. no measures defined, DATASET_VERSION cache strategy) do not block
 * registration. The caller should log warnings but may proceed with the registry.
 *
 * <p>The returned registry is read-only: {@link ContractRegistry#register} and
 * {@link ContractRegistry#remove} throw {@link UnsupportedOperationException}.
 *
 * <p>This class has no Spring dependencies and does not require a Spring application
 * context. It does not execute SQL, call Databricks, call S3, or enforce entitlements.
 *
 * <h2>Usage</h2>
 *
 * <pre>{@code
 * var populator = ContractRegistryPopulator.create();
 *
 * // Simple path — throws on validation errors
 * ContractRegistry registry = populator.populate(contracts);
 *
 * // Structured path — inspect warnings without exception handling
 * ContractRegistryPopulationResult result = populator.populateWithResult(contracts);
 * if (result.hasWarnings()) { /* log warnings *\/ }
 * if (!result.isSuccess())  { /* handle errors *\/ }
 * ContractRegistry registry = result.registry();
 *
 * // End-to-end from file paths (composes D3 loader + D4 populator)
 * ContractRegistryPopulationResult r = populator.populateFromPaths(paths);
 * }</pre>
 */
public final class ContractRegistryPopulator {

    private final SemanticContractValidator validator;

    private ContractRegistryPopulator(SemanticContractValidator validator) {
        this.validator = validator;
    }

    /** Returns a new, stateless populator instance. */
    public static ContractRegistryPopulator create() {
        return new ContractRegistryPopulator(SemanticContractValidator.create());
    }

    /**
     * Validates and populates a registry from the given contracts, throwing on validation errors.
     *
     * <p>Warnings are silently ignored by this method. Use {@link #populateWithResult} if
     * warning visibility is required.
     *
     * @param contracts the contracts to register; must not be null; elements must not be null
     * @return a read-only, populated {@link ContractRegistry}; never null
     * @throws ContractRegistryPopulationException if validation produces any ERROR issues
     */
    public ContractRegistry populate(List<SemanticContract> contracts) {
        var result = populateWithResult(contracts);
        if (!result.isSuccess()) {
            throw new ContractRegistryPopulationException(result.validationResult());
        }
        return result.registry();
    }

    /**
     * Validates and populates a registry from the given contracts, returning a structured result.
     *
     * <p>If validation has ERROR issues, {@link ContractRegistryPopulationResult#registry()} is
     * {@code null} and {@link ContractRegistryPopulationResult#isSuccess()} is {@code false}.
     * If validation has only WARNING issues (or no issues), the registry is populated and
     * {@link ContractRegistryPopulationResult#isSuccess()} is {@code true}.
     *
     * @param contracts the contracts to validate and register; must not be null;
     *                  elements must not be null
     * @return structured result; never null
     */
    public ContractRegistryPopulationResult populateWithResult(List<SemanticContract> contracts) {
        Objects.requireNonNull(contracts, "contracts must not be null");
        var validationResult = validator.validateAll(contracts);
        if (validationResult.hasErrors()) {
            return new ContractRegistryPopulationResult(null, validationResult, 0);
        }
        var registry = LoadedContractRegistry.of(contracts);
        return new ContractRegistryPopulationResult(registry, validationResult, contracts.size());
    }

    /**
     * Loads contracts from the given JSON file paths using {@link JsonContractLoader},
     * then validates and populates a registry.
     *
     * <p>This is a convenience composition of the D3 loader and D4 populator. File loading
     * errors throw {@link org.iceforge.skadi.semantic.loader.ContractLoadException} before
     * validation begins.
     *
     * @param paths ordered list of JSON contract file paths; must not be null
     * @return structured result; never null
     * @throws org.iceforge.skadi.semantic.loader.ContractLoadException if any path cannot
     *         be opened or parsed
     */
    public ContractRegistryPopulationResult populateFromPaths(List<Path> paths) {
        Objects.requireNonNull(paths, "paths must not be null");
        var contracts = JsonContractLoader.create().loadAll(paths);
        return populateWithResult(contracts);
    }
}
