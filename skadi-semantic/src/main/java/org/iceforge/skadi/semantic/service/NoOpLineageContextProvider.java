package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.query.SemanticReference;

import java.util.List;
import java.util.Objects;

/**
 * A no-op {@link LineageContextProvider} that returns an empty reference list.
 *
 * <p>Use this implementation when lineage context is not required — for example
 * in tests, in deployments where BCBS239 integration has not been activated,
 * or in the SQL-first execution path where no contract is resolved.
 *
 * <p>BCBS239 lineage and Market Risk Brain integration are post-Lane C
 * concerns tracked by DQR-003.
 */
public final class NoOpLineageContextProvider implements LineageContextProvider {

    /** Singleton instance — this class has no state. */
    public static final NoOpLineageContextProvider INSTANCE = new NoOpLineageContextProvider();

    @Override
    public List<SemanticReference> referencesFor(ExecutionContext context, String contractName) {
        Objects.requireNonNull(context,      "context must not be null");
        Objects.requireNonNull(contractName, "contractName must not be null");
        return List.of();
    }
}
