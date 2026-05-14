package org.iceforge.skadi.semantic.query;

import org.iceforge.skadi.semantic.contract.SemanticContractVersion;

import java.util.List;
import java.util.Objects;

/**
 * A named, versioned query contract that declares the output shape of a
 * governed query over a {@link org.iceforge.skadi.semantic.contract.SemanticContract}.
 *
 * <p>A {@code SemanticQueryContract} answers the question: <em>"When a caller
 * queries the dataset described by {@link #sourceContract()}, what shape of
 * result do they receive?"</em> It is a static descriptor, not an executable
 * query. It does not:
 * <ul>
 *   <li>Execute SQL or call Databricks</li>
 *   <li>Hold filter conditions or parameter bindings</li>
 *   <li>Implement semantic planning or query optimisation</li>
 *   <li>Contain Spring beans or REST endpoints</li>
 * </ul>
 *
 * <p>The relationship between a {@code SemanticQueryContract} and a
 * {@link org.iceforge.skadi.semantic.contract.SemanticContract} is by name only:
 * {@link #sourceContract()} holds the contract name. No runtime lookup occurs here.
 *
 * <p>Query contracts are governance artefacts — they are reviewed in pull
 * requests and versioned with {@link SemanticContractVersion}. Breaking changes
 * to an output shape (removing or renaming a column) require a major version bump.
 *
 * <pre>{@code
 * var qc = new SemanticQueryContract(
 *     "mxl_risk_pnl_by_book",
 *     "Daily PnL aggregated by trading book",
 *     "mxl_risk",
 *     new SemanticContractVersion("1.0.0"),
 *     shape,
 *     List.of());
 * }</pre>
 */
public record SemanticQueryContract(
        String name,
        String description,
        String sourceContract,
        SemanticContractVersion version,
        SemanticOutputShape outputShape,
        List<SemanticReference> references) {

    /**
     * @param name           unique identifier for this query contract; must not be null or blank
     * @param description    human-readable purpose; may be empty but not null
     * @param sourceContract name of the {@code SemanticContract} this query is derived from;
     *                       must not be null or blank
     * @param version        contract version; must not be null
     * @param outputShape    declared output shape; must not be null
     * @param references     query-level lineage or semantic pointers; must not be null;
     *                       copied defensively
     */
    public SemanticQueryContract {
        Objects.requireNonNull(name,           "name must not be null");
        Objects.requireNonNull(description,    "description must not be null");
        Objects.requireNonNull(sourceContract, "sourceContract must not be null");
        Objects.requireNonNull(version,        "version must not be null");
        Objects.requireNonNull(outputShape,    "outputShape must not be null");
        Objects.requireNonNull(references,     "references must not be null");
        if (name.isBlank())           throw new IllegalArgumentException("name must not be blank");
        if (sourceContract.isBlank()) throw new IllegalArgumentException("sourceContract must not be blank");
        references = List.copyOf(references);
    }
}
