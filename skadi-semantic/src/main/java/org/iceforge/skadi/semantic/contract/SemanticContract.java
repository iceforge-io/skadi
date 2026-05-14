package org.iceforge.skadi.semantic.contract;

import java.util.List;
import java.util.Objects;

/**
 * Top-level semantic contract describing a governed logical dataset.
 *
 * <p>A {@code SemanticContract} is the authoritative definition of a dataset
 * that the Skadi semantic layer exposes to callers. It binds a logical name to
 * a physical {@link SemanticEntity} and declares the measures and dimensions
 * available for querying.
 *
 * <p>Contracts are versioned (see {@link SemanticContractVersion}). Breaking
 * changes — renaming a measure, removing a dimension — require a major version
 * bump and a migration note. Contracts are governance artefacts: they are
 * reviewed in pull requests and deployed alongside code.
 *
 * <p>This record is a plain data structure. It does not:
 * <ul>
 *   <li>Execute SQL or evaluate rule logic</li>
 *   <li>Load configuration from disk</li>
 *   <li>Connect to Databricks, S3, or any external system</li>
 *   <li>Contain Spring beans or runtime wiring</li>
 * </ul>
 *
 * <p>Access policy and cache policy metadata (added in C2.3):
 * <ul>
 *   <li>{@link SemanticAccessPolicy} — declares allowed roles, principals, and
 *       governance rules; descriptive only, no enforcement here</li>
 *   <li>{@link SemanticCachePolicy} — declares the preferred cache strategy and TTL;
 *       a hint to the cache layer, not an eviction command</li>
 * </ul>
 *
 * <pre>{@code
 * var contract = new SemanticContract(
 *     "mxl_risk",
 *     new SemanticContractVersion("1.0.0"),
 *     "MXL risk gold layer — daily PnL and Greeks",
 *     new SemanticEntity("mxl_risk", "...",
 *         new SemanticEndpoint("main", "risk", "gold_risk"), List.of()),
 *     List.of(new SemanticMeasure("pnl", "Total PnL", "SUM(pnl)",
 *                                 SemanticFieldType.DECIMAL, "")),
 *     List.of(new SemanticDimension("book", "book",
 *                                   SemanticFieldType.STRING, "Trading Book",
 *                                   true, true)),
 *     SemanticAccessPolicy.unrestricted(),
 *     SemanticCachePolicy.ttl(300L));
 * }</pre>
 */
public record SemanticContract(
        String name,
        SemanticContractVersion version,
        String description,
        SemanticEntity entity,
        List<SemanticMeasure> measures,
        List<SemanticDimension> dimensions,
        SemanticAccessPolicy accessPolicy,
        SemanticCachePolicy cachePolicy) {

    /**
     * @param name         unique contract identifier used in semantic queries
     *                     and dashboard bricks; must not be null or blank
     * @param version      contract version; must not be null
     * @param description  human-readable description; may be empty but not null
     * @param entity       logical entity and physical binding; must not be null
     * @param measures     metric definitions available for querying; must not be
     *                     null; the list is copied defensively
     * @param dimensions   dimension definitions available for filtering and
     *                     grouping; must not be null; the list is copied defensively
     * @param accessPolicy declared access restrictions; must not be null;
     *                     use {@link SemanticAccessPolicy#unrestricted()} for no restrictions
     * @param cachePolicy  declared cache hint; must not be null;
     *                     use {@link SemanticCachePolicy#none()} to opt out of caching
     */
    public SemanticContract {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(version, "version must not be null");
        Objects.requireNonNull(description, "description must not be null");
        Objects.requireNonNull(entity, "entity must not be null");
        Objects.requireNonNull(measures, "measures must not be null");
        Objects.requireNonNull(dimensions, "dimensions must not be null");
        Objects.requireNonNull(accessPolicy, "accessPolicy must not be null");
        Objects.requireNonNull(cachePolicy, "cachePolicy must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
        measures   = List.copyOf(measures);
        dimensions = List.copyOf(dimensions);
    }
}
