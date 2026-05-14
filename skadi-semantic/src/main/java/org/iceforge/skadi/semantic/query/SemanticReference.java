package org.iceforge.skadi.semantic.query;

import java.util.Objects;

/**
 * A lightweight pointer to an entity in an external system.
 *
 * <p>A {@code SemanticReference} carries just enough information to identify a
 * related entity — it does not resolve the reference, call any external system,
 * or validate that the target exists. Resolution is the responsibility of a
 * future lineage or governance integration (post-Lane C).
 *
 * <p>Examples:
 * <ul>
 *   <li>A Databricks table column:
 *       {@code source="databricks", type="column", id="main.risk.gold_risk.pnl"}</li>
 *   <li>A BCBS239 data element:
 *       {@code source="bcbs239", type="data-element", id="BCBS239-PNL-001"}</li>
 *   <li>A lineage node:
 *       {@code source="lineage", type="query", id="mxl_risk_pnl_by_book_v1"}</li>
 * </ul>
 *
 * <pre>{@code
 * var ref = new SemanticReference("databricks", "column", "main.risk.gold_risk.pnl", "pnl");
 * }</pre>
 */
public record SemanticReference(
        String source,
        String type,
        String id,
        String name) {

    /**
     * @param source source system identifier; must not be null or blank
     * @param type   entity type within the source system; must not be null or blank
     * @param id     unique entity identifier within the source system; must not be null or blank
     * @param name   human-readable name; may be empty but not null
     */
    public SemanticReference {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(type,   "type must not be null");
        Objects.requireNonNull(id,     "id must not be null");
        Objects.requireNonNull(name,   "name must not be null");
        if (source.isBlank()) throw new IllegalArgumentException("source must not be blank");
        if (type.isBlank())   throw new IllegalArgumentException("type must not be blank");
        if (id.isBlank())     throw new IllegalArgumentException("id must not be blank");
    }
}
