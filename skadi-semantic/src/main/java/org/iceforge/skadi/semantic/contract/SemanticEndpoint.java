package org.iceforge.skadi.semantic.contract;

import java.util.Objects;

/**
 * Physical data binding for a {@link SemanticEntity}.
 *
 * <p>Describes the catalog, schema, and table (or view) in Databricks Unity
 * Catalog that backs a semantic entity. This is a descriptor only — no
 * connection is opened and no SQL is executed by this record.
 *
 * <p>The separation between a {@code SemanticEndpoint} (physical location) and
 * a {@link SemanticEntity} (logical name + description) means that the physical
 * table can be renamed or migrated by updating the endpoint binding without
 * changing the logical semantic model.
 *
 * <pre>{@code
 * var ep = new SemanticEndpoint("main", "risk", "gold_risk");
 * }</pre>
 */
public record SemanticEndpoint(
        String catalog,
        String schema,
        String table) {

    /**
     * @param catalog Unity Catalog catalog name; must not be null or blank
     * @param schema  schema name within the catalog; must not be null or blank
     * @param table   table or view name; must not be null or blank
     */
    public SemanticEndpoint {
        Objects.requireNonNull(catalog, "catalog must not be null");
        Objects.requireNonNull(schema, "schema must not be null");
        Objects.requireNonNull(table, "table must not be null");
        if (catalog.isBlank()) throw new IllegalArgumentException("catalog must not be blank");
        if (schema.isBlank())  throw new IllegalArgumentException("schema must not be blank");
        if (table.isBlank())   throw new IllegalArgumentException("table must not be blank");
    }

    /** Returns the fully-qualified three-part name {@code catalog.schema.table}. */
    public String fullyQualified() {
        return catalog + "." + schema + "." + table;
    }
}
