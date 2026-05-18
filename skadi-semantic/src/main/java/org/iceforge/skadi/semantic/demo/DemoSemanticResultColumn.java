package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Objects;

/**
 * Descriptor for a single column in a {@link DemoSemanticQueryExecutionResponse}.
 *
 * <p>Carries the column identifier, a UI-friendly display name, the logical data
 * type string, and an optional semantic role hint for chart-axis binding.
 *
 * <pre>{@code
 * new DemoSemanticResultColumn("delta_exposure", "Delta Exposure", "DECIMAL", "MEASURE");
 * new DemoSemanticResultColumn("legal_entity",   "Legal Entity",   "STRING",  "DIMENSION");
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticResultColumn(
        String name,
        String displayName,
        String dataType,
        String semanticRole) {

    /**
     * @param name         column identifier matching result row keys; must not be null or blank
     * @param displayName  human-readable label for UI; must not be null
     * @param dataType     logical data type string (e.g. "DECIMAL", "STRING", "DATE");
     *                     must not be null
     * @param semanticRole optional semantic role hint (e.g. "MEASURE", "DIMENSION",
     *                     "TIME_AXIS"); null when not declared
     */
    public DemoSemanticResultColumn {
        Objects.requireNonNull(name,        "name must not be null");
        Objects.requireNonNull(displayName, "displayName must not be null");
        Objects.requireNonNull(dataType,    "dataType must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
    }

    /** Convenience constructor with no semantic role. */
    public DemoSemanticResultColumn(String name, String displayName, String dataType) {
        this(name, displayName, dataType, null);
    }
}
