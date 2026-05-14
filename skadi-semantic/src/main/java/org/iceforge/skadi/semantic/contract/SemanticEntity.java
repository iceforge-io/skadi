package org.iceforge.skadi.semantic.contract;

import java.util.List;
import java.util.Objects;

/**
 * A logical entity that maps a name and description to a physical
 * {@link SemanticEndpoint}.
 *
 * <p>An entity is the logical representation of a dataset — it decouples the
 * semantic name (e.g. {@code "mxl_risk"}) from the physical table coordinates.
 * When the physical table is renamed or migrated, only the {@link SemanticEndpoint}
 * binding changes; the entity name and all downstream references remain stable.
 *
 * <p>An entity may carry zero or more {@link SemanticRuleRef rule references}
 * pointing to external governance rules (data-quality checks, BCBS239 lineage
 * requirements, etc.). The rules are not evaluated here.
 *
 * <pre>{@code
 * var entity = new SemanticEntity(
 *     "mxl_risk",
 *     "MXL risk gold layer — daily PnL and Greeks",
 *     new SemanticEndpoint("main", "risk", "gold_risk"),
 *     List.of());
 * }</pre>
 */
public record SemanticEntity(
        String name,
        String description,
        SemanticEndpoint endpoint,
        List<SemanticRuleRef> ruleRefs) {

    /**
     * @param name        logical entity name; must not be null or blank
     * @param description human-readable description; may be empty but not null
     * @param endpoint    physical data binding; must not be null
     * @param ruleRefs    external governance rule references; must not be null;
     *                    the list is copied defensively
     */
    public SemanticEntity {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(description, "description must not be null");
        Objects.requireNonNull(endpoint, "endpoint must not be null");
        Objects.requireNonNull(ruleRefs, "ruleRefs must not be null");
        if (name.isBlank()) throw new IllegalArgumentException("name must not be blank");
        ruleRefs = List.copyOf(ruleRefs);
    }
}
