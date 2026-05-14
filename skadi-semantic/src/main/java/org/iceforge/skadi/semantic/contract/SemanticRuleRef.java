package org.iceforge.skadi.semantic.contract;

import java.util.Objects;

/**
 * A reference to an external governance rule that applies to a
 * {@link SemanticContract}.
 *
 * <p>A {@code SemanticRuleRef} is a pointer only — it carries an identifier
 * and an optional description. It does not contain rule logic, does not
 * evaluate conditions, and does not execute SQL. The actual rule lives in
 * an external governance system or a future semantic planner (post-Lane C).
 *
 * <p>Examples of rules that might be referenced:
 * <ul>
 *   <li>A BCBS239 data lineage requirement ID</li>
 *   <li>A data-quality check identifier</li>
 *   <li>An access governance policy reference</li>
 * </ul>
 *
 * <pre>{@code
 * var ref = new SemanticRuleRef("DQ-042", "PnL must not be null");
 * }</pre>
 */
public record SemanticRuleRef(
        String ruleId,
        String description) {

    /**
     * @param ruleId      external rule identifier; must not be null or blank
     * @param description human-readable description of the rule reference;
     *                    may be empty but not null
     */
    public SemanticRuleRef {
        Objects.requireNonNull(ruleId, "ruleId must not be null");
        Objects.requireNonNull(description, "description must not be null");
        if (ruleId.isBlank()) {
            throw new IllegalArgumentException("ruleId must not be blank");
        }
    }
}
