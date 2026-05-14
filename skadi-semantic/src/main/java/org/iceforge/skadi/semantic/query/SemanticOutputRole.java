package org.iceforge.skadi.semantic.query;

/**
 * The semantic role of a column in a {@link SemanticOutputShape}.
 *
 * <p>Roles allow consumers (UI bricks, AI context builders, Tableau adapters)
 * to understand the intent of a column beyond its data type. This is a
 * descriptor only — no rendering or execution logic exists here.
 */
public enum SemanticOutputRole {

    /**
     * An aggregated metric value, such as {@code SUM(pnl)} or {@code AVG(delta)}.
     * Consumers typically display measures as numerical values.
     */
    MEASURE,

    /**
     * A grouping or filtering attribute, such as {@code book} or {@code desk}.
     * Consumers typically display dimensions as axis labels or filter chips.
     */
    DIMENSION,

    /**
     * A date or datetime column used to anchor the result in time,
     * such as {@code cob_date} or {@code trade_date}.
     */
    TIMESTAMP,

    /**
     * A surrogate or natural key used to identify a row uniquely,
     * such as {@code trade_id} or {@code position_id}.
     * Identifiers are typically not displayed but may be used for linking.
     */
    IDENTIFIER,

    /**
     * A human-readable description string, such as a counterparty name or
     * a product description. Labels are not aggregated or used for grouping.
     */
    LABEL,

    /**
     * A column derived from other columns by a computation not directly
     * tied to a source measure or dimension — for example a running total
     * or a ratio computed post-aggregation.
     */
    DERIVED
}
