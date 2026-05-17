package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Business-facing explanation metadata for a {@link SemanticMeasure}.
 *
 * <p>Captures intended usage guidance and known caveats that help the buddy chat
 * contextualize a measure for end users. All fields are optional.
 *
 * <pre>{@code
 * var explanation = new SemanticMeasureExplanation(
 *     "Use for daily PnL reporting; do not compare across desks without normalization.",
 *     List.of("Includes unrealized positions", "FX rates are end-of-day"));
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticMeasureExplanation(
        String intendedUsage,
        List<String> caveats) {

    public SemanticMeasureExplanation {
        caveats = caveats == null ? null : List.copyOf(caveats);
    }
}
