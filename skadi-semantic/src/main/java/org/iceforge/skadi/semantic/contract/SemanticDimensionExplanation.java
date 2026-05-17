package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Business-facing explanation metadata for a {@link SemanticDimension}.
 *
 * <p>Captures a description and known caveats that help the buddy chat
 * contextualize a dimension for end users. All fields are optional.
 *
 * <pre>{@code
 * var explanation = new SemanticDimensionExplanation(
 *     "The legal entity book code as per the risk system.",
 *     List.of("Book codes change during reorg periods"));
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SemanticDimensionExplanation(
        String description,
        List<String> caveats) {

    public SemanticDimensionExplanation {
        caveats = caveats == null ? null : List.copyOf(caveats);
    }
}
