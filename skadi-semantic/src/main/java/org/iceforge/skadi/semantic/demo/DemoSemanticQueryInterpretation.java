package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Objects;

/**
 * The result of interpreting a plain-English demo query request.
 *
 * <p>An interpretation maps free-form text to a specific governed contract,
 * measure, groupings, filters, and output shape. It does not include SQL or
 * internal rendering details.
 *
 * <p>The {@link #confidence()} field indicates how reliably the text could be
 * mapped. Callers should check confidence before proceeding to rendering or
 * execution — UNSUPPORTED and AMBIGUOUS interpretations must not be executed.
 *
 * <p>{@link #warnings()} carries non-fatal advisory notes (e.g. a dimension
 * that was inferred rather than stated explicitly).
 *
 * <pre>{@code
 * // Exposure-grid interpretation
 * new DemoSemanticQueryInterpretation(
 *     "Show me delta exposure by legal entity and risk class for CIB USD as of 2026-05-15",
 *     "risk_sensitivity_exposure_grid",
 *     "delta_exposure",
 *     List.of("legal_entity", "risk_class"),
 *     List.of(DemoSemanticFilter.eq("org", "CIB"),
 *             DemoSemanticFilter.eq("currency", "USD"),
 *             DemoSemanticFilter.eq("cob_date", "2026-05-15")),
 *     DemoSemanticOutputShape.GRID,
 *     DemoSemanticInterpretationConfidence.HIGH,
 *     List.of());
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticQueryInterpretation(
        String originalText,
        String contractId,
        String measure,
        List<String> groupBy,
        List<DemoSemanticFilter> filters,
        DemoSemanticOutputShape outputShape,
        DemoSemanticInterpretationConfidence confidence,
        List<String> warnings) {

    /**
     * @param originalText the original plain-English text; must not be null
     * @param contractId   the matched contract name; null when confidence is UNSUPPORTED
     * @param measure      the matched measure name; null when confidence is UNSUPPORTED
     * @param groupBy      group-by dimension names; must not be null; copied defensively
     * @param filters      filter criteria; must not be null; copied defensively
     * @param outputShape  intended output shape; must not be null
     * @param confidence   interpretation confidence level; must not be null
     * @param warnings     advisory notes; must not be null; copied defensively
     */
    public DemoSemanticQueryInterpretation {
        Objects.requireNonNull(originalText, "originalText must not be null");
        Objects.requireNonNull(groupBy,      "groupBy must not be null");
        Objects.requireNonNull(filters,      "filters must not be null");
        Objects.requireNonNull(outputShape,  "outputShape must not be null");
        Objects.requireNonNull(confidence,   "confidence must not be null");
        Objects.requireNonNull(warnings,     "warnings must not be null");
        groupBy  = List.copyOf(groupBy);
        filters  = List.copyOf(filters);
        warnings = List.copyOf(warnings);
    }

    /** Returns true if this interpretation can proceed to SQL rendering and execution. */
    public boolean isExecutable() {
        return confidence != DemoSemanticInterpretationConfidence.UNSUPPORTED
                && confidence != DemoSemanticInterpretationConfidence.AMBIGUOUS
                && contractId != null
                && measure != null;
    }
}
