package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.iceforge.skadi.semantic.request.SemanticValidationOutcome;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The full response for a demo semantic query execution.
 *
 * <p>Carries the original request text, the interpretation that was derived from
 * it, the validation outcome (allowed or denied), execution metadata, the column
 * descriptors for the result, and the result rows.
 *
 * <p>When validation is denied or the interpretation is not executable,
 * {@link #columns()} and {@link #rows()} will typically be empty.
 *
 * <p>Result rows are plain {@code Map<String, Object>} keyed by column name,
 * matching the column names in {@link #columns()}.
 *
 * <pre>{@code
 * new DemoSemanticQueryExecutionResponse(
 *     "Show delta exposure by legal entity for CIB USD as of 2026-05-15",
 *     interpretation,
 *     SemanticValidationOutcome.allowed("risk_sensitivity_exposure_grid", "OK"),
 *     metadata,
 *     List.of(new DemoSemanticResultColumn("legal_entity", "Legal Entity", "STRING", "DIMENSION"),
 *             new DemoSemanticResultColumn("delta_exposure", "Delta Exposure", "DECIMAL", "MEASURE")),
 *     List.of(Map.of("legal_entity", "CIB-US", "delta_exposure", 12345.67)));
 * }</pre>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DemoSemanticQueryExecutionResponse(
        String requestText,
        DemoSemanticQueryInterpretation interpretation,
        SemanticValidationOutcome validation,
        DemoSemanticExecutionMetadata metadata,
        List<DemoSemanticResultColumn> columns,
        List<Map<String, Object>> rows) {

    /**
     * @param requestText    the original plain-English text; must not be null
     * @param interpretation the semantic interpretation; must not be null
     * @param validation     the validation outcome; must not be null
     * @param metadata       execution metadata; must not be null
     * @param columns        result column descriptors; must not be null; outer list copied defensively
     * @param rows           result rows keyed by column name; must not be null; outer list copied defensively
     */
    public DemoSemanticQueryExecutionResponse {
        Objects.requireNonNull(requestText,    "requestText must not be null");
        Objects.requireNonNull(interpretation, "interpretation must not be null");
        Objects.requireNonNull(validation,     "validation must not be null");
        Objects.requireNonNull(metadata,       "metadata must not be null");
        Objects.requireNonNull(columns,        "columns must not be null");
        Objects.requireNonNull(rows,           "rows must not be null");
        columns = List.copyOf(columns);
        rows    = List.copyOf(rows);
    }
}
