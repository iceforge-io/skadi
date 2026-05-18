package org.iceforge.skadi.semantic.demo;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.request.SemanticValidationOutcome;
import org.iceforge.skadi.semantic.request.SemanticValidationReasonCode;
import org.iceforge.skadi.semantic.service.ExecutionStatus;
import org.iceforge.skadi.semantic.service.SemanticExecutionFailure;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the demo semantic query DTOs.
 *
 * <p>Covers issue #123 definition-of-done criteria:
 * DTOs represent exposure-grid and historical-time-series requests,
 * Jackson round-trips, validation outcomes, denied/unsupported interpretations,
 * execution metadata, result rows, and absence of raw SQL fields.
 *
 * <p>No Spring context, no Databricks, no S3, no network calls,
 * no skadi-sql-gateway changes.
 */
class DemoSemanticQueryDtoTest {

    private static final String GRID_CONTRACT = "risk_sensitivity_exposure_grid";
    private static final String TS_CONTRACT   = "risk_sensitivity_historical_timeseries";

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    }

    // ── 1. DTOs can represent exposure-grid requests ───────────────────────────

    @Test
    void exposureGrid_queryRequest_holdsText() {
        var req = new DemoSemanticQueryRequest(
                "Show me delta exposure by legal entity and risk class for CIB in USD as of 2026-05-15");
        assertEquals(
                "Show me delta exposure by legal entity and risk class for CIB in USD as of 2026-05-15",
                req.text());
        assertNull(req.limit());
    }

    @Test
    void exposureGrid_interpretation_holdsAllFields() {
        var interp = exposureGridInterpretation();

        assertEquals(GRID_CONTRACT,                              interp.contractId());
        assertEquals("delta_exposure",                          interp.measure());
        assertEquals(List.of("legal_entity", "risk_class"),     interp.groupBy());
        assertEquals(DemoSemanticOutputShape.GRID,              interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH, interp.confidence());
        assertTrue(interp.warnings().isEmpty());
    }

    @Test
    void exposureGrid_filters_arePreserved() {
        var interp = exposureGridInterpretation();

        assertEquals(3, interp.filters().size());
        assertTrue(interp.filters().stream().anyMatch(f -> f.name().equals("org")      && "CIB".equals(f.value())));
        assertTrue(interp.filters().stream().anyMatch(f -> f.name().equals("currency") && "USD".equals(f.value())));
        assertTrue(interp.filters().stream().anyMatch(f -> f.name().equals("cob_date") && "2026-05-15".equals(f.value())));
    }

    @Test
    void exposureGrid_interpretation_isExecutable() {
        assertTrue(exposureGridInterpretation().isExecutable());
    }

    // ── 2. DTOs can represent historical-time-series requests ─────────────────

    @Test
    void timeseries_queryRequest_holdsText() {
        var req = new DemoSemanticQueryRequest(
                "Show dv01 trend by desk for CIB rates over the last 30 days", 100);
        assertEquals("Show dv01 trend by desk for CIB rates over the last 30 days", req.text());
        assertEquals(100, req.limit());
    }

    @Test
    void timeseries_interpretation_holdsAllFields() {
        var interp = timeseriesInterpretation();

        assertEquals(TS_CONTRACT,                               interp.contractId());
        assertEquals("dv01",                                    interp.measure());
        assertEquals(List.of("desk"),                           interp.groupBy());
        assertEquals(DemoSemanticOutputShape.TIMESERIES,        interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH, interp.confidence());
    }

    @Test
    void timeseries_filters_includeDateRange() {
        var interp = timeseriesInterpretation();

        assertTrue(interp.filters().stream().anyMatch(f -> f.name().equals("org")        && "CIB".equals(f.value())));
        assertTrue(interp.filters().stream().anyMatch(f -> f.name().equals("risk_class") && "rates".equals(f.value())));
        assertTrue(interp.filters().stream().anyMatch(f -> f.name().equals("date_range") && "last_30_days".equals(f.value())));
    }

    @Test
    void timeseries_interpretation_isExecutable() {
        assertTrue(timeseriesInterpretation().isExecutable());
    }

    // ── 3. Jackson round-trip ─────────────────────────────────────────────────

    @Test
    void jacksonRoundTrip_queryRequest() throws Exception {
        var req = new DemoSemanticQueryRequest("Show delta exposure for CIB", 50);
        var rt  = mapper.readValue(mapper.writeValueAsString(req), DemoSemanticQueryRequest.class);
        assertEquals(req.text(),  rt.text());
        assertEquals(req.limit(), rt.limit());
    }

    @Test
    void jacksonRoundTrip_queryRequest_noLimit() throws Exception {
        var req = new DemoSemanticQueryRequest("Show delta exposure");
        var rt  = mapper.readValue(mapper.writeValueAsString(req), DemoSemanticQueryRequest.class);
        assertEquals(req.text(), rt.text());
        assertNull(rt.limit());
    }

    @Test
    void jacksonRoundTrip_filter_equality() throws Exception {
        var f  = DemoSemanticFilter.eq("currency", "USD");
        var rt = mapper.readValue(mapper.writeValueAsString(f), DemoSemanticFilter.class);
        assertEquals("currency", rt.name());
        assertEquals("USD",      rt.value());
        assertNull(rt.operator());
        assertNull(rt.values());
    }

    @Test
    void jacksonRoundTrip_filter_in() throws Exception {
        var f  = DemoSemanticFilter.in("currency", List.of("USD", "EUR"));
        var rt = mapper.readValue(mapper.writeValueAsString(f), DemoSemanticFilter.class);
        assertEquals("currency", rt.name());
        assertEquals("IN",       rt.operator());
        assertEquals(List.of("USD", "EUR"), rt.values());
        assertNull(rt.value());
    }

    @Test
    void jacksonRoundTrip_filter_between() throws Exception {
        var f  = DemoSemanticFilter.between("cob_date", "2026-04-01", "2026-05-15");
        var rt = mapper.readValue(mapper.writeValueAsString(f), DemoSemanticFilter.class);
        assertEquals("BETWEEN",   rt.operator());
        assertEquals("2026-04-01", rt.start());
        assertEquals("2026-05-15", rt.end());
        assertNull(rt.value());
    }

    @Test
    void jacksonRoundTrip_interpretation_exposureGrid() throws Exception {
        var interp = exposureGridInterpretation();
        var json   = mapper.writeValueAsString(interp);
        var rt     = mapper.readValue(json, DemoSemanticQueryInterpretation.class);
        assertEquals(interp.contractId(),  rt.contractId());
        assertEquals(interp.measure(),     rt.measure());
        assertEquals(interp.groupBy(),     rt.groupBy());
        assertEquals(interp.outputShape(), rt.outputShape());
        assertEquals(interp.confidence(),  rt.confidence());
        assertEquals(interp.filters().size(), rt.filters().size());
    }

    @Test
    void jacksonRoundTrip_resultColumn() throws Exception {
        var col = new DemoSemanticResultColumn("delta_exposure", "Delta Exposure", "DECIMAL", "MEASURE");
        var rt  = mapper.readValue(mapper.writeValueAsString(col), DemoSemanticResultColumn.class);
        assertEquals(col.name(),         rt.name());
        assertEquals(col.displayName(),  rt.displayName());
        assertEquals(col.dataType(),     rt.dataType());
        assertEquals(col.semanticRole(), rt.semanticRole());
    }

    @Test
    void jacksonRoundTrip_resultColumn_noRole() throws Exception {
        var col = new DemoSemanticResultColumn("legal_entity", "Legal Entity", "STRING");
        var rt  = mapper.readValue(mapper.writeValueAsString(col), DemoSemanticResultColumn.class);
        assertNull(rt.semanticRole());
    }

    @Test
    void jacksonRoundTrip_executionMetadata_completed() throws Exception {
        var meta = new DemoSemanticExecutionMetadata(
                GRID_CONTRACT, "q-001", null, 42L, 210L, ExecutionStatus.COMPLETED, null);
        var rt   = mapper.readValue(mapper.writeValueAsString(meta), DemoSemanticExecutionMetadata.class);
        assertEquals(meta.contractId(),      rt.contractId());
        assertEquals(meta.queryId(),         rt.queryId());
        assertEquals(meta.rowCount(),        rt.rowCount());
        assertEquals(meta.executionStatus(), rt.executionStatus());
        assertNull(rt.cacheStatus());
        assertNull(rt.failureClassification());
    }

    @Test
    void jacksonRoundTrip_executionResponse() throws Exception {
        var resp = minimalGridResponse();
        var json = mapper.writeValueAsString(resp);
        var rt   = mapper.readValue(json, DemoSemanticQueryExecutionResponse.class);
        assertEquals(resp.requestText(),               rt.requestText());
        assertEquals(resp.interpretation().contractId(), rt.interpretation().contractId());
        assertEquals(resp.metadata().rowCount(),       rt.metadata().rowCount());
        assertEquals(resp.columns().size(),            rt.columns().size());
        assertEquals(resp.rows().size(),               rt.rows().size());
    }

    // ── 4. Allowed validation outcome ─────────────────────────────────────────

    @Test
    void validation_allowed_integratesWithResponse() {
        var validation = SemanticValidationOutcome.allowed(GRID_CONTRACT,
                "delta_exposure by legal_entity and risk_class is permitted.");
        var resp = new DemoSemanticQueryExecutionResponse(
                "Show delta exposure for CIB",
                exposureGridInterpretation(),
                validation,
                completedMetadata(GRID_CONTRACT, 10),
                List.of(new DemoSemanticResultColumn("delta_exposure", "Delta Exposure", "DECIMAL", "MEASURE")),
                List.of(Map.of("delta_exposure", 12345.67)));

        assertTrue(resp.validation().allowed());
        assertEquals(GRID_CONTRACT, resp.validation().contractId());
        assertEquals(1, resp.rows().size());
    }

    // ── 5. Denied / unsupported / ambiguous outcomes ──────────────────────────

    @Test
    void confidence_unsupported_isNotExecutable() {
        var interp = new DemoSemanticQueryInterpretation(
                "What is the weather like today?",
                null, null,
                List.of(), List.of(),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.UNSUPPORTED,
                List.of("No matching contract found for this request."));

        assertFalse(interp.isExecutable());
        assertNull(interp.contractId());
        assertNull(interp.measure());
        assertFalse(interp.warnings().isEmpty());
    }

    @Test
    void confidence_ambiguous_isNotExecutable() {
        var interp = new DemoSemanticQueryInterpretation(
                "Show me risk",
                GRID_CONTRACT, null,
                List.of(), List.of(),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.AMBIGUOUS,
                List.of("'risk' matches delta_exposure, dv01, vega, gamma — please be more specific."));

        assertFalse(interp.isExecutable());
    }

    @Test
    void validation_denied_unknownContract_representable() {
        var outcome = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.UNKNOWN_CONTRACT,
                "Contract 'foo' is not registered.");
        assertFalse(outcome.allowed());
        assertEquals(SemanticValidationReasonCode.UNKNOWN_CONTRACT, outcome.reasonCode());
    }

    @Test
    void validation_denied_unsupportedOutputShape_representable() {
        var outcome = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.UNSUPPORTED_OUTPUT_SHAPE,
                "Output shape TIMESERIES is not supported by risk_sensitivity_exposure_grid.",
                GRID_CONTRACT,
                List.of("outputShape"));
        assertFalse(outcome.allowed());
        assertEquals(GRID_CONTRACT, outcome.contractId());
    }

    @Test
    void validation_denied_ambiguousRequest_representable() {
        var outcome = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.AMBIGUOUS_REQUEST,
                "The term 'risk' matches multiple measures. Please be more specific.");
        assertFalse(outcome.allowed());
        assertNull(outcome.contractId());
    }

    @Test
    void confidence_allValues_representable() {
        for (var c : DemoSemanticInterpretationConfidence.values()) {
            assertNotNull(c.name());
        }
        var values = List.of(DemoSemanticInterpretationConfidence.values());
        assertTrue(values.contains(DemoSemanticInterpretationConfidence.HIGH));
        assertTrue(values.contains(DemoSemanticInterpretationConfidence.MEDIUM));
        assertTrue(values.contains(DemoSemanticInterpretationConfidence.LOW));
        assertTrue(values.contains(DemoSemanticInterpretationConfidence.UNSUPPORTED));
        assertTrue(values.contains(DemoSemanticInterpretationConfidence.AMBIGUOUS));
    }

    // ── 6. Result metadata and result rows ────────────────────────────────────

    @Test
    void executionMetadata_completed_holdsFields() {
        var meta = new DemoSemanticExecutionMetadata(
                GRID_CONTRACT, "q-xyz", "ABSENT", 42L, 310L, ExecutionStatus.COMPLETED, null);
        assertEquals(GRID_CONTRACT,           meta.contractId());
        assertEquals("q-xyz",                 meta.queryId());
        assertEquals("ABSENT",                meta.cacheStatus());
        assertEquals(42L,                     meta.rowCount());
        assertEquals(310L,                    meta.elapsedMs());
        assertEquals(ExecutionStatus.COMPLETED, meta.executionStatus());
        assertNull(meta.failureClassification());
    }

    @Test
    void executionMetadata_cacheHit_holdsFields() {
        var meta = new DemoSemanticExecutionMetadata(
                TS_CONTRACT, "q-cache", "FRESH", 18L, null, ExecutionStatus.CACHE_HIT, null);
        assertEquals(ExecutionStatus.CACHE_HIT, meta.executionStatus());
        assertEquals("FRESH",                   meta.cacheStatus());
        assertNull(meta.elapsedMs());
    }

    @Test
    void executionMetadata_failed_holdsFailureClassification() {
        var meta = new DemoSemanticExecutionMetadata(
                GRID_CONTRACT, "q-fail", null, 0L, null,
                ExecutionStatus.FAILED, SemanticExecutionFailure.UNAVAILABLE);
        assertEquals(ExecutionStatus.FAILED,             meta.executionStatus());
        assertEquals(SemanticExecutionFailure.UNAVAILABLE, meta.failureClassification());
    }

    @Test
    void executionMetadata_failureClassification_rejectedWhenNotFailed() {
        assertThrows(IllegalArgumentException.class, () ->
                new DemoSemanticExecutionMetadata(
                        GRID_CONTRACT, "q-bad", null, 5L, null,
                        ExecutionStatus.COMPLETED, SemanticExecutionFailure.TIMEOUT));
    }

    @Test
    void resultRows_representMultipleRows() {
        var rows = List.of(
                Map.<String, Object>of("legal_entity", "CIB-US", "delta_exposure", 12345.67),
                Map.<String, Object>of("legal_entity", "CIB-EU", "delta_exposure", 9876.54));
        var resp = new DemoSemanticQueryExecutionResponse(
                "Show delta exposure",
                exposureGridInterpretation(),
                SemanticValidationOutcome.allowed(GRID_CONTRACT, "OK"),
                completedMetadata(GRID_CONTRACT, rows.size()),
                List.of(new DemoSemanticResultColumn("legal_entity",   "Legal Entity",   "STRING",  "DIMENSION"),
                        new DemoSemanticResultColumn("delta_exposure",  "Delta Exposure", "DECIMAL", "MEASURE")),
                rows);

        assertEquals(2, resp.rows().size());
        assertEquals("CIB-US", resp.rows().get(0).get("legal_entity"));
        assertEquals(9876.54,  resp.rows().get(1).get("delta_exposure"));
    }

    @Test
    void resultRows_emptyList_safeForDeniedRequest() {
        var resp = new DemoSemanticQueryExecutionResponse(
                "What is the weather?",
                new DemoSemanticQueryInterpretation(
                        "What is the weather?",
                        null, null,
                        List.of(), List.of(),
                        DemoSemanticOutputShape.GRID,
                        DemoSemanticInterpretationConfidence.UNSUPPORTED,
                        List.of("No matching contract.")),
                SemanticValidationOutcome.denied(
                        SemanticValidationReasonCode.UNSUPPORTED_REQUEST,
                        "Request cannot be fulfilled by any demo contract."),
                new DemoSemanticExecutionMetadata(
                        "unknown", "q-noop", null, 0L, null, ExecutionStatus.FAILED,
                        SemanticExecutionFailure.UNEXPECTED),
                List.of(),
                List.of());

        assertFalse(resp.validation().allowed());
        assertTrue(resp.columns().isEmpty());
        assertTrue(resp.rows().isEmpty());
    }

    // ── 7. No raw SQL fields ───────────────────────────────────────────────────

    @Test
    void queryRequest_hasNoSqlField() throws Exception {
        var req  = new DemoSemanticQueryRequest("Show delta exposure");
        var json = mapper.writeValueAsString(req);
        assertFalse(json.contains("sql"),    "request must not expose a sql field");
        assertFalse(json.contains("SELECT"), "request must not expose SQL content");
    }

    @Test
    void interpretation_hasNoSqlField() throws Exception {
        var interp = exposureGridInterpretation();
        var json   = mapper.writeValueAsString(interp);
        assertFalse(json.contains("\"sql\""),    "interpretation must not expose a sql field");
        assertFalse(json.contains("SELECT"),      "interpretation must not expose SQL content");
    }

    @Test
    void executionResponse_hasNoSqlField() throws Exception {
        var json = mapper.writeValueAsString(minimalGridResponse());
        assertFalse(json.contains("\"sql\""),    "response must not expose a sql field");
        assertFalse(json.contains("SELECT"),      "response must not expose SQL content");
        assertFalse(json.contains("normalizedSql"), "response must not expose normalizedSql");
    }

    // ── Defensive copying ─────────────────────────────────────────────────────

    @Test
    void interpretation_groupBy_isUnmodifiable() {
        var interp = exposureGridInterpretation();
        assertThrows(UnsupportedOperationException.class, () -> interp.groupBy().add("extra"));
    }

    @Test
    void interpretation_filters_isUnmodifiable() {
        var interp = exposureGridInterpretation();
        assertThrows(UnsupportedOperationException.class, () -> interp.filters().add(null));
    }

    @Test
    void interpretation_warnings_isUnmodifiable() {
        var interp = exposureGridInterpretation();
        assertThrows(UnsupportedOperationException.class, () -> interp.warnings().add("extra"));
    }

    @Test
    void response_columns_isUnmodifiable() {
        var resp = minimalGridResponse();
        assertThrows(UnsupportedOperationException.class, () -> resp.columns().add(null));
    }

    @Test
    void response_rows_isUnmodifiable() {
        var resp = minimalGridResponse();
        assertThrows(UnsupportedOperationException.class, () -> resp.rows().add(null));
    }

    @Test
    void filter_values_isUnmodifiable() {
        var f = DemoSemanticFilter.in("currency", List.of("USD", "EUR"));
        assertThrows(UnsupportedOperationException.class, () -> f.values().add("GBP"));
    }

    // ── Null / blank guard tests ───────────────────────────────────────────────

    @Test
    void queryRequest_rejectsNullText() {
        assertThrows(NullPointerException.class, () -> new DemoSemanticQueryRequest(null));
    }

    @Test
    void queryRequest_rejectsBlankText() {
        assertThrows(IllegalArgumentException.class, () -> new DemoSemanticQueryRequest("  "));
    }

    @Test
    void queryRequest_rejectsNonPositiveLimit() {
        assertThrows(IllegalArgumentException.class, () -> new DemoSemanticQueryRequest("text", 0));
        assertThrows(IllegalArgumentException.class, () -> new DemoSemanticQueryRequest("text", -1));
    }

    @Test
    void filter_rejectsNullName() {
        assertThrows(NullPointerException.class, () -> DemoSemanticFilter.eq(null, "val"));
    }

    @Test
    void filter_rejectsBlankName() {
        assertThrows(IllegalArgumentException.class, () -> DemoSemanticFilter.eq("", "val"));
    }

    @Test
    void resultColumn_rejectsNullName() {
        assertThrows(NullPointerException.class, () ->
                new DemoSemanticResultColumn(null, "Display", "STRING"));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static DemoSemanticQueryInterpretation exposureGridInterpretation() {
        return new DemoSemanticQueryInterpretation(
                "Show me delta exposure by legal entity and risk class for CIB in USD as of 2026-05-15",
                GRID_CONTRACT,
                "delta_exposure",
                List.of("legal_entity", "risk_class"),
                List.of(
                        DemoSemanticFilter.eq("org",      "CIB"),
                        DemoSemanticFilter.eq("currency", "USD"),
                        DemoSemanticFilter.eq("cob_date", "2026-05-15")),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH,
                List.of());
    }

    private static DemoSemanticQueryInterpretation timeseriesInterpretation() {
        return new DemoSemanticQueryInterpretation(
                "Show dv01 trend by desk for CIB rates over the last 30 days",
                TS_CONTRACT,
                "dv01",
                List.of("desk"),
                List.of(
                        DemoSemanticFilter.eq("org",        "CIB"),
                        DemoSemanticFilter.eq("risk_class", "rates"),
                        DemoSemanticFilter.eq("date_range", "last_30_days")),
                DemoSemanticOutputShape.TIMESERIES,
                DemoSemanticInterpretationConfidence.HIGH,
                List.of());
    }

    private static DemoSemanticExecutionMetadata completedMetadata(String contractId, long rowCount) {
        return new DemoSemanticExecutionMetadata(
                contractId, "q-test", null, rowCount, 150L, ExecutionStatus.COMPLETED, null);
    }

    private DemoSemanticQueryExecutionResponse minimalGridResponse() {
        return new DemoSemanticQueryExecutionResponse(
                "Show delta exposure by legal entity for CIB USD as of 2026-05-15",
                exposureGridInterpretation(),
                SemanticValidationOutcome.allowed(GRID_CONTRACT, "Request is permitted."),
                completedMetadata(GRID_CONTRACT, 2),
                List.of(
                        new DemoSemanticResultColumn("legal_entity",  "Legal Entity",   "STRING",  "DIMENSION"),
                        new DemoSemanticResultColumn("delta_exposure", "Delta Exposure", "DECIMAL", "MEASURE")),
                List.of(
                        Map.of("legal_entity", "CIB-US", "delta_exposure", 12345.67),
                        Map.of("legal_entity", "CIB-EU", "delta_exposure", 9876.54)));
    }
}
