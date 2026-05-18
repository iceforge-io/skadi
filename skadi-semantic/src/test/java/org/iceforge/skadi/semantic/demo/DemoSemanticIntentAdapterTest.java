package org.iceforge.skadi.semantic.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RuleBasedDemoSemanticIntentAdapter}.
 *
 * <p>Covers issue #124 definition-of-done criteria: all 6 required demo prompts
 * map to correct structured interpretations, case/punctuation tolerance,
 * unsupported and ambiguous prompts fail safely, and no SQL is returned.
 *
 * <p>No Spring context, no Databricks, no S3, no network calls,
 * no skadi-sql-gateway changes.
 */
class DemoSemanticIntentAdapterTest {

    private static final String GRID_CONTRACT = "risk_sensitivity_exposure_grid";
    private static final String TS_CONTRACT   = "risk_sensitivity_historical_timeseries";

    private DemoSemanticIntentAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = RuleBasedDemoSemanticIntentAdapter.create();
    }

    // ── 1. Exposure-grid prompt: delta by legal entity and risk class ──────────

    @Test
    void prompt1_deltaExposure_byLegalEntityAndRiskClass_mapsToGrid() {
        var req    = req("Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15");
        var interp = adapter.interpret(req);

        assertEquals(GRID_CONTRACT,                              interp.contractId());
        assertEquals("delta_exposure",                          interp.measure());
        assertEquals(List.of("legal_entity", "risk_class"),     interp.groupBy());
        assertEquals(DemoSemanticOutputShape.GRID,              interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH, interp.confidence());
        assertTrue(interp.isExecutable());
    }

    @Test
    void prompt1_filters_orgCurrencyDate() {
        var interp = adapter.interpret(req(
                "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15"));

        assertEquals(3, interp.filters().size());
        assertFilter(interp, "org",      "CIB");
        assertFilter(interp, "currency", "USD");
        assertFilter(interp, "cob_date", "2026-05-15");
    }

    // ── 2. Time-series prompt: last 30 days rates DV01 by desk ────────────────

    @Test
    void prompt2_last30DaysRatesDv01_byDesk_mapsToTimeseries() {
        var req    = req("Show the last 30 days of rates DV01 by desk for CIB");
        var interp = adapter.interpret(req);

        assertEquals(TS_CONTRACT,                               interp.contractId());
        assertEquals("dv01",                                    interp.measure());
        assertEquals(List.of("desk"),                           interp.groupBy());
        assertEquals(DemoSemanticOutputShape.TIMESERIES,        interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH, interp.confidence());
        assertTrue(interp.isExecutable());
    }

    @Test
    void prompt2_filters_orgRatesDateRange() {
        var interp = adapter.interpret(req("Show the last 30 days of rates DV01 by desk for CIB"));

        assertEquals(3, interp.filters().size());
        assertFilter(interp, "org",        "CIB");
        assertFilter(interp, "risk_class", "rates");
        assertFilter(interp, "date_range", "last_30_days");
    }

    // ── 3. Exposure-grid prompt: vega by risk factor ──────────────────────────

    @Test
    void prompt3_vegaExposure_byRiskFactor_mapsToGrid() {
        var req    = req("Show vega exposure by risk factor for CIB on 2026-05-15");
        var interp = adapter.interpret(req);

        assertEquals(GRID_CONTRACT,                              interp.contractId());
        assertEquals("vega",                                     interp.measure());
        assertEquals(List.of("risk_factor"),                     interp.groupBy());
        assertEquals(DemoSemanticOutputShape.GRID,               interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH,  interp.confidence());
        assertTrue(interp.isExecutable());
    }

    @Test
    void prompt3_filters_orgDate() {
        var interp = adapter.interpret(req("Show vega exposure by risk factor for CIB on 2026-05-15"));
        assertEquals(2, interp.filters().size());
        assertFilter(interp, "org",      "CIB");
        assertFilter(interp, "cob_date", "2026-05-15");
    }

    // ── 4. Exposure-grid prompt: gamma by desk and currency ───────────────────

    @Test
    void prompt4_gammaExposure_byDeskAndCurrency_mapsToGrid() {
        var req    = req("Show gamma exposure by desk and currency for CIB on 2026-05-15");
        var interp = adapter.interpret(req);

        assertEquals(GRID_CONTRACT,                              interp.contractId());
        assertEquals("gamma",                                    interp.measure());
        assertEquals(List.of("desk", "currency"),                interp.groupBy());
        assertEquals(DemoSemanticOutputShape.GRID,               interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH,  interp.confidence());
        assertTrue(interp.isExecutable());
    }

    @Test
    void prompt4_filters_orgDate_noCurrencyFilter() {
        var interp = adapter.interpret(req("Show gamma exposure by desk and currency for CIB on 2026-05-15"));
        assertEquals(2, interp.filters().size());
        assertFilter(interp, "org",      "CIB");
        assertFilter(interp, "cob_date", "2026-05-15");
        // "currency" is a groupBy dimension here, not a filter value
        assertTrue(interp.filters().stream().noneMatch(f -> f.name().equals("currency")));
    }

    // ── 5. Exposure-grid prompt: delta by desk ────────────────────────────────

    @Test
    void prompt5_deltaExposure_byDesk_mapsToGrid() {
        var req    = req("Show delta exposure by desk for CIB on 2026-05-15");
        var interp = adapter.interpret(req);

        assertEquals(GRID_CONTRACT,                              interp.contractId());
        assertEquals("delta_exposure",                          interp.measure());
        assertEquals(List.of("desk"),                           interp.groupBy());
        assertEquals(DemoSemanticOutputShape.GRID,              interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH, interp.confidence());
        assertTrue(interp.isExecutable());
    }

    // ── 6. Time-series prompt: rates DV01 history by desk ────────────────────

    @Test
    void prompt6_ratesDv01History_byDesk_mapsToTimeseries() {
        var req    = req("Show rates DV01 history by desk for CIB over the last 30 days");
        var interp = adapter.interpret(req);

        assertEquals(TS_CONTRACT,                               interp.contractId());
        assertEquals("dv01",                                    interp.measure());
        assertEquals(List.of("desk"),                           interp.groupBy());
        assertEquals(DemoSemanticOutputShape.TIMESERIES,        interp.outputShape());
        assertEquals(DemoSemanticInterpretationConfidence.HIGH, interp.confidence());
        assertTrue(interp.isExecutable());
    }

    @Test
    void prompt6_filters_orgRatesDateRange() {
        var interp = adapter.interpret(req("Show rates DV01 history by desk for CIB over the last 30 days"));
        assertEquals(3, interp.filters().size());
        assertFilter(interp, "org",        "CIB");
        assertFilter(interp, "risk_class", "rates");
        assertFilter(interp, "date_range", "last_30_days");
    }

    // ── Case insensitivity ────────────────────────────────────────────────────

    @Test
    void caseInsensitive_allCaps_mapsCorrectly() {
        var interp = adapter.interpret(req(
                "SHOW USD DELTA EXPOSURE BY LEGAL ENTITY AND RISK CLASS FOR CIB ON 2026-05-15"));

        assertEquals(GRID_CONTRACT,    interp.contractId());
        assertEquals("delta_exposure", interp.measure());
        assertEquals(List.of("legal_entity", "risk_class"), interp.groupBy());
        assertFilter(interp, "org",      "CIB");
        assertFilter(interp, "currency", "USD");
        assertFilter(interp, "cob_date", "2026-05-15");
        assertTrue(interp.isExecutable());
    }

    @Test
    void caseInsensitive_mixedCase_mapsCorrectly() {
        var interp = adapter.interpret(req(
                "Show Rates DV01 History by Desk for CIB over the Last 30 Days"));

        assertEquals(TS_CONTRACT, interp.contractId());
        assertEquals("dv01",      interp.measure());
        assertEquals(List.of("desk"), interp.groupBy());
        assertTrue(interp.isExecutable());
    }

    // ── Punctuation tolerance ─────────────────────────────────────────────────

    @Test
    void punctuation_trailingPeriod_mapsCorrectly() {
        var interp = adapter.interpret(req(
                "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15."));

        assertEquals(GRID_CONTRACT,    interp.contractId());
        assertEquals("delta_exposure", interp.measure());
        assertFilter(interp, "cob_date", "2026-05-15");
        assertTrue(interp.isExecutable());
    }

    @Test
    void punctuation_commasInPrompt_mapsCorrectly() {
        var interp = adapter.interpret(req(
                "Show USD delta exposure, by legal entity and risk class, for CIB on 2026-05-15"));

        assertEquals("delta_exposure", interp.measure());
        assertEquals(List.of("legal_entity", "risk_class"), interp.groupBy());
        assertTrue(interp.isExecutable());
    }

    @Test
    void punctuation_questionMark_mapsCorrectly() {
        var interp = adapter.interpret(req(
                "Show vega exposure by risk factor for CIB on 2026-05-15?"));

        assertEquals("vega", interp.measure());
        assertTrue(interp.isExecutable());
    }

    // ── Unsupported prompt ────────────────────────────────────────────────────

    @Test
    void unsupported_genericPrompt_returnsUnsupported() {
        var interp = adapter.interpret(req("Show me everything in the table"));

        assertEquals(DemoSemanticInterpretationConfidence.UNSUPPORTED, interp.confidence());
        assertFalse(interp.isExecutable());
        assertNull(interp.contractId());
        assertNull(interp.measure());
    }

    @Test
    void unsupported_prompt_hasWarning() {
        var interp = adapter.interpret(req("List all the data"));
        assertFalse(interp.warnings().isEmpty());
        assertTrue(interp.warnings().get(0).contains("Supported measures"));
    }

    @Test
    void unsupported_emptyGroupByAndFilters_forNonFinancial() {
        var interp = adapter.interpret(req("What is the weather today?"));
        assertEquals(DemoSemanticInterpretationConfidence.UNSUPPORTED, interp.confidence());
        assertTrue(interp.groupBy().isEmpty());
        assertTrue(interp.filters().isEmpty());
        assertFalse(interp.isExecutable());
    }

    // ── Ambiguous prompt ──────────────────────────────────────────────────────

    @Test
    void ambiguous_exposureWithoutMeasure_returnsAmbiguous() {
        var interp = adapter.interpret(req("Show exposure"));

        assertEquals(DemoSemanticInterpretationConfidence.AMBIGUOUS, interp.confidence());
        assertFalse(interp.isExecutable());
        assertNull(interp.contractId());
        assertNull(interp.measure());
    }

    @Test
    void ambiguous_prompt_hasWarning() {
        var interp = adapter.interpret(req("Show exposure"));
        assertFalse(interp.warnings().isEmpty());
        assertTrue(interp.warnings().get(0).contains("delta_exposure"));
    }

    @Test
    void ambiguous_deltaWithoutExposure_isNotResolved() {
        // "delta" alone is not enough — must be paired with "exposure"
        var interp = adapter.interpret(req("Show delta by desk for CIB on 2026-05-15"));
        assertEquals(DemoSemanticInterpretationConfidence.AMBIGUOUS, interp.confidence());
        assertFalse(interp.isExecutable());
    }

    @Test
    void ambiguous_sensitivityKeyword_isNotUnsupported() {
        // "sensitivity" is a financial keyword — should be AMBIGUOUS, not UNSUPPORTED
        var interp = adapter.interpret(req("Show sensitivity for CIB"));
        assertEquals(DemoSemanticInterpretationConfidence.AMBIGUOUS, interp.confidence());
    }

    // ── Medium confidence ─────────────────────────────────────────────────────

    @Test
    void medium_measureFoundButNoGroupBy_returnsMedium() {
        var interp = adapter.interpret(req("Show delta exposure for CIB on 2026-05-15"));

        assertEquals(DemoSemanticInterpretationConfidence.MEDIUM, interp.confidence());
        assertEquals("delta_exposure", interp.measure());
        assertTrue(interp.groupBy().isEmpty());
        // isExecutable() is false because contractId+measure are present but groupBy is empty;
        // the isExecutable() check in the DTO requires contractId != null && measure != null,
        // which holds here — so MEDIUM with measure and contractId IS executable
        assertTrue(interp.isExecutable());
        assertFalse(interp.warnings().isEmpty());
    }

    // ── Original text preserved ───────────────────────────────────────────────

    @Test
    void originalText_isPreservedVerbatim() {
        String text   = "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15";
        var    interp = adapter.interpret(req(text));
        assertEquals(text, interp.originalText());
    }

    @Test
    void originalText_preservedEvenForUnsupported() {
        String text   = "Show me everything";
        var    interp = adapter.interpret(req(text));
        assertEquals(text, interp.originalText());
    }

    // ── No SQL in returned DTOs ───────────────────────────────────────────────

    @Test
    void noSql_interpretationHasNoSqlFields() {
        var interp = adapter.interpret(req(
                "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15"));
        // DemoSemanticQueryInterpretation has no sql field; verify none of the filter
        // values contain SQL keywords
        for (DemoSemanticFilter f : interp.filters()) {
            assertFalse(f.value() != null && f.value().toUpperCase().contains("SELECT"),
                    "filter value must not contain SQL: " + f.value());
        }
        assertNull(interp.measure() != null && interp.measure().contains("SELECT")
                ? "SQL found in measure" : null);
    }

    @Test
    void noSql_unsupportedInterpretationHasNoSql() {
        var interp = adapter.interpret(req("Show me everything in the table"));
        assertTrue(interp.filters().isEmpty());
        // warnings are plain English, not SQL
        for (String w : interp.warnings()) {
            assertFalse(w.toUpperCase().contains("SELECT"), "warning must not contain SQL");
        }
    }

    // ── Null guard ─────────────────────────────────────────────────────────────

    @Test
    void nullRequest_throws() {
        assertThrows(NullPointerException.class, () -> adapter.interpret(null));
    }

    // ── Interface contract ────────────────────────────────────────────────────

    @Test
    void adapter_isInstanceOfInterface() {
        assertInstanceOf(DemoSemanticIntentAdapter.class, adapter);
    }

    @Test
    void adapter_neverReturnsNull() {
        String[] prompts = {
            "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15",
            "Show the last 30 days of rates DV01 by desk for CIB",
            "Show vega exposure by risk factor for CIB on 2026-05-15",
            "Show gamma exposure by desk and currency for CIB on 2026-05-15",
            "Show delta exposure by desk for CIB on 2026-05-15",
            "Show rates DV01 history by desk for CIB over the last 30 days",
            "Show me everything in the table",
            "Show exposure"
        };
        for (String p : prompts) {
            assertNotNull(adapter.interpret(req(p)), "interpret() must not return null for: " + p);
        }
    }

    // ── All 6 required prompts: isExecutable() ────────────────────────────────

    @Test
    void allSixDemoPrompts_areExecutable() {
        String[] supported = {
            "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15",
            "Show the last 30 days of rates DV01 by desk for CIB",
            "Show vega exposure by risk factor for CIB on 2026-05-15",
            "Show gamma exposure by desk and currency for CIB on 2026-05-15",
            "Show delta exposure by desk for CIB on 2026-05-15",
            "Show rates DV01 history by desk for CIB over the last 30 days"
        };
        for (String p : supported) {
            var interp = adapter.interpret(req(p));
            assertTrue(interp.isExecutable(),
                    "prompt should be executable: " + p + " — confidence=" + interp.confidence());
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private static DemoSemanticQueryRequest req(String text) {
        return new DemoSemanticQueryRequest(text);
    }

    private static void assertFilter(DemoSemanticQueryInterpretation interp, String name, String value) {
        assertTrue(interp.filters().stream()
                .anyMatch(f -> f.name().equals(name) && value.equals(f.value())),
                "expected filter " + name + "=" + value + " in " + interp.filters());
    }
}
