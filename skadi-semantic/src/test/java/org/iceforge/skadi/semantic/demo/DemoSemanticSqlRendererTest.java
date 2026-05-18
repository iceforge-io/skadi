package org.iceforge.skadi.semantic.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link WhitelistedDemoSemanticSqlRenderer}.
 *
 * <p>Covers issue #125 definition-of-done criteria: exposure-grid and time-series
 * render paths, configured view names, allowlist rejections, limit handling,
 * deterministic date expansion, and injection-safety via bind parameters.
 *
 * <p>No Spring context, no Databricks, no S3, no network calls,
 * no skadi-sql-gateway changes.
 */
class DemoSemanticSqlRendererTest {

    // Fixed clock at 2026-05-18 00:00 UTC — start_date = 2026-04-18, end_date = 2026-05-18
    private static final LocalDate FIXED_DATE        = LocalDate.of(2026, 5, 18);
    private static final LocalDate EXPECTED_START    = FIXED_DATE.minusDays(30); // 2026-04-18
    private static final Clock     FIXED_CLOCK       = Clock.fixed(
            FIXED_DATE.atStartOfDay(ZoneOffset.UTC).toInstant(), ZoneOffset.UTC);

    private static final String GRID_CONTRACT  = "risk_sensitivity_exposure_grid";
    private static final String TS_CONTRACT    = "risk_sensitivity_historical_timeseries";
    private static final String EXPOSURE_VIEW  = "demo.market_risk.v_sensitivity_exposure_demo";
    private static final String HISTORY_VIEW   = "demo.market_risk.v_sensitivity_history_demo";

    private DemoSemanticSqlRenderer renderer;

    @BeforeEach
    void setUp() {
        renderer = WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), FIXED_CLOCK);
    }

    // ── 1. Exposure-grid renders expected SQL shape and params ─────────────────

    @Test
    void grid_rendersSelectClause() {
        var result = renderer.render(gridInterp());
        String sql = result.sql();
        assertTrue(sql.contains("select legal_entity, risk_class, sum(delta_exposure) as delta_exposure"),
                "unexpected select clause: " + sql);
    }

    @Test
    void grid_rendersFromClause() {
        var result = renderer.render(gridInterp());
        assertTrue(result.sql().contains("from " + EXPOSURE_VIEW));
    }

    @Test
    void grid_rendersWhereClause_allFilters() {
        var result = renderer.render(gridInterp());
        String sql = result.sql();
        assertTrue(sql.contains("where org = :org and currency = :currency and cob_date = :cob_date"),
                "unexpected where clause: " + sql);
    }

    @Test
    void grid_rendersGroupByAndOrderBy() {
        var result = renderer.render(gridInterp());
        String sql = result.sql();
        assertTrue(sql.contains("group by legal_entity, risk_class"), sql);
        assertTrue(sql.contains("order by legal_entity, risk_class"), sql);
    }

    @Test
    void grid_rendersLimitPlaceholder() {
        assertTrue(renderer.render(gridInterp()).sql().contains("limit :limit"));
    }

    @Test
    void grid_params_containsExpectedValues() {
        var params = renderer.render(gridInterp()).params();
        assertEquals("CIB",        params.get("org"));
        assertEquals("USD",        params.get("currency"));
        assertEquals("2026-05-15", params.get("cob_date"));
    }

    @Test
    void grid_params_limitIsDefault() {
        assertEquals(WhitelistedDemoSemanticSqlRenderer.DEFAULT_LIMIT,
                renderer.render(gridInterp()).params().get("limit"));
    }

    @Test
    void grid_metadata_contractIdAndShape() {
        var result = renderer.render(gridInterp());
        assertEquals(GRID_CONTRACT,                result.contractId());
        assertEquals(DemoSemanticOutputShape.GRID,  result.outputShape());
        assertEquals("delta_exposure",             result.measure());
        assertEquals(List.of("legal_entity", "risk_class"), result.groupBy());
    }

    // ── 2. Historical time-series renders expected SQL shape and params ─────────

    @Test
    void timeseries_rendersSelectWithCobDate() {
        var result = renderer.render(tsInterp());
        assertTrue(result.sql().contains("select cob_date, desk, sum(dv01) as dv01"),
                result.sql());
    }

    @Test
    void timeseries_rendersFromHistoryView() {
        assertTrue(renderer.render(tsInterp()).sql().contains("from " + HISTORY_VIEW));
    }

    @Test
    void timeseries_rendersDateRangeFirstInWhere() {
        var result = renderer.render(tsInterp());
        String sql = result.sql();
        int dateRangeIdx = sql.indexOf("cob_date between :start_date and :end_date");
        int orgIdx       = sql.indexOf("org = :org");
        assertTrue(dateRangeIdx >= 0,  "date range not found: " + sql);
        assertTrue(orgIdx >= 0,        "org filter not found: " + sql);
        assertTrue(dateRangeIdx < orgIdx, "date range must precede org filter");
    }

    @Test
    void timeseries_rendersOtherFiltersWithAnd() {
        var result = renderer.render(tsInterp());
        assertTrue(result.sql().contains("and org = :org"),        result.sql());
        assertTrue(result.sql().contains("and risk_class = :risk_class"), result.sql());
    }

    @Test
    void timeseries_rendersGroupByWithCobDate() {
        assertTrue(renderer.render(tsInterp()).sql().contains("group by cob_date, desk"));
    }

    @Test
    void timeseries_rendersOrderByWithCobDate() {
        assertTrue(renderer.render(tsInterp()).sql().contains("order by cob_date, desk"));
    }

    @Test
    void timeseries_params_containsExpectedValues() {
        var params = renderer.render(tsInterp()).params();
        assertEquals("CIB",   params.get("org"));
        assertEquals("rates", params.get("risk_class"));
    }

    @Test
    void timeseries_metadata_contractIdAndShape() {
        var result = renderer.render(tsInterp());
        assertEquals(TS_CONTRACT,                                      result.contractId());
        assertEquals(DemoSemanticOutputShape.TIMESERIES,               result.outputShape());
        assertEquals("dv01",                                           result.measure());
        assertEquals(List.of("desk"),                                  result.groupBy());
    }

    // ── 3. Renderer uses configured exposure view for GRID ─────────────────────

    @Test
    void grid_viewName_isConfiguredExposureView() {
        assertEquals(EXPOSURE_VIEW, renderer.render(gridInterp()).viewName());
    }

    @Test
    void grid_customViewConfig_usedInSql() {
        var customConfig = new DemoViewConfig("dev.risk.exposure_v", "dev.risk.history_v");
        var customRenderer = WhitelistedDemoSemanticSqlRenderer.create(customConfig, FIXED_CLOCK);
        assertTrue(customRenderer.render(gridInterp()).sql().contains("from dev.risk.exposure_v"));
    }

    // ── 4. Renderer uses configured history view for TIMESERIES ───────────────

    @Test
    void timeseries_viewName_isConfiguredHistoryView() {
        assertEquals(HISTORY_VIEW, renderer.render(tsInterp()).viewName());
    }

    @Test
    void timeseries_customViewConfig_usedInSql() {
        var customConfig   = new DemoViewConfig("dev.risk.exposure_v", "dev.risk.history_v");
        var customRenderer = WhitelistedDemoSemanticSqlRenderer.create(customConfig, FIXED_CLOCK);
        assertTrue(customRenderer.render(tsInterp()).sql().contains("from dev.risk.history_v"));
    }

    // ── 5. Unknown contract is rejected ────────────────────────────────────────

    @Test
    void unknownContract_throws() {
        var interp = interpWithContract("unknown_contract");
        assertThrows(DemoSemanticSqlRenderException.class, () -> renderer.render(interp));
    }

    @Test
    void unknownContract_messageNamesContract() {
        var ex = assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(interpWithContract("bad_contract")));
        assertTrue(ex.getMessage().contains("bad_contract"), ex.getMessage());
    }

    // ── 6. Non-executable interpretation is rejected ───────────────────────────

    @Test
    void unsupportedInterp_throws() {
        assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(unsupportedInterp()));
    }

    @Test
    void ambiguousInterp_throws() {
        assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(ambiguousInterp()));
    }

    @Test
    void nonExecutable_messageContainsConfidence() {
        var ex = assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(unsupportedInterp()));
        assertTrue(ex.getMessage().contains("UNSUPPORTED"), ex.getMessage());
    }

    // ── 7. Unknown measure is rejected ─────────────────────────────────────────

    @Test
    void unknownMeasure_throws() {
        var interp = interpWithMeasure("net_pnl");
        assertThrows(DemoSemanticSqlRenderException.class, () -> renderer.render(interp));
    }

    @Test
    void unknownMeasure_messageNamesMeasure() {
        var ex = assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(interpWithMeasure("bad_measure")));
        assertTrue(ex.getMessage().contains("bad_measure"), ex.getMessage());
    }

    // ── 8. Unknown groupBy dimension is rejected ───────────────────────────────

    @Test
    void unknownGroupByDimension_throws() {
        var interp = interpWithGroupBy(List.of("unknown_dim"));
        assertThrows(DemoSemanticSqlRenderException.class, () -> renderer.render(interp));
    }

    @Test
    void unknownGroupByDimension_messageNamesDimension() {
        var ex = assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(interpWithGroupBy(List.of("secret_col"))));
        assertTrue(ex.getMessage().contains("secret_col"), ex.getMessage());
    }

    // ── 9. Unknown filter is rejected ──────────────────────────────────────────

    @Test
    void unknownFilter_throws() {
        var interp = interpWithExtraFilter("unknown_filter", "val");
        assertThrows(DemoSemanticSqlRenderException.class, () -> renderer.render(interp));
    }

    @Test
    void unknownFilter_messageNamesFilter() {
        var ex = assertThrows(DemoSemanticSqlRenderException.class,
                () -> renderer.render(interpWithExtraFilter("bad_filter", "val")));
        assertTrue(ex.getMessage().contains("bad_filter"), ex.getMessage());
    }

    // ── 10. Contract/shape mismatch is rejected ────────────────────────────────

    @Test
    void gridContractWithTimeseriesShape_throws() {
        var interp = new DemoSemanticQueryInterpretation(
                "test", GRID_CONTRACT, "delta_exposure",
                List.of("desk"), List.of(DemoSemanticFilter.eq("org", "CIB")),
                DemoSemanticOutputShape.TIMESERIES,  // wrong shape for GRID contract
                DemoSemanticInterpretationConfidence.HIGH, List.of());
        assertThrows(DemoSemanticSqlRenderException.class, () -> renderer.render(interp));
    }

    @Test
    void tsContractWithGridShape_throws() {
        var interp = new DemoSemanticQueryInterpretation(
                "test", TS_CONTRACT, "dv01",
                List.of("desk"), List.of(DemoSemanticFilter.eq("org", "CIB")),
                DemoSemanticOutputShape.GRID,  // wrong shape for TS contract
                DemoSemanticInterpretationConfidence.HIGH, List.of());
        assertThrows(DemoSemanticSqlRenderException.class, () -> renderer.render(interp));
    }

    // ── 11. date_range expands to deterministic start/end dates ───────────────

    @Test
    void timeseries_dateRange_startDateIsCorrect() {
        var params = renderer.render(tsInterp()).params();
        assertEquals(EXPECTED_START.toString(), params.get("start_date"),
                "start_date should be 30 days before fixed date");
    }

    @Test
    void timeseries_dateRange_endDateIsFixedDate() {
        var params = renderer.render(tsInterp()).params();
        assertEquals(FIXED_DATE.toString(), params.get("end_date"),
                "end_date should be the fixed clock date");
    }

    @Test
    void timeseries_dateRange_sqlContainsBetweenClause() {
        var sql = renderer.render(tsInterp()).sql();
        assertTrue(sql.contains("cob_date between :start_date and :end_date"), sql);
    }

    @Test
    void timeseries_dateRange_doesNotContainHardcodedDate() {
        var sql = renderer.render(tsInterp()).sql();
        // Dates must be in params, not in SQL text
        assertFalse(sql.contains(FIXED_DATE.toString()),
                "end date must not appear literally in SQL");
        assertFalse(sql.contains(EXPECTED_START.toString()),
                "start date must not appear literally in SQL");
    }

    // ── 12. Default limit is applied ───────────────────────────────────────────

    @Test
    void defaultLimit_appliedToGridRender() {
        var result = renderer.render(gridInterp());
        assertEquals(WhitelistedDemoSemanticSqlRenderer.DEFAULT_LIMIT,
                result.params().get("limit"));
    }

    @Test
    void defaultLimit_appliedToTimeseriesRender() {
        var result = renderer.render(tsInterp());
        assertEquals(WhitelistedDemoSemanticSqlRenderer.DEFAULT_LIMIT,
                result.params().get("limit"));
    }

    // ── 13. Limit cap is applied ───────────────────────────────────────────────

    @Test
    void limitAboveMax_cappedAtMax() {
        var cappedRenderer = WhitelistedDemoSemanticSqlRenderer.create(
                DemoViewConfig.defaults(), FIXED_CLOCK, 999_999);
        int limitInParams = (int) cappedRenderer.render(gridInterp()).params().get("limit");
        assertEquals(WhitelistedDemoSemanticSqlRenderer.MAX_LIMIT, limitInParams,
                "limit exceeding MAX_LIMIT must be capped");
    }

    @Test
    void limitZero_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), FIXED_CLOCK, 0));
    }

    @Test
    void limitNegative_throws() {
        assertThrows(IllegalArgumentException.class, () ->
                WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), FIXED_CLOCK, -1));
    }

    @Test
    void customLimit_belowMax_appliedExactly() {
        var r      = WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), FIXED_CLOCK, 100);
        int actual = (int) r.render(gridInterp()).params().get("limit");
        assertEquals(100, actual);
    }

    // ── 14. Rendered SQL never contains raw user-supplied text ─────────────────

    @Test
    void filterValues_notInSqlText_grid() {
        var result = renderer.render(gridInterp());
        assertFalse(result.sql().contains("CIB"),        "raw org value must not appear in SQL");
        assertFalse(result.sql().contains("USD"),        "raw currency value must not appear in SQL");
        assertFalse(result.sql().contains("2026-05-15"), "raw date value must not appear in SQL");
    }

    @Test
    void filterValues_notInSqlText_timeseries() {
        var result = renderer.render(tsInterp());
        assertFalse(result.sql().contains("CIB"),   "raw org value must not appear in SQL");
        assertFalse(result.sql().contains("rates"),
                "raw risk_class value must not appear in SQL (only param name :risk_class should)");
    }

    // ── Security: injection attempt via filter value ───────────────────────────

    @Test
    void injectionAttempt_orgValue_notInSqlText() {
        String injected = "CIB' OR 1=1 --";
        var interp = new DemoSemanticQueryInterpretation(
                "test", GRID_CONTRACT, "delta_exposure",
                List.of("legal_entity"),
                List.of(DemoSemanticFilter.eq("org", injected)),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH,
                List.of());

        var result = renderer.render(interp);

        // SQL contains only the named placeholder
        assertTrue(result.sql().contains(":org"), result.sql());
        // Raw injection string is NOT in SQL text
        assertFalse(result.sql().contains("OR 1=1"), result.sql());
        assertFalse(result.sql().contains("--"),      result.sql());
        // Raw value IS in params map (where it will be safely bound by JDBC)
        assertEquals(injected, result.params().get("org"));
    }

    @Test
    void injectionAttempt_measureNotSubstituted() {
        // measure comes from the allowlist check — a bad measure name is rejected before SQL
        assertThrows(DemoSemanticSqlRenderException.class, () ->
                renderer.render(interpWithMeasure("delta_exposure; DROP TABLE users; --")));
    }

    // ── 15. Renderer requires no external services ─────────────────────────────

    @Test
    void renderer_createsWithDefaults_noExternalDeps() {
        // createWithDefaults() uses real clock, but render is still pure string manipulation
        assertDoesNotThrow(() -> WhitelistedDemoSemanticSqlRenderer.createWithDefaults());
    }

    @Test
    void renderer_isInstanceOfInterface() {
        assertInstanceOf(DemoSemanticSqlRenderer.class, renderer);
    }

    // ── Param map is unmodifiable ──────────────────────────────────────────────

    @Test
    void params_areUnmodifiable() {
        var result = renderer.render(gridInterp());
        assertThrows(UnsupportedOperationException.class,
                () -> result.params().put("extra", "val"));
    }

    @Test
    void groupBy_isUnmodifiable() {
        var result = renderer.render(gridInterp());
        assertThrows(UnsupportedOperationException.class,
                () -> result.groupBy().add("extra"));
    }

    // ── Null guard ─────────────────────────────────────────────────────────────

    @Test
    void nullInterpretation_throws() {
        assertThrows(NullPointerException.class, () -> renderer.render(null));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Standard exposure-grid interpretation matching the issue's required render example. */
    private static DemoSemanticQueryInterpretation gridInterp() {
        return new DemoSemanticQueryInterpretation(
                "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15",
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

    /** Standard time-series interpretation matching the issue's required render example. */
    private static DemoSemanticQueryInterpretation tsInterp() {
        return new DemoSemanticQueryInterpretation(
                "Show the last 30 days of rates DV01 by desk for CIB",
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

    private static DemoSemanticQueryInterpretation interpWithContract(String contractId) {
        return new DemoSemanticQueryInterpretation(
                "test", contractId, "delta_exposure",
                List.of("desk"), List.of(DemoSemanticFilter.eq("org", "CIB")),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH, List.of());
    }

    private static DemoSemanticQueryInterpretation interpWithMeasure(String measure) {
        return new DemoSemanticQueryInterpretation(
                "test", GRID_CONTRACT, measure,
                List.of("desk"), List.of(DemoSemanticFilter.eq("org", "CIB")),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH, List.of());
    }

    private static DemoSemanticQueryInterpretation interpWithGroupBy(List<String> groupBy) {
        return new DemoSemanticQueryInterpretation(
                "test", GRID_CONTRACT, "delta_exposure",
                groupBy, List.of(DemoSemanticFilter.eq("org", "CIB")),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH, List.of());
    }

    private static DemoSemanticQueryInterpretation interpWithExtraFilter(String name, String value) {
        return new DemoSemanticQueryInterpretation(
                "test", GRID_CONTRACT, "delta_exposure",
                List.of("desk"),
                List.of(DemoSemanticFilter.eq("org", "CIB"),
                        DemoSemanticFilter.eq(name, value)),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH, List.of());
    }

    private static DemoSemanticQueryInterpretation unsupportedInterp() {
        return new DemoSemanticQueryInterpretation(
                "Show me everything", null, null,
                List.of(), List.of(), DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.UNSUPPORTED,
                List.of("Unsupported request"));
    }

    private static DemoSemanticQueryInterpretation ambiguousInterp() {
        return new DemoSemanticQueryInterpretation(
                "Show exposure", null, null,
                List.of(), List.of(), DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.AMBIGUOUS,
                List.of("Cannot determine measure"));
    }
}
