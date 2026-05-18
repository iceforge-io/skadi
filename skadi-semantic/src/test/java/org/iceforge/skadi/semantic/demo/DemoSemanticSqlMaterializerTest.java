package org.iceforge.skadi.semantic.demo;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link DemoSemanticSqlMaterializer}.
 *
 * <p>No Spring context, no Databricks, no S3, no network.
 */
class DemoSemanticSqlMaterializerTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(
            LocalDate.of(2026, 5, 18).atStartOfDay(ZoneOffset.UTC).toInstant(),
            ZoneOffset.UTC);

    private static final String GRID_CONTRACT = "risk_sensitivity_exposure_grid";
    private static final String TS_CONTRACT   = "risk_sensitivity_historical_timeseries";

    // ── 1. Exposure-grid: no :params remain after materialization ─────────────

    @Test
    void exposureGrid_materializedSql_noOrgPlaceholder() {
        var sql = materializeGrid();
        assertFalse(sql.contains(":org"),      "materialized SQL must not contain :org");
    }

    @Test
    void exposureGrid_materializedSql_noCurrencyPlaceholder() {
        var sql = materializeGrid();
        assertFalse(sql.contains(":currency"), "materialized SQL must not contain :currency");
    }

    @Test
    void exposureGrid_materializedSql_noCobDatePlaceholder() {
        var sql = materializeGrid();
        assertFalse(sql.contains(":cob_date"), "materialized SQL must not contain :cob_date");
    }

    @Test
    void exposureGrid_materializedSql_noLimitPlaceholder() {
        var sql = materializeGrid();
        assertFalse(sql.contains(":limit"),    "materialized SQL must not contain :limit");
    }

    // ── 2. Time-series: no :params remain after materialization ───────────────

    @Test
    void timeseries_materializedSql_noStartDatePlaceholder() {
        var sql = materializeTimeseries();
        assertFalse(sql.contains(":start_date"), sql);
    }

    @Test
    void timeseries_materializedSql_noEndDatePlaceholder() {
        var sql = materializeTimeseries();
        assertFalse(sql.contains(":end_date"), sql);
    }

    @Test
    void timeseries_materializedSql_noOrgPlaceholder() {
        var sql = materializeTimeseries();
        assertFalse(sql.contains(":org"), sql);
    }

    @Test
    void timeseries_materializedSql_noRiskClassPlaceholder() {
        var sql = materializeTimeseries();
        assertFalse(sql.contains(":risk_class"), sql);
    }

    @Test
    void timeseries_materializedSql_noLimitPlaceholder() {
        var sql = materializeTimeseries();
        assertFalse(sql.contains(":limit"), sql);
    }

    // ── 3. Expected safe literals in materialized exposure-grid SQL ───────────

    @Test
    void exposureGrid_materializedSql_containsCibLiteral() {
        var sql = materializeGrid();
        assertTrue(sql.contains("'CIB'"), "org=CIB must appear as quoted literal: " + sql);
    }

    @Test
    void exposureGrid_materializedSql_containsUsdLiteral() {
        var sql = materializeGrid();
        assertTrue(sql.contains("'USD'"), "currency=USD must appear as quoted literal: " + sql);
    }

    @Test
    void exposureGrid_materializedSql_containsDateLiteral() {
        var sql = materializeGrid();
        assertTrue(sql.contains("'2026-05-15'"), "cob_date must appear as quoted date: " + sql);
    }

    @Test
    void exposureGrid_materializedSql_containsNumericLimit() {
        var sql = materializeGrid();
        assertTrue(sql.contains("limit 500"), "limit must appear as bare integer: " + sql);
    }

    // ── 4. Injection: single-quote in string value is doubled ─────────────────

    @Test
    void injection_singleQuoteInOrgValue_isDoubled() {
        var rendered = renderedWithParams(
                "select * from v where org = :org",
                Map.of("org", "CIB' OR 1=1 --"));
        var sql = DemoSemanticSqlMaterializer.materialize(rendered);

        // After quote-doubling the single quote inside the value becomes '' within the string.
        // The SQL parser sees one string literal containing "CIB' OR 1=1 --", not two strings
        // with an OR clause between them.
        assertTrue(sql.contains("'CIB'' OR 1=1 --'"), "injection value must be double-quoted: " + sql);
        // The premature string-close form 'CIB' (which would let OR 1=1 escape into SQL)
        // must not appear — the doubled '' prevents the string from ending early.
        assertFalse(sql.contains("= 'CIB' OR"),
                "unescaped injection break-out must not appear: " + sql);
    }

    @Test
    void injection_doubleQuoteInString_isHandled() {
        var rendered = renderedWithParams(
                "select * from v where org = :org",
                Map.of("org", "CIB\" test"));
        var sql = DemoSemanticSqlMaterializer.materialize(rendered);
        // Double-quotes are not SQL injection vectors in single-quoted strings
        assertTrue(sql.contains("'CIB\" test'"), sql);
    }

    // ── 5. Unknown placeholder: throws ───────────────────────────────────────

    @Test
    void unknownPlaceholder_throwsMaterializationException() {
        var rendered = renderedWithParams(
                "select * from v where org = :unknown_param",
                Map.of("org", "CIB"));
        assertThrows(DemoSemanticSqlMaterializationException.class,
                () -> DemoSemanticSqlMaterializer.materialize(rendered));
    }

    @Test
    void unknownPlaceholder_exceptionMessageNamesParameter() {
        var rendered = renderedWithParams(
                "select * from v where org = :secret_param",
                Map.of("org", "CIB"));
        var ex = assertThrows(DemoSemanticSqlMaterializationException.class,
                () -> DemoSemanticSqlMaterializer.materialize(rendered));
        assertTrue(ex.getMessage().contains("secret_param"), ex.getMessage());
    }

    // ── 6. Missing param for declared placeholder: throws ─────────────────────

    @Test
    void missingParam_forDeclaredPlaceholder_throws() {
        // SQL has :risk_class but params map only has :org
        var rendered = renderedWithParams(
                "select * from v where org = :org and risk_class = :risk_class",
                Map.of("org", "CIB"));
        assertThrows(DemoSemanticSqlMaterializationException.class,
                () -> DemoSemanticSqlMaterializer.materialize(rendered));
    }

    // ── 7. Deterministic materialization ─────────────────────────────────────

    @Test
    void materialization_isDeterministic() {
        var sql1 = materializeGrid();
        var sql2 = materializeGrid();
        assertEquals(sql1, sql2, "materialization must be deterministic");
    }

    @Test
    void stringValues_alwaysSingleQuoted() {
        var rendered = renderedWithParams(
                ":org and :currency",
                Map.of("org", "CIB", "currency", "USD"));
        var sql = DemoSemanticSqlMaterializer.materialize(rendered);
        assertEquals("'CIB' and 'USD'", sql);
    }

    @Test
    void integerLimit_renderedWithoutQuotes() {
        var rendered = renderedWithParams("limit :limit", Map.of("limit", 500));
        assertEquals("limit 500", DemoSemanticSqlMaterializer.materialize(rendered));
    }

    @Test
    void longValue_renderedWithoutQuotes() {
        var rendered = renderedWithParams("limit :limit", Map.of("limit", 5000L));
        assertEquals("limit 5000", DemoSemanticSqlMaterializer.materialize(rendered));
    }

    @Test
    void localDateValue_renderedWithDatePrefix() {
        var rendered = renderedWithParams(
                "cob_date = :dt",
                Map.of("dt", LocalDate.of(2026, 5, 15)));
        assertEquals("cob_date = DATE '2026-05-15'",
                DemoSemanticSqlMaterializer.materialize(rendered));
    }

    // ── 8. Unsupported param type: throws ─────────────────────────────────────

    @Test
    void unsupportedParamType_boolean_throws() {
        var rendered = renderedWithParams(":flag", Map.of("flag", Boolean.TRUE));
        var ex = assertThrows(DemoSemanticSqlMaterializationException.class,
                () -> DemoSemanticSqlMaterializer.materialize(rendered));
        assertTrue(ex.getMessage().contains("Boolean"), ex.getMessage());
        assertTrue(ex.getMessage().contains("flag"),    ex.getMessage());
    }

    @Test
    void unsupportedParamType_list_throws() {
        // Use a raw map to bypass type safety
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("val", List.of("a", "b"));
        var rendered = renderedWithParams(":val", params);
        assertThrows(DemoSemanticSqlMaterializationException.class,
                () -> DemoSemanticSqlMaterializer.materialize(rendered));
    }

    // ── 9. Full renderer output can be materialized ───────────────────────────

    @Test
    void rendererOutput_gridInterpretation_fullyMaterializes() {
        var rendered = renderGrid();
        assertDoesNotThrow(() -> DemoSemanticSqlMaterializer.materialize(rendered));
    }

    @Test
    void rendererOutput_timeseriesInterpretation_fullyMaterializes() {
        var rendered = renderTimeseries();
        assertDoesNotThrow(() -> DemoSemanticSqlMaterializer.materialize(rendered));
    }

    @Test
    void rendererOutput_noPlaceholdersRemainAfterMaterialization() {
        for (var rendered : List.of(renderGrid(), renderTimeseries())) {
            var materialized = DemoSemanticSqlMaterializer.materialize(rendered);
            assertFalse(materialized.contains(":"),
                    "no colon-param placeholders should remain: " + materialized);
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /** Renders and materializes the exposure-grid example. */
    private static String materializeGrid() {
        return DemoSemanticSqlMaterializer.materialize(renderGrid());
    }

    /** Renders and materializes the time-series example. */
    private static String materializeTimeseries() {
        return DemoSemanticSqlMaterializer.materialize(renderTimeseries());
    }

    private static DemoSemanticRenderedSql renderGrid() {
        var renderer = WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), FIXED_CLOCK);
        return renderer.render(new DemoSemanticQueryInterpretation(
                "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15",
                GRID_CONTRACT, "delta_exposure",
                List.of("legal_entity", "risk_class"),
                List.of(
                        DemoSemanticFilter.eq("org",      "CIB"),
                        DemoSemanticFilter.eq("currency", "USD"),
                        DemoSemanticFilter.eq("cob_date", "2026-05-15")),
                DemoSemanticOutputShape.GRID,
                DemoSemanticInterpretationConfidence.HIGH,
                List.of()));
    }

    private static DemoSemanticRenderedSql renderTimeseries() {
        var renderer = WhitelistedDemoSemanticSqlRenderer.create(DemoViewConfig.defaults(), FIXED_CLOCK);
        return renderer.render(new DemoSemanticQueryInterpretation(
                "Show the last 30 days of rates DV01 by desk for CIB",
                TS_CONTRACT, "dv01",
                List.of("desk"),
                List.of(
                        DemoSemanticFilter.eq("org",        "CIB"),
                        DemoSemanticFilter.eq("risk_class", "rates"),
                        DemoSemanticFilter.eq("date_range", "last_30_days")),
                DemoSemanticOutputShape.TIMESERIES,
                DemoSemanticInterpretationConfidence.HIGH,
                List.of()));
    }

    /** Creates a minimal DemoSemanticRenderedSql with a given SQL string and params map. */
    private static DemoSemanticRenderedSql renderedWithParams(String sql,
                                                               Map<String, Object> params) {
        return new DemoSemanticRenderedSql(
                sql, params, GRID_CONTRACT, DemoSemanticOutputShape.GRID,
                "delta_exposure", List.of("desk"), "demo.view");
    }
}
