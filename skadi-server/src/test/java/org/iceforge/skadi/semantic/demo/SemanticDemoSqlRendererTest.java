package org.iceforge.skadi.semantic.demo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SemanticDemoSqlRenderer}.
 *
 * <p>No Spring context, no JDBC, no network. Tests the allowlist rules, column mapping,
 * SQL structure, and parameterisation safety.
 */
class SemanticDemoSqlRendererTest {

    private static final String TEST_VIEW = "demo.test.v_market_risk";
    private static final int    MAX_ROWS  = 50;

    private SemanticDemoSqlRenderer renderer;

    @BeforeEach
    void setUp() {
        renderer = SemanticDemoSqlRenderer.create(TEST_VIEW, MAX_ROWS);
    }

    // ── Measure allowlist ──────────────────────────────────────────────────────

    @Test
    void knownMeasure_var_accepted() {
        var rendered = renderer.render(req("var", List.of("legal_entity"), Map.of()));
        assertNotNull(rendered);
        assertTrue(rendered.execSql().contains("var_amount"),
                "var must map to physical column var_amount");
    }

    @Test
    void knownMeasure_expectedShortfall_accepted() {
        var rendered = renderer.render(req("expected_shortfall", List.of("legal_entity"), Map.of()));
        assertTrue(rendered.execSql().contains("es_amount"),
                "expected_shortfall must map to es_amount");
    }

    @Test
    void knownMeasure_sensitivity_accepted() {
        var rendered = renderer.render(req("sensitivity", List.of("desk"), Map.of()));
        assertTrue(rendered.execSql().contains("sensitivity_amount"),
                "sensitivity must map to sensitivity_amount");
    }

    @Test
    void unknownMeasure_throws() {
        assertThrows(SemanticDemoSqlRenderException.class,
                () -> renderer.render(req("delta_exposure", List.of("legal_entity"), Map.of())),
                "delta_exposure is not on the allowlist for this renderer");
    }

    @Test
    void unknownMeasure_dv01_throws() {
        assertThrows(SemanticDemoSqlRenderException.class,
                () -> renderer.render(req("dv01", List.of("legal_entity"), Map.of())));
    }

    // ── Dimension allowlist ────────────────────────────────────────────────────

    @Test
    void knownDimension_legalEntity_accepted() {
        assertDoesNotThrow(() -> renderer.render(req("var", List.of("legal_entity"), Map.of())));
    }

    @Test
    void knownDimension_lob_accepted() {
        assertDoesNotThrow(() -> renderer.render(req("var", List.of("lob"), Map.of())));
    }

    @Test
    void knownDimension_desk_accepted() {
        assertDoesNotThrow(() -> renderer.render(req("var", List.of("desk"), Map.of())));
    }

    @Test
    void knownDimension_riskClass_accepted() {
        assertDoesNotThrow(() -> renderer.render(req("var", List.of("risk_class"), Map.of())));
    }

    @Test
    void knownDimension_cobDate_accepted() {
        assertDoesNotThrow(() -> renderer.render(req("var", List.of("cob_date"), Map.of())));
    }

    @Test
    void unknownDimension_businessUnit_throws() {
        assertThrows(SemanticDemoSqlRenderException.class,
                () -> renderer.render(req("var", List.of("business_unit"), Map.of())),
                "business_unit is not on the allowed dimension list");
    }

    @Test
    void unknownDimension_currency_throws() {
        assertThrows(SemanticDemoSqlRenderException.class,
                () -> renderer.render(req("var", List.of("currency"), Map.of())));
    }

    // ── Filter key allowlist ───────────────────────────────────────────────────

    @Test
    void knownFilterKey_lob_accepted() {
        assertDoesNotThrow(() -> renderer.render(
                req("var", List.of("legal_entity"), Map.of("lob", "CIB"))));
    }

    @Test
    void unknownFilterKey_org_throws() {
        assertThrows(SemanticDemoSqlRenderException.class,
                () -> renderer.render(req("var", List.of("legal_entity"), Map.of("org", "CIB"))),
                "'org' is not on the allowed filter key list");
    }

    @Test
    void unknownFilterKey_currency_throws() {
        assertThrows(SemanticDemoSqlRenderException.class,
                () -> renderer.render(req("var", List.of("legal_entity"), Map.of("currency", "USD"))));
    }

    // ── Contract allowlist ─────────────────────────────────────────────────────

    @Test
    void unknownContract_throws() {
        var req = new SemanticDemoRequest(
                "test", "other-contract", "var", List.of("legal_entity"), Map.of());
        assertThrows(SemanticDemoSqlRenderException.class, () -> renderer.render(req));
    }

    // ── SQL structure ──────────────────────────────────────────────────────────

    @Test
    void generatedSql_includesLimit() {
        var rendered = renderer.render(req("var", List.of("legal_entity"), Map.of()));
        assertTrue(rendered.execSql().contains("limit " + MAX_ROWS),
                "generated SQL must contain limit " + MAX_ROWS + ": " + rendered.execSql());
    }

    @Test
    void generatedSql_usesConfiguredViewName() {
        var rendered = renderer.render(req("var", List.of("legal_entity"), Map.of()));
        assertTrue(rendered.execSql().contains(TEST_VIEW),
                "generated SQL must reference the configured view: " + rendered.execSql());
    }

    @Test
    void generatedSql_containsSumAggregate() {
        var rendered = renderer.render(req("var", List.of("legal_entity"), Map.of()));
        assertTrue(rendered.execSql().contains("sum(var_amount)"),
                "generated SQL must use SUM aggregate: " + rendered.execSql());
    }

    @Test
    void generatedSql_groupByPresent() {
        var rendered = renderer.render(req("var", List.of("legal_entity", "desk"), Map.of()));
        assertTrue(rendered.execSql().contains("group by legal_entity, desk"),
                "generated SQL must contain GROUP BY: " + rendered.execSql());
    }

    // ── Filter parameterisation ────────────────────────────────────────────────

    @Test
    void filterValues_notInterpolatedIntoExecSql() {
        String injectionAttempt = "CIB'; DROP TABLE market_risk; --";
        var rendered = renderer.render(
                req("var", List.of("legal_entity"), Map.of("lob", injectionAttempt)));
        assertFalse(rendered.execSql().contains("DROP"),
                "injection attempt must not appear in execSql: " + rendered.execSql());
        assertTrue(rendered.execSql().contains("?"),
                "execSql must use ? placeholder for filter value");
    }

    @Test
    void filterValues_appearsInParamValues() {
        var rendered = renderer.render(
                req("var", List.of("legal_entity"), Map.of("lob", "CIB")));
        assertTrue(rendered.paramValues().contains("CIB"),
                "filter value must appear in paramValues");
        assertFalse(rendered.execSql().contains("CIB"),
                "filter value must NOT appear in execSql");
    }

    @Test
    void filterValues_quotedInDisplaySql() {
        var rendered = renderer.render(
                req("var", List.of("legal_entity"), Map.of("lob", "CIB")));
        assertTrue(rendered.displaySql().contains("'CIB'"),
                "displaySql must contain quoted filter value: " + rendered.displaySql());
    }

    // ── Columns ────────────────────────────────────────────────────────────────

    @Test
    void columns_includesGroupByAndMeasureCol() {
        var rendered = renderer.render(req("var", List.of("legal_entity", "lob"), Map.of()));
        assertTrue(rendered.columns().contains("legal_entity"));
        assertTrue(rendered.columns().contains("lob"));
        assertTrue(rendered.columns().contains("var_amount"));
    }

    @Test
    void columns_measureColIsPhysicalName() {
        var rendered = renderer.render(req("expected_shortfall", List.of("desk"), Map.of()));
        assertTrue(rendered.columns().contains("es_amount"),
                "physical column es_amount must be in columns for expected_shortfall");
        assertFalse(rendered.columns().contains("expected_shortfall"),
                "semantic name must not appear in columns");
    }

    // ── Explanation ────────────────────────────────────────────────────────────

    @Test
    void explanation_containsMeasureAndContract() {
        var rendered = renderer.render(req("var", List.of("legal_entity"), Map.of()));
        assertNotNull(rendered.explanation());
        assertTrue(rendered.explanation().contains("var"),        rendered.explanation());
        assertTrue(rendered.explanation().contains("market-risk-demo"), rendered.explanation());
    }

    // ── Factory guard ──────────────────────────────────────────────────────────

    @Test
    void create_nullViewName_throws() {
        assertThrows(NullPointerException.class,
                () -> SemanticDemoSqlRenderer.create(null, 100));
    }

    @Test
    void create_blankViewName_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> SemanticDemoSqlRenderer.create("  ", 100));
    }

    @Test
    void create_zeroMaxRows_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> SemanticDemoSqlRenderer.create(TEST_VIEW, 0));
    }

    @Test
    void render_nullRequest_throws() {
        assertThrows(NullPointerException.class, () -> renderer.render(null));
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static SemanticDemoRequest req(String measure, List<String> groupBy,
                                           Map<String, String> filters) {
        return new SemanticDemoRequest("test question", "market-risk-demo", measure, groupBy, filters);
    }
}
