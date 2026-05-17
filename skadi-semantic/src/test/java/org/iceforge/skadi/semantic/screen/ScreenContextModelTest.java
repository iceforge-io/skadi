package org.iceforge.skadi.semantic.screen;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the screen-context and widget semantic binding model types.
 *
 * <p>No Spring context, no SQL, no external services.
 */
class ScreenContextModelTest {

    // ── helpers ────────────────────────────────────────────────────────────────

    private static ContractBinding binding() {
        return new ContractBinding("cib_var", "2.0.0");
    }

    private static WidgetSemanticBinding widgetBinding(String widgetId) {
        return new WidgetSemanticBinding(
                widgetId,
                binding(),
                List.of(new BoundMeasure("var_1d", "1-Day VaR")),
                List.of(new BoundDimension("legal_entity", "Legal Entity")),
                List.of(new AppliedFilter("cob_date", "equals", "2026-05-17")),
                new UserEntitlementScope("alice", "cib-risk-team"));
    }

    // ── ContractBinding ────────────────────────────────────────────────────────

    @Test
    void contractBinding_holdsFields() {
        var b = new ContractBinding("cib_var", "2.0.0");
        assertEquals("cib_var", b.contractId());
        assertEquals("2.0.0", b.contractVersion());
    }

    @Test
    void contractBinding_rejectsNullContractId() {
        assertThrows(NullPointerException.class, () -> new ContractBinding(null, "1.0.0"));
    }

    @Test
    void contractBinding_rejectsBlankVersion() {
        assertThrows(IllegalArgumentException.class, () -> new ContractBinding("c", "  "));
    }

    // ── BoundMeasure ──────────────────────────────────────────────────────────

    @Test
    void boundMeasure_holdsFields() {
        var m = new BoundMeasure("var_1d", "1-Day VaR");
        assertEquals("var_1d", m.measureId());
        assertEquals("1-Day VaR", m.label());
    }

    @Test
    void boundMeasure_nullLabelAllowed() {
        assertDoesNotThrow(() -> new BoundMeasure("pnl", null));
    }

    @Test
    void boundMeasure_rejectsBlankMeasureId() {
        assertThrows(IllegalArgumentException.class, () -> new BoundMeasure("", "label"));
    }

    // ── BoundDimension ────────────────────────────────────────────────────────

    @Test
    void boundDimension_holdsFields() {
        var d = new BoundDimension("legal_entity", "Legal Entity");
        assertEquals("legal_entity", d.dimensionId());
        assertEquals("Legal Entity", d.label());
    }

    @Test
    void boundDimension_nullLabelAllowed() {
        assertDoesNotThrow(() -> new BoundDimension("book", null));
    }

    @Test
    void boundDimension_rejectsNullDimensionId() {
        assertThrows(NullPointerException.class, () -> new BoundDimension(null, "label"));
    }

    // ── AppliedFilter ─────────────────────────────────────────────────────────

    @Test
    void appliedFilter_holdsFields() {
        var f = new AppliedFilter("cob_date", "equals", "2026-05-17");
        assertEquals("cob_date", f.dimensionId());
        assertEquals("equals", f.operator());
        assertEquals("2026-05-17", f.value());
    }

    @Test
    void appliedFilter_rejectsNullOperator() {
        assertThrows(NullPointerException.class,
                () -> new AppliedFilter("d", null, "v"));
    }

    @Test
    void appliedFilter_rejectsBlankDimensionId() {
        assertThrows(IllegalArgumentException.class,
                () -> new AppliedFilter("  ", "gte", "v"));
    }

    // ── UserEntitlementScope ──────────────────────────────────────────────────

    @Test
    void entitlementScope_holdsFields() {
        var s = new UserEntitlementScope("alice", "cib-risk-team");
        assertEquals("alice", s.principalId());
        assertEquals("cib-risk-team", s.entitlementSetId());
    }

    @Test
    void entitlementScope_nullEntitlementSetIdAllowed() {
        var s = new UserEntitlementScope("alice", null);
        assertNull(s.entitlementSetId());
    }

    @Test
    void entitlementScope_rejectsBlankPrincipalId() {
        assertThrows(IllegalArgumentException.class,
                () -> new UserEntitlementScope("  ", null));
    }

    // ── ActiveWidgetContext ────────────────────────────────────────────────────

    @Test
    void activeWidgetContext_holdsFields() {
        var a = new ActiveWidgetContext("widget-pnl", "bar_chart");
        assertEquals("widget-pnl", a.widgetId());
        assertEquals("bar_chart", a.widgetType());
    }

    @Test
    void activeWidgetContext_rejectsBlankWidgetType() {
        assertThrows(IllegalArgumentException.class,
                () -> new ActiveWidgetContext("w", ""));
    }

    // ── WidgetSemanticBinding ─────────────────────────────────────────────────

    @Test
    void widgetBinding_holdsAllFields() {
        var wb = widgetBinding("widget-pnl");
        assertEquals("widget-pnl", wb.widgetId());
        assertEquals("cib_var", wb.contractBinding().contractId());
        assertEquals(1, wb.measures().size());
        assertEquals("var_1d", wb.measures().get(0).measureId());
        assertEquals(1, wb.dimensions().size());
        assertEquals(1, wb.filters().size());
        assertNotNull(wb.userEntitlementScope());
        assertEquals("alice", wb.userEntitlementScope().principalId());
    }

    @Test
    void widgetBinding_nullEntitlementScopeAllowed() {
        var wb = new WidgetSemanticBinding(
                "w", binding(),
                List.of(new BoundMeasure("m", null)),
                List.of(),
                List.of(),
                null);
        assertNull(wb.userEntitlementScope());
    }

    @Test
    void widgetBinding_measures_defensivelyCopied() {
        var mutable = new ArrayList<BoundMeasure>();
        mutable.add(new BoundMeasure("m1", null));
        var wb = new WidgetSemanticBinding("w", binding(), mutable, List.of(), List.of(), null);
        mutable.add(new BoundMeasure("m2", null));
        assertEquals(1, wb.measures().size());
    }

    @Test
    void widgetBinding_filters_unmodifiable() {
        var wb = widgetBinding("w");
        assertThrows(UnsupportedOperationException.class,
                () -> wb.filters().add(new AppliedFilter("d", "eq", "v")));
    }

    @Test
    void widgetBinding_rejectsNullContractBinding() {
        assertThrows(NullPointerException.class,
                () -> new WidgetSemanticBinding("w", null, List.of(), List.of(), List.of(), null));
    }

    // ── DashboardScreenContext ────────────────────────────────────────────────

    @Test
    void screenContext_holdsFields() {
        var ctx = new DashboardScreenContext(
                "dash-risk",
                new ActiveWidgetContext("widget-pnl", "bar_chart"),
                List.of(widgetBinding("widget-pnl")));
        assertEquals("dash-risk", ctx.dashboardId());
        assertNotNull(ctx.activeWidget());
        assertEquals(1, ctx.widgetBindings().size());
    }

    @Test
    void screenContext_nullDashboardIdAllowed() {
        var ctx = new DashboardScreenContext(null, null, List.of());
        assertNull(ctx.dashboardId());
        assertNull(ctx.activeWidget());
        assertTrue(ctx.widgetBindings().isEmpty());
    }

    @Test
    void screenContext_widgetBindings_defensivelyCopied() {
        var mutable = new ArrayList<WidgetSemanticBinding>();
        mutable.add(widgetBinding("w1"));
        var ctx = new DashboardScreenContext(null, null, mutable);
        mutable.add(widgetBinding("w2"));
        assertEquals(1, ctx.widgetBindings().size());
    }

    @Test
    void screenContext_activeWidgetBinding_foundWhenPresent() {
        var wb = widgetBinding("widget-pnl");
        var ctx = new DashboardScreenContext(
                "dash",
                new ActiveWidgetContext("widget-pnl", "bar_chart"),
                List.of(wb));
        var found = ctx.activeWidgetBinding();
        assertTrue(found.isPresent());
        assertEquals("widget-pnl", found.get().widgetId());
    }

    @Test
    void screenContext_activeWidgetBinding_emptyWhenNoActiveWidget() {
        var ctx = new DashboardScreenContext(null, null, List.of(widgetBinding("w")));
        assertTrue(ctx.activeWidgetBinding().isEmpty());
    }

    @Test
    void screenContext_activeWidgetBinding_emptyWhenNoMatchingBinding() {
        var ctx = new DashboardScreenContext(
                null,
                new ActiveWidgetContext("widget-other", "table"),
                List.of(widgetBinding("widget-pnl")));
        assertTrue(ctx.activeWidgetBinding().isEmpty());
    }

    // ── ExplainWidgetBindingRequest ───────────────────────────────────────────

    @Test
    void explainRequest_holdsFields() {
        var ctx = new DashboardScreenContext(null, null, List.of());
        var req = new ExplainWidgetBindingRequest("widget-pnl", ctx);
        assertEquals("widget-pnl", req.widgetId());
        assertSame(ctx, req.screenContext());
    }

    @Test
    void explainRequest_rejectsNullScreenContext() {
        assertThrows(NullPointerException.class,
                () -> new ExplainWidgetBindingRequest("w", null));
    }

    @Test
    void explainRequest_rejectsBlankWidgetId() {
        assertThrows(IllegalArgumentException.class,
                () -> new ExplainWidgetBindingRequest("", new DashboardScreenContext(null, null, List.of())));
    }

    // ── JSON serialization ────────────────────────────────────────────────────

    @Test
    void widgetBinding_nullEntitlementScope_omittedFromJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var wb = new WidgetSemanticBinding(
                "w", binding(),
                List.of(new BoundMeasure("m", null)),
                List.of(), List.of(), null);
        var json = mapper.writeValueAsString(wb);
        assertFalse(json.contains("userEntitlementScope"), "null scope must be omitted");
        assertTrue(json.contains("cib_var"));
    }

    @Test
    void boundMeasure_nullLabel_omittedFromJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var json = mapper.writeValueAsString(new BoundMeasure("pnl", null));
        assertFalse(json.contains("label"), "null label must be omitted");
        assertTrue(json.contains("pnl"));
    }

    @Test
    void screenContext_nullDashboardId_omittedFromJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var ctx = new DashboardScreenContext(null, null, List.of());
        var json = mapper.writeValueAsString(ctx);
        assertFalse(json.contains("dashboardId"), "null dashboardId must be omitted");
        assertFalse(json.contains("activeWidget"), "null activeWidget must be omitted");
    }

    @Test
    void fullRequest_roundTripJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var wb = widgetBinding("widget-pnl");
        var ctx = new DashboardScreenContext(
                "dash-risk",
                new ActiveWidgetContext("widget-pnl", "bar_chart"),
                List.of(wb));
        var req = new ExplainWidgetBindingRequest("widget-pnl", ctx);
        var json = mapper.writeValueAsString(req);
        var restored = mapper.readValue(json, ExplainWidgetBindingRequest.class);
        assertEquals(req.widgetId(), restored.widgetId());
        assertEquals(req.screenContext().dashboardId(), restored.screenContext().dashboardId());
        assertEquals(1, restored.screenContext().widgetBindings().size());
        assertEquals("var_1d", restored.screenContext().widgetBindings()
                .get(0).measures().get(0).measureId());
    }
}
