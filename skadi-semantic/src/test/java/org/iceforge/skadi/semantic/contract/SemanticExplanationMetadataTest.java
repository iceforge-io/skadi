package org.iceforge.skadi.semantic.contract;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the E1.1 explanation metadata model types.
 *
 * <p>Covers construction, field access, defensive copying, JSON round-trips,
 * and backward compatibility with existing contracts that carry no explanation.
 * No Spring context, no external services.
 */
class SemanticExplanationMetadataTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper()
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    }

    // ── SemanticMeasureExplanation ─────────────────────────────────────────────

    @Test
    void measureExplanation_holdsFields() {
        var exp = new SemanticMeasureExplanation(
                "Use for daily PnL reporting.",
                List.of("Includes unrealized positions", "FX rates are end-of-day"));
        assertEquals("Use for daily PnL reporting.", exp.intendedUsage());
        assertEquals(2, exp.caveats().size());
        assertEquals("Includes unrealized positions", exp.caveats().get(0));
    }

    @Test
    void measureExplanation_nullFieldsAllowed() {
        var exp = new SemanticMeasureExplanation(null, null);
        assertNull(exp.intendedUsage());
        assertNull(exp.caveats());
    }

    @Test
    void measureExplanation_caveats_defensivelyCopied() {
        var mutable = new ArrayList<>(List.of("caveat1"));
        var exp = new SemanticMeasureExplanation("usage", mutable);
        mutable.add("caveat2");
        assertEquals(1, exp.caveats().size());
    }

    @Test
    void measureExplanation_caveats_listIsUnmodifiable() {
        var exp = new SemanticMeasureExplanation("usage", List.of("c1"));
        assertThrows(UnsupportedOperationException.class,
                () -> exp.caveats().add("extra"));
    }

    @Test
    void measureExplanation_roundTrip_withExplanation() throws Exception {
        var exp = new SemanticMeasureExplanation(
                "Daily PnL reporting.",
                List.of("Unrealized positions included"));
        var restored = mapper.readValue(mapper.writeValueAsString(exp),
                SemanticMeasureExplanation.class);
        assertEquals(exp.intendedUsage(), restored.intendedUsage());
        assertEquals(exp.caveats(), restored.caveats());
    }

    @Test
    void measureExplanation_roundTrip_nullFieldsOmittedFromJson() throws Exception {
        var exp = new SemanticMeasureExplanation(null, null);
        var json = mapper.writeValueAsString(exp);
        assertFalse(json.contains("intendedUsage"), "null field should be omitted");
        assertFalse(json.contains("caveats"), "null field should be omitted");
    }

    // ── SemanticDimensionExplanation ───────────────────────────────────────────

    @Test
    void dimensionExplanation_holdsFields() {
        var exp = new SemanticDimensionExplanation(
                "Legal entity book code from the risk system.",
                List.of("Changes during reorg periods"));
        assertEquals("Legal entity book code from the risk system.", exp.description());
        assertEquals(1, exp.caveats().size());
    }

    @Test
    void dimensionExplanation_nullFieldsAllowed() {
        var exp = new SemanticDimensionExplanation(null, null);
        assertNull(exp.description());
        assertNull(exp.caveats());
    }

    @Test
    void dimensionExplanation_caveats_defensivelyCopied() {
        var mutable = new ArrayList<>(List.of("c1"));
        var exp = new SemanticDimensionExplanation("desc", mutable);
        mutable.add("c2");
        assertEquals(1, exp.caveats().size());
    }

    @Test
    void dimensionExplanation_roundTrip() throws Exception {
        var exp = new SemanticDimensionExplanation(
                "Trading desk identifier.", List.of("Null for legacy books"));
        var restored = mapper.readValue(mapper.writeValueAsString(exp),
                SemanticDimensionExplanation.class);
        assertEquals(exp.description(), restored.description());
        assertEquals(exp.caveats(), restored.caveats());
    }

    @Test
    void dimensionExplanation_roundTrip_nullFieldsOmittedFromJson() throws Exception {
        var exp = new SemanticDimensionExplanation(null, null);
        var json = mapper.writeValueAsString(exp);
        assertFalse(json.contains("description"), "null field should be omitted");
        assertFalse(json.contains("caveats"), "null field should be omitted");
    }

    // ── SemanticLineageHook ────────────────────────────────────────────────────

    @Test
    void lineageHook_holdsFields() {
        var hook = new SemanticLineageHook("RISK-LIN-01", "BCBS 239 lineage hook");
        assertEquals("RISK-LIN-01", hook.hookId());
        assertEquals("BCBS 239 lineage hook", hook.description());
    }

    @Test
    void lineageHook_nullFieldsAllowed() {
        var hook = new SemanticLineageHook(null, null);
        assertNull(hook.hookId());
        assertNull(hook.description());
    }

    @Test
    void lineageHook_roundTrip() throws Exception {
        var hook = new SemanticLineageHook("H-001", "desc");
        var restored = mapper.readValue(mapper.writeValueAsString(hook),
                SemanticLineageHook.class);
        assertEquals(hook.hookId(), restored.hookId());
        assertEquals(hook.description(), restored.description());
    }

    @Test
    void lineageHook_roundTrip_nullFieldsOmittedFromJson() throws Exception {
        var hook = new SemanticLineageHook(null, null);
        var json = mapper.writeValueAsString(hook);
        assertFalse(json.contains("hookId"), "null field should be omitted");
        assertFalse(json.contains("description"), "null field should be omitted");
    }

    // ── SemanticEntitlementBehavior ────────────────────────────────────────────

    @Test
    void entitlementBehavior_holdsFields() {
        var eb = new SemanticEntitlementBehavior("ROW_FILTER", "Filtered by book entitlement");
        assertEquals("ROW_FILTER", eb.mode());
        assertEquals("Filtered by book entitlement", eb.description());
    }

    @Test
    void entitlementBehavior_nullFieldsAllowed() {
        var eb = new SemanticEntitlementBehavior(null, null);
        assertNull(eb.mode());
        assertNull(eb.description());
    }

    @Test
    void entitlementBehavior_roundTrip() throws Exception {
        var eb = new SemanticEntitlementBehavior("COLUMN_MASK", "PII columns masked");
        var restored = mapper.readValue(mapper.writeValueAsString(eb),
                SemanticEntitlementBehavior.class);
        assertEquals(eb.mode(), restored.mode());
        assertEquals(eb.description(), restored.description());
    }

    // ── SemanticContractExplanation ────────────────────────────────────────────

    @Test
    void contractExplanation_holdsAllFields() {
        var hook = new SemanticLineageHook("H-01", "lineage");
        var eb   = new SemanticEntitlementBehavior("ROW_FILTER", "filtered");
        var exp  = new SemanticContractExplanation(
                "Risk Analytics",
                "Use for daily PnL reporting.",
                List.of("Data available after 7 AM UTC"),
                "One row per book per COB date",
                List.of("pnl_by_book"),
                hook, eb,
                List.of("cob_date", "book"));

        assertEquals("Risk Analytics", exp.owner());
        assertEquals("Use for daily PnL reporting.", exp.intendedUsage());
        assertEquals(1, exp.caveats().size());
        assertEquals("One row per book per COB date", exp.grain());
        assertEquals(1, exp.outputShapes().size());
        assertSame(hook, exp.lineageHook());
        assertSame(eb, exp.entitlementBehavior());
        assertEquals(List.of("cob_date", "book"), exp.validGroupingPaths());
    }

    @Test
    void contractExplanation_allNullFieldsAllowed() {
        var exp = new SemanticContractExplanation(null, null, null, null, null, null, null, null);
        assertNull(exp.owner());
        assertNull(exp.intendedUsage());
        assertNull(exp.caveats());
        assertNull(exp.grain());
        assertNull(exp.outputShapes());
        assertNull(exp.lineageHook());
        assertNull(exp.entitlementBehavior());
        assertNull(exp.validGroupingPaths());
    }

    @Test
    void contractExplanation_lists_defensivelyCopied() {
        var mutableCaveats  = new ArrayList<>(List.of("c1"));
        var mutableShapes   = new ArrayList<>(List.of("s1"));
        var mutablePaths    = new ArrayList<>(List.of("p1"));
        var exp = new SemanticContractExplanation(
                null, null, mutableCaveats, null, mutableShapes, null, null, mutablePaths);

        mutableCaveats.add("c2");
        mutableShapes.add("s2");
        mutablePaths.add("p2");

        assertEquals(1, exp.caveats().size());
        assertEquals(1, exp.outputShapes().size());
        assertEquals(1, exp.validGroupingPaths().size());
    }

    @Test
    void contractExplanation_lists_unmodifiable() {
        var exp = new SemanticContractExplanation(
                null, null, List.of("c1"), null, List.of("s1"), null, null, List.of("p1"));
        assertThrows(UnsupportedOperationException.class, () -> exp.caveats().add("extra"));
        assertThrows(UnsupportedOperationException.class, () -> exp.outputShapes().add("extra"));
        assertThrows(UnsupportedOperationException.class, () -> exp.validGroupingPaths().add("extra"));
    }

    @Test
    void contractExplanation_roundTrip_full() throws Exception {
        var exp = new SemanticContractExplanation(
                "Risk Analytics",
                "Use for daily PnL reporting.",
                List.of("Data available after 7 AM UTC"),
                "One row per book per COB date",
                List.of("pnl_by_book"),
                new SemanticLineageHook("H-01", "lineage"),
                new SemanticEntitlementBehavior("ROW_FILTER", "filtered"),
                List.of("cob_date", "book"));

        var restored = mapper.readValue(mapper.writeValueAsString(exp),
                SemanticContractExplanation.class);

        assertEquals(exp.owner(), restored.owner());
        assertEquals(exp.intendedUsage(), restored.intendedUsage());
        assertEquals(exp.caveats(), restored.caveats());
        assertEquals(exp.grain(), restored.grain());
        assertEquals(exp.outputShapes(), restored.outputShapes());
        assertEquals(exp.lineageHook(), restored.lineageHook());
        assertEquals(exp.entitlementBehavior(), restored.entitlementBehavior());
        assertEquals(exp.validGroupingPaths(), restored.validGroupingPaths());
    }

    @Test
    void contractExplanation_roundTrip_nullFieldsOmittedFromJson() throws Exception {
        var exp = new SemanticContractExplanation(null, null, null, null, null, null, null, null);
        var json = mapper.writeValueAsString(exp);
        assertEquals("{}", json, "all-null explanation should serialize to empty object");
    }

    // ── SemanticContract backward compatibility ────────────────────────────────

    @Test
    void contract_nullExplanation_isAllowed() {
        var entity = new SemanticEntity("c", "", new SemanticEndpoint("a", "b", "c"), List.of());
        var c = new SemanticContract(
                "c", new SemanticContractVersion("1.0.0"), "", entity,
                List.of(), List.of(),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null);
        assertNull(c.explanation());
    }

    @Test
    void contract_withExplanation_roundTrip() throws Exception {
        var entity = new SemanticEntity("c", "", new SemanticEndpoint("a", "b", "c"), List.of());
        var explanation = new SemanticContractExplanation(
                "Team Alpha", "Use for reporting.", List.of("Caveat 1"),
                "One row per day", List.of("shape1"),
                new SemanticLineageHook("H1", "hook"),
                new SemanticEntitlementBehavior("FULL", "full access"),
                List.of("dim1"));
        var c = new SemanticContract(
                "c", new SemanticContractVersion("1.0.0"), "", entity,
                List.of(new SemanticMeasure("m", "M", "SUM(m)", SemanticFieldType.DECIMAL, "", null)),
                List.of(new SemanticDimension("d", "d", SemanticFieldType.STRING, "D", true, true, null)),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), explanation);

        var restored = mapper.readValue(mapper.writeValueAsString(c), SemanticContract.class);
        assertNotNull(restored.explanation());
        assertEquals("Team Alpha", restored.explanation().owner());
        assertEquals("Use for reporting.", restored.explanation().intendedUsage());
    }

    @Test
    void contract_nullExplanation_omittedFromJson() throws Exception {
        var entity = new SemanticEntity("c", "", new SemanticEndpoint("a", "b", "c"), List.of());
        var c = new SemanticContract(
                "c", new SemanticContractVersion("1.0.0"), "", entity,
                List.of(), List.of(),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null);

        var json = mapper.writeValueAsString(c);
        assertFalse(json.contains("explanation"), "null explanation must not appear in JSON");
    }

    // ── SemanticMeasure with explanation ───────────────────────────────────────

    @Test
    void measure_withExplanation_roundTrip() throws Exception {
        var explanation = new SemanticMeasureExplanation(
                "Daily PnL reporting.", List.of("Unrealized included"));
        var m = new SemanticMeasure("pnl", "PnL", "SUM(pnl)", SemanticFieldType.DECIMAL,
                "Sum of PnL", explanation);

        var restored = mapper.readValue(mapper.writeValueAsString(m), SemanticMeasure.class);
        assertNotNull(restored.explanation());
        assertEquals("Daily PnL reporting.", restored.explanation().intendedUsage());
        assertEquals(1, restored.explanation().caveats().size());
    }

    @Test
    void measure_nullExplanation_omittedFromJson() throws Exception {
        var m = new SemanticMeasure("pnl", "PnL", "SUM(pnl)", SemanticFieldType.DECIMAL, "", null);
        var json = mapper.writeValueAsString(m);
        assertFalse(json.contains("explanation"), "null explanation must not appear in JSON");
    }

    // ── SemanticDimension with explanation ─────────────────────────────────────

    @Test
    void dimension_withExplanation_roundTrip() throws Exception {
        var explanation = new SemanticDimensionExplanation(
                "Legal entity book code.", List.of("Changes during reorg"));
        var d = new SemanticDimension("book", "book", SemanticFieldType.STRING,
                "Trading Book", true, true, explanation);

        var restored = mapper.readValue(mapper.writeValueAsString(d), SemanticDimension.class);
        assertNotNull(restored.explanation());
        assertEquals("Legal entity book code.", restored.explanation().description());
    }

    @Test
    void dimension_nullExplanation_omittedFromJson() throws Exception {
        var d = new SemanticDimension("book", "book", SemanticFieldType.STRING,
                "Trading Book", true, true, null);
        var json = mapper.writeValueAsString(d);
        assertFalse(json.contains("explanation"), "null explanation must not appear in JSON");
    }
}
