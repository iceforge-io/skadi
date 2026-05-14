package org.iceforge.skadi.semantic.contract;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic structural tests for core semantic contract records.
 * No JSON serialization — that belongs to C2.5.
 * No Spring context, no external services.
 */
class SemanticContractTest {

    // ── helpers ──────────────────────────────────────────────────────────────

    static SemanticEndpoint endpoint() {
        return new SemanticEndpoint("main", "risk", "gold_risk");
    }

    static SemanticEntity entity() {
        return new SemanticEntity("mxl_risk", "MXL risk gold layer", endpoint(), List.of());
    }

    static SemanticMeasure pnlMeasure() {
        return new SemanticMeasure("pnl", "Total PnL", "SUM(pnl)", SemanticFieldType.DECIMAL, "");
    }

    static SemanticDimension bookDim() {
        return new SemanticDimension("book", "book", SemanticFieldType.STRING, "Trading Book", true, true);
    }

    static SemanticContract contract() {
        return new SemanticContract(
                "mxl_risk",
                new SemanticContractVersion("1.0.0"),
                "MXL risk gold layer",
                entity(),
                List.of(pnlMeasure()),
                List.of(bookDim()),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.ttl(300L));
    }

    // ── SemanticContractVersion ───────────────────────────────────────────────

    @Test
    void version_holdsValue() {
        var v = new SemanticContractVersion("2.1.0");
        assertEquals("2.1.0", v.value());
        assertEquals("2.1.0", v.toString());
    }

    @Test
    void version_rejectsNull() {
        assertThrows(NullPointerException.class, () -> new SemanticContractVersion(null));
    }

    @Test
    void version_rejectsBlank() {
        assertThrows(IllegalArgumentException.class, () -> new SemanticContractVersion("  "));
    }

    // ── SemanticEndpoint ─────────────────────────────────────────────────────

    @Test
    void endpoint_holdsFields() {
        var ep = endpoint();
        assertEquals("main", ep.catalog());
        assertEquals("risk", ep.schema());
        assertEquals("gold_risk", ep.table());
    }

    @Test
    void endpoint_fullyQualified() {
        assertEquals("main.risk.gold_risk", endpoint().fullyQualified());
    }

    @Test
    void endpoint_rejectsBlankCatalog() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticEndpoint("", "risk", "gold_risk"));
    }

    // ── SemanticRuleRef ───────────────────────────────────────────────────────

    @Test
    void ruleRef_holdsFields() {
        var ref = new SemanticRuleRef("DQ-042", "PnL must not be null");
        assertEquals("DQ-042", ref.ruleId());
        assertEquals("PnL must not be null", ref.description());
    }

    @Test
    void ruleRef_rejectsBlankRuleId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticRuleRef("", "desc"));
    }

    @Test
    void ruleRef_allowsEmptyDescription() {
        assertDoesNotThrow(() -> new SemanticRuleRef("DQ-001", ""));
    }

    // ── SemanticMeasure ───────────────────────────────────────────────────────

    @Test
    void measure_holdsFields() {
        var m = pnlMeasure();
        assertEquals("pnl", m.name());
        assertEquals("Total PnL", m.label());
        assertEquals("SUM(pnl)", m.expression());
        assertEquals(SemanticFieldType.DECIMAL, m.type());
    }

    @Test
    void measure_rejectsBlankExpression() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticMeasure("pnl", "PnL", "", SemanticFieldType.DECIMAL, ""));
    }

    @Test
    void measure_rejectsNullType() {
        assertThrows(NullPointerException.class,
                () -> new SemanticMeasure("pnl", "PnL", "SUM(pnl)", null, ""));
    }

    // ── SemanticDimension ─────────────────────────────────────────────────────

    @Test
    void dimension_holdsFields() {
        var d = bookDim();
        assertEquals("book", d.name());
        assertEquals("book", d.column());
        assertEquals(SemanticFieldType.STRING, d.type());
        assertTrue(d.filterable());
        assertTrue(d.groupable());
    }

    @Test
    void dimension_nonFilterableNonGroupable() {
        var d = new SemanticDimension("id", "id", SemanticFieldType.LONG, "ID", false, false);
        assertFalse(d.filterable());
        assertFalse(d.groupable());
    }

    // ── SemanticEntity ────────────────────────────────────────────────────────

    @Test
    void entity_holdsFields() {
        var e = entity();
        assertEquals("mxl_risk", e.name());
        assertEquals(endpoint(), e.endpoint());
        assertTrue(e.ruleRefs().isEmpty());
    }

    @Test
    void entity_ruleRefs_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticRuleRef>();
        mutable.add(new SemanticRuleRef("R1", ""));
        var e = new SemanticEntity("e", "", endpoint(), mutable);
        mutable.add(new SemanticRuleRef("R2", ""));      // mutate original
        assertEquals(1, e.ruleRefs().size());             // entity unaffected
    }

    @Test
    void entity_ruleRefs_listIsUnmodifiable() {
        var e = entity();
        assertThrows(UnsupportedOperationException.class,
                () -> e.ruleRefs().add(new SemanticRuleRef("X", "")));
    }

    // ── SemanticContract ──────────────────────────────────────────────────────

    @Test
    void contract_holdsFields() {
        var c = contract();
        assertEquals("mxl_risk", c.name());
        assertEquals("1.0.0", c.version().value());
        assertEquals(1, c.measures().size());
        assertEquals(1, c.dimensions().size());
    }

    @Test
    void contract_measures_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticMeasure>();
        mutable.add(pnlMeasure());
        var c = new SemanticContract(
                "c", new SemanticContractVersion("1.0.0"), "",
                entity(), mutable, List.of(),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none());
        mutable.add(pnlMeasure());           // mutate original
        assertEquals(1, c.measures().size()); // contract unaffected
    }

    @Test
    void contract_dimensions_listIsUnmodifiable() {
        var c = contract();
        assertThrows(UnsupportedOperationException.class,
                () -> c.dimensions().add(bookDim()));
    }

    @Test
    void contract_rejectsBlankName() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticContract(
                        " ", new SemanticContractVersion("1.0.0"), "",
                        entity(), List.of(), List.of(),
                        SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none()));
    }

    @Test
    void contract_rejectsNullVersion() {
        assertThrows(NullPointerException.class,
                () -> new SemanticContract(
                        "c", null, "", entity(), List.of(), List.of(),
                        SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none()));
    }

    @Test
    void contract_holdsAccessAndCachePolicy() {
        var c = contract();
        assertTrue(c.accessPolicy().isEmpty());
        assertEquals(SemanticCacheStrategy.TTL, c.cachePolicy().strategy());
        assertEquals(300L, c.cachePolicy().ttlSeconds());
    }

    @Test
    void contract_rejectsNullAccessPolicy() {
        assertThrows(NullPointerException.class,
                () -> new SemanticContract(
                        "c", new SemanticContractVersion("1.0.0"), "", entity(),
                        List.of(), List.of(), null, SemanticCachePolicy.none()));
    }

    @Test
    void contract_rejectsNullCachePolicy() {
        assertThrows(NullPointerException.class,
                () -> new SemanticContract(
                        "c", new SemanticContractVersion("1.0.0"), "", entity(),
                        List.of(), List.of(), SemanticAccessPolicy.unrestricted(), null));
    }
}
