package org.iceforge.skadi.semantic.query;

import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Structural, validation, and immutability tests for query output-shape records.
 * No JSON serialization, no Spring context, no external services.
 */
class SemanticOutputShapeTest {

    // ── helpers ───────────────────────────────────────────────────────────────

    static SemanticReference databricksRef(String id) {
        return new SemanticReference("databricks", "column", id, id);
    }

    static SemanticFormatHint decimalHint() {
        return new SemanticFormatHint("#,##0.00", "GBP", "GBP", 2, 2);
    }

    static SemanticOutputColumn pnlColumn() {
        return new SemanticOutputColumn(
                "pnl", "Total PnL", SemanticFieldType.DECIMAL, true,
                SemanticOutputRole.MEASURE, decimalHint(),
                List.of(new SemanticReference("bcbs239", "data-element", "BCBS239-PNL-001", "PnL")));
    }

    static SemanticOutputColumn bookColumn() {
        return new SemanticOutputColumn(
                "book", "Trading Book", SemanticFieldType.STRING, false,
                SemanticOutputRole.DIMENSION, null, List.of());
    }

    static SemanticOutputColumn cobDateColumn() {
        return new SemanticOutputColumn(
                "cob_date", "Close of Business Date", SemanticFieldType.DATE, false,
                SemanticOutputRole.TIMESTAMP,
                new SemanticFormatHint("yyyy-MM-dd", null, null, null, null),
                List.of());
    }

    static SemanticOutputShape shape() {
        return new SemanticOutputShape(
                List.of(cobDateColumn(), bookColumn(), pnlColumn()),
                1000L,
                List.of(new SemanticReference("lineage", "query", "q1", "Q1")));
    }

    // ── SemanticReference ─────────────────────────────────────────────────────

    @Test
    void reference_holdsFields() {
        var r = new SemanticReference("databricks", "column", "main.risk.t.col", "col");
        assertEquals("databricks", r.source());
        assertEquals("column",     r.type());
        assertEquals("main.risk.t.col", r.id());
        assertEquals("col",        r.name());
    }

    @Test
    void reference_allowsEmptyName() {
        assertDoesNotThrow(() -> new SemanticReference("src", "type", "id", ""));
    }

    @Test
    void reference_rejectsBlankSource() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticReference("", "type", "id", "n"));
    }

    @Test
    void reference_rejectsBlankType() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticReference("src", " ", "id", "n"));
    }

    @Test
    void reference_rejectsBlankId() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticReference("src", "type", "", "n"));
    }

    @Test
    void reference_rejectsNullName() {
        assertThrows(NullPointerException.class,
                () -> new SemanticReference("src", "type", "id", null));
    }

    // ── SemanticFormatHint ────────────────────────────────────────────────────

    @Test
    void formatHint_holdsAllFields() {
        var h = new SemanticFormatHint("#,##0.00", "GBP", "GBP", 2, 2);
        assertEquals("#,##0.00", h.pattern());
        assertEquals("GBP",      h.unit());
        assertEquals("GBP",      h.currency());
        assertEquals(2,           h.precision());
        assertEquals(2,           h.scale());
    }

    @Test
    void formatHint_allFieldsNullable() {
        var h = new SemanticFormatHint(null, null, null, null, null);
        assertNull(h.pattern());
        assertNull(h.unit());
        assertNull(h.currency());
        assertNull(h.precision());
        assertNull(h.scale());
    }

    @Test
    void formatHint_datePatternOnly() {
        var h = new SemanticFormatHint("yyyy-MM-dd", null, null, null, null);
        assertEquals("yyyy-MM-dd", h.pattern());
        assertNull(h.unit());
    }

    // ── SemanticOutputColumn ──────────────────────────────────────────────────

    @Test
    void outputColumn_holdsFields() {
        var c = pnlColumn();
        assertEquals("pnl",                      c.name());
        assertEquals("Total PnL",                c.displayName());
        assertEquals(SemanticFieldType.DECIMAL,  c.fieldType());
        assertTrue(c.nullable());
        assertEquals(SemanticOutputRole.MEASURE, c.role());
        assertNotNull(c.formatHint());
        assertEquals(1, c.references().size());
    }

    @Test
    void outputColumn_nullFormatHint_isValid() {
        var c = bookColumn();
        assertNull(c.formatHint());
        assertTrue(c.references().isEmpty());
    }

    @Test
    void outputColumn_rejectsBlankName() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticOutputColumn(
                        "  ", "label", SemanticFieldType.STRING, false,
                        SemanticOutputRole.DIMENSION, null, List.of()));
    }

    @Test
    void outputColumn_rejectsNullFieldType() {
        assertThrows(NullPointerException.class,
                () -> new SemanticOutputColumn(
                        "x", "x", null, false,
                        SemanticOutputRole.DIMENSION, null, List.of()));
    }

    @Test
    void outputColumn_rejectsNullRole() {
        assertThrows(NullPointerException.class,
                () -> new SemanticOutputColumn(
                        "x", "x", SemanticFieldType.STRING, false,
                        null, null, List.of()));
    }

    @Test
    void outputColumn_references_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticReference>();
        mutable.add(databricksRef("id1"));
        var c = new SemanticOutputColumn(
                "x", "x", SemanticFieldType.STRING, false,
                SemanticOutputRole.LABEL, null, mutable);
        mutable.add(databricksRef("id2"));
        assertEquals(1, c.references().size());
    }

    @Test
    void outputColumn_references_listIsUnmodifiable() {
        var c = pnlColumn();
        assertThrows(UnsupportedOperationException.class,
                () -> c.references().add(databricksRef("x")));
    }

    @Test
    void outputColumn_allRolesWork() {
        for (var role : SemanticOutputRole.values()) {
            var c = new SemanticOutputColumn(
                    "c", "c", SemanticFieldType.STRING, false, role, null, List.of());
            assertEquals(role, c.role());
        }
    }

    // ── SemanticOutputShape ───────────────────────────────────────────────────

    @Test
    void shape_holdsFields() {
        var s = shape();
        assertEquals(3,     s.columnCount());
        assertEquals(1000L, s.rowCountHint());
        assertEquals(1,     s.references().size());
    }

    @Test
    void shape_convenienceConstructor_setsNullHintAndEmptyRefs() {
        var s = new SemanticOutputShape(List.of(pnlColumn()));
        assertNull(s.rowCountHint());
        assertTrue(s.references().isEmpty());
        assertEquals(1, s.columnCount());
    }

    @Test
    void shape_findColumn_found() {
        var s = shape();
        var col = s.findColumn("pnl");
        assertTrue(col.isPresent());
        assertEquals(SemanticOutputRole.MEASURE, col.get().role());
    }

    @Test
    void shape_findColumn_notFound() {
        assertEquals(Optional.empty(), shape().findColumn("missing"));
    }

    @Test
    void shape_columns_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticOutputColumn>();
        mutable.add(pnlColumn());
        var s = new SemanticOutputShape(mutable, null, List.of());
        mutable.add(bookColumn());
        assertEquals(1, s.columnCount());
    }

    @Test
    void shape_columns_listIsUnmodifiable() {
        assertThrows(UnsupportedOperationException.class,
                () -> shape().columns().add(bookColumn()));
    }

    @Test
    void shape_rejectsNullColumns() {
        assertThrows(NullPointerException.class,
                () -> new SemanticOutputShape(null, null, List.of()));
    }

    @Test
    void shape_allowsEmptyColumns() {
        assertDoesNotThrow(() -> new SemanticOutputShape(List.of(), null, List.of()));
    }

    @Test
    void shape_nullRowCountHint_isValid() {
        var s = new SemanticOutputShape(List.of(), null, List.of());
        assertNull(s.rowCountHint());
    }
}
