package org.iceforge.skadi.semantic.query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JSON serialization/deserialization tests for query contract and output-shape records.
 * No Spring context, no external services, no YAML loading, no JSON Schema validation.
 */
class SemanticQueryContractJsonTest {

    private ObjectMapper mapper;

    @BeforeEach
    void setUp() {
        // Same config as C2 JSON tests: AUTO_DETECT_IS_GETTERS disabled so that
        // helper methods like SemanticOutputShape.columnCount() are not serialised.
        mapper = new ObjectMapper()
                .disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    static SemanticOutputColumn pnlColumn() {
        return new SemanticOutputColumn(
                "pnl", "Total PnL", SemanticFieldType.DECIMAL, true,
                SemanticOutputRole.MEASURE,
                new SemanticFormatHint("#,##0.00", "GBP", "GBP", 2, 2),
                List.of(new SemanticReference("bcbs239", "data-element", "BCBS239-PNL-001", "PnL")));
    }

    static SemanticOutputColumn bookColumn() {
        return new SemanticOutputColumn(
                "book", "Trading Book", SemanticFieldType.STRING, false,
                SemanticOutputRole.DIMENSION, null,
                List.of(new SemanticReference("databricks", "column", "main.risk.gold_risk.book", "book")));
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
                null,
                List.of(new SemanticReference("lineage", "query", "mxl_risk_pnl_by_book_v1", "PnL by Book")));
    }

    static SemanticQueryContract contract() {
        return new SemanticQueryContract(
                "mxl_risk_pnl_by_book",
                "Daily PnL aggregated by trading book",
                "mxl_risk",
                new SemanticContractVersion("1.0.0"),
                shape(),
                List.of());
    }

    // ── SemanticReference ─────────────────────────────────────────────────────

    @Test
    void roundTrip_SemanticReference() throws Exception {
        var r  = new SemanticReference("databricks", "column", "main.risk.t.col", "col");
        var r2 = mapper.readValue(mapper.writeValueAsString(r), SemanticReference.class);
        assertEquals(r, r2);
    }

    @Test
    void roundTrip_SemanticReference_emptyName() throws Exception {
        var r  = new SemanticReference("src", "type", "id", "");
        var r2 = mapper.readValue(mapper.writeValueAsString(r), SemanticReference.class);
        assertEquals("", r2.name());
    }

    // ── SemanticFormatHint ────────────────────────────────────────────────────

    @Test
    void roundTrip_SemanticFormatHint_allFields() throws Exception {
        var h  = new SemanticFormatHint("#,##0.00", "GBP", "GBP", 2, 2);
        var h2 = mapper.readValue(mapper.writeValueAsString(h), SemanticFormatHint.class);
        assertEquals(h, h2);
        assertEquals(2, h2.precision());
    }

    @Test
    void roundTrip_SemanticFormatHint_allNulls() throws Exception {
        var h  = new SemanticFormatHint(null, null, null, null, null);
        var h2 = mapper.readValue(mapper.writeValueAsString(h), SemanticFormatHint.class);
        assertNull(h2.pattern());
        assertNull(h2.precision());
    }

    // ── SemanticOutputColumn ──────────────────────────────────────────────────

    @Test
    void roundTrip_outputColumn_withFormatHint() throws Exception {
        var c  = pnlColumn();
        var c2 = mapper.readValue(mapper.writeValueAsString(c), SemanticOutputColumn.class);
        assertEquals(c.name(),      c2.name());
        assertEquals(c.fieldType(), c2.fieldType());
        assertEquals(c.role(),      c2.role());
        assertTrue(c2.nullable());
        assertNotNull(c2.formatHint());
        assertEquals("GBP", c2.formatHint().currency());
    }

    @Test
    void roundTrip_outputColumn_nullFormatHint() throws Exception {
        var c  = bookColumn();
        var c2 = mapper.readValue(mapper.writeValueAsString(c), SemanticOutputColumn.class);
        assertEquals(SemanticOutputRole.DIMENSION, c2.role());
        assertNull(c2.formatHint());
        assertEquals(1, c2.references().size());
        assertEquals("databricks", c2.references().get(0).source());
    }

    @Test
    void roundTrip_allOutputRoles() throws Exception {
        for (var role : SemanticOutputRole.values()) {
            var c  = new SemanticOutputColumn("c", "c", SemanticFieldType.STRING, false,
                    role, null, List.of());
            var c2 = mapper.readValue(mapper.writeValueAsString(c), SemanticOutputColumn.class);
            assertEquals(role, c2.role());
        }
    }

    @Test
    void deserialized_outputColumn_references_areUnmodifiable() throws Exception {
        var c  = pnlColumn();
        var c2 = mapper.readValue(mapper.writeValueAsString(c), SemanticOutputColumn.class);
        assertThrows(UnsupportedOperationException.class,
                () -> c2.references().add(new SemanticReference("s", "t", "i", "")));
    }

    // ── SemanticOutputShape ───────────────────────────────────────────────────

    @Test
    void roundTrip_SemanticOutputShape() throws Exception {
        var s  = shape();
        var s2 = mapper.readValue(mapper.writeValueAsString(s), SemanticOutputShape.class);
        assertEquals(3, s2.columnCount());
        assertNull(s2.rowCountHint());
        assertEquals(1, s2.references().size());
    }

    @Test
    void roundTrip_outputShape_withRowCountHint() throws Exception {
        var s  = new SemanticOutputShape(List.of(pnlColumn()), 500L, List.of());
        var s2 = mapper.readValue(mapper.writeValueAsString(s), SemanticOutputShape.class);
        assertEquals(500L, s2.rowCountHint());
    }

    @Test
    void roundTrip_outputShape_columnOrder_preserved() throws Exception {
        var s  = shape();
        var s2 = mapper.readValue(mapper.writeValueAsString(s), SemanticOutputShape.class);
        var names = s2.columns().stream().map(SemanticOutputColumn::name).toList();
        assertEquals(List.of("cob_date", "book", "pnl"), names);
    }

    @Test
    void deserialized_outputShape_columns_areUnmodifiable() throws Exception {
        var s  = shape();
        var s2 = mapper.readValue(mapper.writeValueAsString(s), SemanticOutputShape.class);
        assertThrows(UnsupportedOperationException.class,
                () -> s2.columns().add(bookColumn()));
    }

    // ── SemanticQueryContract ─────────────────────────────────────────────────

    @Test
    void roundTrip_SemanticQueryContract() throws Exception {
        var qc  = contract();
        var qc2 = mapper.readValue(mapper.writeValueAsString(qc), SemanticQueryContract.class);
        assertEquals(qc.name(),           qc2.name());
        assertEquals(qc.sourceContract(), qc2.sourceContract());
        assertEquals("1.0.0",             qc2.version().value());
        assertEquals(3,                   qc2.outputShape().columnCount());
    }

    @Test
    void roundTrip_queryContract_rejectsNullName() {
        assertThrows(NullPointerException.class,
                () -> new SemanticQueryContract(null, "", "src", new SemanticContractVersion("1.0.0"),
                        new SemanticOutputShape(List.of()), List.of()));
    }

    @Test
    void roundTrip_queryContract_rejectsBlankSourceContract() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticQueryContract("n", "", " ", new SemanticContractVersion("1.0.0"),
                        new SemanticOutputShape(List.of()), List.of()));
    }

    @Test
    void serialization_isDeterministic() throws Exception {
        var json1 = mapper.writeValueAsString(contract());
        var json2 = mapper.writeValueAsString(contract());
        assertEquals(json1, json2);
    }

    @Test
    void deserialized_queryContract_references_areUnmodifiable() throws Exception {
        var qc  = contract();
        var qc2 = mapper.readValue(mapper.writeValueAsString(qc), SemanticQueryContract.class);
        assertThrows(UnsupportedOperationException.class,
                () -> qc2.references().add(
                        new SemanticReference("s", "t", "i", "")));
    }

    // ── Fixture file tests ────────────────────────────────────────────────────

    @Test
    void fixture_loadsAndDeserializesCorrectly() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-query-contract.json")) {
            assertNotNull(in, "fixture file not found on classpath");
            var qc = mapper.readValue(in, SemanticQueryContract.class);
            assertEquals("mxl_risk_pnl_by_book", qc.name());
            assertEquals("mxl_risk",              qc.sourceContract());
            assertEquals("1.0.0",                 qc.version().value());
            assertEquals(3,                       qc.outputShape().columnCount());
        }
    }

    @Test
    void fixture_pnlColumn_hasMeasureRoleAndFormatHint() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-query-contract.json")) {
            var qc  = mapper.readValue(in, SemanticQueryContract.class);
            var pnl = qc.outputShape().findColumn("pnl").orElseThrow();
            assertEquals(SemanticOutputRole.MEASURE,    pnl.role());
            assertTrue(pnl.nullable());
            assertNotNull(pnl.formatHint());
            assertEquals("GBP",                         pnl.formatHint().currency());
        }
    }

    @Test
    void fixture_bookColumn_hasNullFormatHint() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-query-contract.json")) {
            var qc   = mapper.readValue(in, SemanticQueryContract.class);
            var book = qc.outputShape().findColumn("book").orElseThrow();
            assertNull(book.formatHint());
            assertEquals(SemanticOutputRole.DIMENSION, book.role());
            assertFalse(book.nullable());
            assertEquals(1, book.references().size());
            assertEquals("databricks", book.references().get(0).source());
        }
    }

    @Test
    void fixture_shapeHasLineageReference() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-query-contract.json")) {
            var qc  = mapper.readValue(in, SemanticQueryContract.class);
            var ref = qc.outputShape().references();
            assertEquals(1, ref.size());
            assertEquals("lineage", ref.get(0).source());
        }
    }

    @Test
    void fixture_roundTripsToEquivalentJson() throws Exception {
        try (InputStream in = getClass().getResourceAsStream(
                "/fixtures/sample-query-contract.json")) {
            var qc         = mapper.readValue(in, SemanticQueryContract.class);
            var serialized = mapper.writeValueAsString(qc);
            JsonNode fromFixture    = mapper.readTree(
                    getClass().getResourceAsStream("/fixtures/sample-query-contract.json"));
            JsonNode fromSerialized = mapper.readTree(serialized);
            assertEquals(fromFixture, fromSerialized);
        }
    }
}
