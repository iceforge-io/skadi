package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractExplanation;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticDimensionExplanation;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticEntitlementBehavior;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticLineageHook;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.contract.SemanticMeasureExplanation;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link SemanticContractMetadataModels} factory methods.
 *
 * <p>No Spring context, no HTTP, no external services. Tests cover mapping
 * from domain objects to DTOs with and without optional explanation metadata.
 */
class SemanticContractMetadataModelsTest {

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static SemanticEntity entity(String name) {
        return new SemanticEntity(name, "entity description",
                new SemanticEndpoint("main", "risk", name), List.of());
    }

    private static SemanticMeasure measure(String name) {
        return new SemanticMeasure(name, name + " label", "SUM(" + name + ")",
                SemanticFieldType.DECIMAL, name + " description", null);
    }

    private static SemanticMeasure measureWithExplanation(String name) {
        var exp = new SemanticMeasureExplanation(
                "Use for " + name + " reporting.",
                List.of("Caveat for " + name));
        return new SemanticMeasure(name, name + " label", "SUM(" + name + ")",
                SemanticFieldType.DECIMAL, name + " description", exp);
    }

    private static SemanticDimension dimension(String name) {
        return new SemanticDimension(name, name + "_col", SemanticFieldType.STRING,
                name + " label", true, true, null);
    }

    private static SemanticDimension dimensionWithExplanation(String name) {
        var exp = new SemanticDimensionExplanation(
                "Description for " + name,
                List.of("Dimension caveat for " + name));
        return new SemanticDimension(name, name + "_col", SemanticFieldType.STRING,
                name + " label", true, false, exp);
    }

    private static SemanticContract minimalContract() {
        return new SemanticContract(
                "mxl_risk", new SemanticContractVersion("1.0.0"), "MXL risk layer",
                entity("mxl_risk"),
                List.of(measure("pnl"), measure("delta")),
                List.of(dimension("book"), dimension("cob_date")),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.ttl(7200L), null);
    }

    private static SemanticContract enrichedContract() {
        var exp = new SemanticContractExplanation(
                "Risk Analytics",
                "Use for daily VaR reporting.",
                List.of("Data after 8 AM UTC", "FX rates are end-of-day"),
                "One row per legal entity and COB date",
                List.of("var_by_entity", "var_by_risk_class"),
                new SemanticLineageHook("RISK-LIN-01", "BCBS 239 lineage hook"),
                new SemanticEntitlementBehavior("ROW_FILTER", "Filtered by entity entitlement"),
                List.of("cob_date", "legal_entity", "cob_date,legal_entity"));
        return new SemanticContract(
                "cib_var", new SemanticContractVersion("2.0.0"), "CIB VaR layer",
                entity("cib_var"),
                List.of(measureWithExplanation("var_1d"), measureWithExplanation("var_10d")),
                List.of(dimensionWithExplanation("legal_entity"),
                        dimensionWithExplanation("desk")),
                SemanticAccessPolicy.unrestricted(),
                new SemanticCachePolicy(SemanticCacheStrategy.TTL, 14400L, List.of()),
                exp);
    }

    // ── summaryFrom — minimal contract ─────────────────────────────────────────

    @Test
    void summaryFrom_minimal_holdsBasicFields() {
        var summary = SemanticContractMetadataModels.summaryFrom(minimalContract());
        assertEquals("mxl_risk", summary.name());
        assertEquals("1.0.0", summary.version());
        assertEquals("MXL risk layer", summary.description());
        assertEquals(2, summary.measureCount());
        assertEquals(2, summary.dimensionCount());
    }

    @Test
    void summaryFrom_minimal_ownerAndGrainAreNull() {
        var summary = SemanticContractMetadataModels.summaryFrom(minimalContract());
        assertNull(summary.owner(), "no explanation → owner must be null");
        assertNull(summary.grain(), "no explanation → grain must be null");
    }

    // ── summaryFrom — enriched contract ────────────────────────────────────────

    @Test
    void summaryFrom_enriched_includesOwnerAndGrain() {
        var summary = SemanticContractMetadataModels.summaryFrom(enrichedContract());
        assertEquals("cib_var", summary.name());
        assertEquals("Risk Analytics", summary.owner());
        assertEquals("One row per legal entity and COB date", summary.grain());
    }

    @Test
    void summaryFrom_enriched_countsAreCorrect() {
        var summary = SemanticContractMetadataModels.summaryFrom(enrichedContract());
        assertEquals(2, summary.measureCount());
        assertEquals(2, summary.dimensionCount());
    }

    // ── detailFrom — minimal contract ──────────────────────────────────────────

    @Test
    void detailFrom_minimal_holdsBasicAndCacheFields() {
        var detail = SemanticContractMetadataModels.detailFrom(minimalContract());
        assertEquals("mxl_risk", detail.name());
        assertEquals("1.0.0", detail.version());
        assertEquals("TTL", detail.cacheStrategy());
        assertEquals(7200L, detail.ttlSeconds());
        assertEquals(2, detail.measureCount());
        assertEquals(2, detail.dimensionCount());
    }

    @Test
    void detailFrom_minimal_explanationFieldsAreNull() {
        var detail = SemanticContractMetadataModels.detailFrom(minimalContract());
        assertNull(detail.owner());
        assertNull(detail.intendedUsage());
        assertNull(detail.caveats());
        assertNull(detail.grain());
        assertNull(detail.outputShapes());
        assertNull(detail.lineageHook());
        assertNull(detail.entitlementBehavior());
        assertNull(detail.validGroupingPaths());
    }

    @Test
    void detailFrom_minimal_measuresAndDimensionsPresent() {
        var detail = SemanticContractMetadataModels.detailFrom(minimalContract());
        assertEquals(2, detail.measures().size());
        assertEquals(2, detail.dimensions().size());
        assertEquals("pnl", detail.measures().get(0).name());
        assertEquals("book", detail.dimensions().get(0).name());
    }

    @Test
    void detailFrom_noCachePolicy_ttlSecondsIsNull() {
        var c = new SemanticContract(
                "c", new SemanticContractVersion("1.0.0"), "", entity("c"),
                List.of(measure("m")), List.of(dimension("d")),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null);
        var detail = SemanticContractMetadataModels.detailFrom(c);
        assertEquals("NONE", detail.cacheStrategy());
        assertNull(detail.ttlSeconds());
    }

    // ── detailFrom — enriched contract ─────────────────────────────────────────

    @Test
    void detailFrom_enriched_includesAllExplanationFields() {
        var detail = SemanticContractMetadataModels.detailFrom(enrichedContract());
        assertEquals("Risk Analytics", detail.owner());
        assertEquals("Use for daily VaR reporting.", detail.intendedUsage());
        assertEquals(2, detail.caveats().size());
        assertEquals("One row per legal entity and COB date", detail.grain());
        assertEquals(2, detail.outputShapes().size());
        assertTrue(detail.outputShapes().contains("var_by_entity"));
    }

    @Test
    void detailFrom_enriched_lineageHookMapped() {
        var detail = SemanticContractMetadataModels.detailFrom(enrichedContract());
        assertNotNull(detail.lineageHook());
        assertEquals("RISK-LIN-01", detail.lineageHook().hookId());
        assertEquals("BCBS 239 lineage hook", detail.lineageHook().description());
    }

    @Test
    void detailFrom_enriched_entitlementBehaviorMapped() {
        var detail = SemanticContractMetadataModels.detailFrom(enrichedContract());
        assertNotNull(detail.entitlementBehavior());
        assertEquals("ROW_FILTER", detail.entitlementBehavior().mode());
        assertNotNull(detail.entitlementBehavior().description());
    }

    @Test
    void detailFrom_enriched_validGroupingPathsMapped() {
        var detail = SemanticContractMetadataModels.detailFrom(enrichedContract());
        assertNotNull(detail.validGroupingPaths());
        assertEquals(3, detail.validGroupingPaths().size());
        assertTrue(detail.validGroupingPaths().contains("cob_date,legal_entity"));
    }

    @Test
    void detailFrom_enriched_measuresContainExplanation() {
        var detail = SemanticContractMetadataModels.detailFrom(enrichedContract());
        var var1d = detail.measures().stream()
                .filter(m -> m.name().equals("var_1d"))
                .findFirst().orElseThrow();
        assertEquals("Use for var_1d reporting.", var1d.intendedUsage());
        assertEquals(1, var1d.caveats().size());
    }

    @Test
    void detailFrom_enriched_dimensionsContainExplanation() {
        var detail = SemanticContractMetadataModels.detailFrom(enrichedContract());
        var le = detail.dimensions().stream()
                .filter(d -> d.name().equals("legal_entity"))
                .findFirst().orElseThrow();
        assertEquals("Description for legal_entity", le.description());
        assertEquals(1, le.caveats().size());
        assertFalse(le.groupable(), "dimensionWithExplanation sets groupable=false");
    }

    // ── measureDetailFrom ──────────────────────────────────────────────────────

    @Test
    void measureDetailFrom_withoutExplanation_nullExplanationFields() {
        var dto = SemanticContractMetadataModels.measureDetailFrom(measure("pnl"));
        assertEquals("pnl", dto.name());
        assertEquals("pnl label", dto.label());
        assertEquals("pnl description", dto.description());
        assertEquals("DECIMAL", dto.type());
        assertNull(dto.intendedUsage());
        assertNull(dto.caveats());
    }

    @Test
    void measureDetailFrom_withExplanation_includesUsageAndCaveats() {
        var dto = SemanticContractMetadataModels.measureDetailFrom(measureWithExplanation("pnl"));
        assertEquals("Use for pnl reporting.", dto.intendedUsage());
        assertNotNull(dto.caveats());
        assertEquals(1, dto.caveats().size());
        assertTrue(dto.caveats().get(0).contains("pnl"));
    }

    // ── dimensionDetailFrom ────────────────────────────────────────────────────

    @Test
    void dimensionDetailFrom_withoutExplanation_nullExplanationFields() {
        var dto = SemanticContractMetadataModels.dimensionDetailFrom(dimension("book"));
        assertEquals("book", dto.name());
        assertEquals("book_col", dto.column());
        assertEquals("book label", dto.label());
        assertEquals("STRING", dto.type());
        assertTrue(dto.filterable());
        assertTrue(dto.groupable());
        assertNull(dto.description());
        assertNull(dto.caveats());
    }

    @Test
    void dimensionDetailFrom_withExplanation_includesDescriptionAndCaveats() {
        var dto = SemanticContractMetadataModels.dimensionDetailFrom(
                dimensionWithExplanation("desk"));
        assertEquals("Description for desk", dto.description());
        assertEquals(1, dto.caveats().size());
        assertFalse(dto.groupable());
    }

    // ── JSON serialization — null fields omitted ───────────────────────────────

    @Test
    void summaryFrom_minimal_nullFieldsOmittedFromJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var json = mapper.writeValueAsString(
                SemanticContractMetadataModels.summaryFrom(minimalContract()));
        assertFalse(json.contains("owner"), "null owner must be omitted");
        assertFalse(json.contains("grain"), "null grain must be omitted");
        assertTrue(json.contains("measureCount"));
    }

    @Test
    void detailFrom_minimal_nullFieldsOmittedFromJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var json = mapper.writeValueAsString(
                SemanticContractMetadataModels.detailFrom(minimalContract()));
        assertFalse(json.contains("lineageHook"), "null lineageHook must be omitted");
        assertFalse(json.contains("entitlementBehavior"), "null entitlement must be omitted");
        assertFalse(json.contains("validGroupingPaths"), "null paths must be omitted");
        assertTrue(json.contains("cacheStrategy"));
        assertTrue(json.contains("measures"));
    }

    @Test
    void detailFrom_enriched_allFieldsPresentInJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var json = mapper.writeValueAsString(
                SemanticContractMetadataModels.detailFrom(enrichedContract()));
        assertTrue(json.contains("lineageHook"));
        assertTrue(json.contains("entitlementBehavior"));
        assertTrue(json.contains("validGroupingPaths"));
        assertTrue(json.contains("RISK-LIN-01"));
        assertTrue(json.contains("ROW_FILTER"));
    }

    @Test
    void measureDetailFrom_withoutExplanation_nullFieldsOmittedFromJson() throws Exception {
        var mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
        var json = mapper.writeValueAsString(
                SemanticContractMetadataModels.measureDetailFrom(measure("pnl")));
        assertFalse(json.contains("intendedUsage"), "null intendedUsage must be omitted");
        assertFalse(json.contains("caveats"), "null caveats must be omitted");
        assertTrue(json.contains("DECIMAL"));
    }
}
