package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.loader.JsonContractLoader;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ADR-012 semantic metadata foundation fitness tests.
 *
 * <p>This class documents and enforces the ADR-012 governance constraints that
 * are verifiable before any buddy-chat runtime exists. Each test is named to make
 * the architectural invariant explicit.
 *
 * <h2>Invariants tested</h2>
 * <ol>
 *   <li>Enriched explanation metadata survives JSON load (E1.1 + E1.2)</li>
 *   <li>Enriched metadata is preserved through the registry boundary (E1.3)</li>
 *   <li>Missing optional metadata is handled safely — no NPE (E1.1)</li>
 *   <li>Measure explanation metadata is accessible from the registry (E1.3)</li>
 *   <li>Dimension explanation metadata is accessible from the registry (E1.3)</li>
 *   <li>Non-groupable dimensions are correctly declared in metadata (E1.1)</li>
 *   <li>The enriched fixture covers all required explanation metadata fields (E1.2)</li>
 *   <li>Minimal contracts are valid with no explanation metadata (backward compat)</li>
 * </ol>
 *
 * <h2>Future guardrails (TODOs)</h2>
 * <ul>
 *   <li>TODO: Add ArchUnit rule preventing code outside the semantic contract model
 *       from defining business measure/dimension labels or descriptions (blocks AI meaning sprawl)</li>
 *   <li>TODO: Add contract linting warning when explanation metadata is absent
 *       (owner, intendedUsage, grain) — implement as {@code ContractValidationSeverity.INFO}
 *       once ADR-012 compliance becomes a governance requirement</li>
 *   <li>DONE (skadi#81): Validation endpoint does not invoke query execution —
 *       covered by {@code SemanticRequestValidationControllerTest#adr012_validate_doesNotDeclareExecutionServiceDependency}</li>
 *   <li>DONE (skadi#81): Non-filterable dimension rejected as filter —
 *       covered by {@code SemanticRequestValidationControllerTest#adr012_dimensionFilter_nonFilterableDimension_returnsDenied}</li>
 * </ul>
 *
 * @see <a href="ai/adr/ADR-012-buddy-chat-semantic-model-interrogation.md">ADR-012</a>
 */
class Adr012FitnessTest {

    // ── 1. Enriched metadata survives JSON load ───────────────────────────────

    @Test
    void adr012_enrichedContractExplanation_survivesJsonLoad() throws Exception {
        var loader = JsonContractLoader.create();
        try (InputStream in = stream("/fixtures/enriched-contract.json")) {
            var c = loader.load(in);
            assertNotNull(c.explanation(),
                    "ADR-012: contract explanation metadata must survive JSON load");
            assertNotNull(c.explanation().owner(),
                    "ADR-012: owner must be present in enriched contract");
            assertNotNull(c.explanation().grain(),
                    "ADR-012: grain must be present in enriched contract");
            assertNotNull(c.explanation().lineageHook(),
                    "ADR-012: lineage hook must survive JSON load");
            assertNotNull(c.explanation().entitlementBehavior(),
                    "ADR-012: entitlement behavior must survive JSON load");
        }
    }

    @Test
    void adr012_measureExplanation_survivesJsonLoad() throws Exception {
        var loader = JsonContractLoader.create();
        try (InputStream in = stream("/fixtures/enriched-contract.json")) {
            var c = loader.load(in);
            c.measures().forEach(m ->
                assertNotNull(m.explanation(),
                        "ADR-012: all measures in enriched contract must carry explanation — failed for: " + m.name()));
        }
    }

    @Test
    void adr012_dimensionExplanation_survivesJsonLoad() throws Exception {
        var loader = JsonContractLoader.create();
        try (InputStream in = stream("/fixtures/enriched-contract.json")) {
            var c = loader.load(in);
            c.dimensions().forEach(d ->
                assertNotNull(d.explanation(),
                        "ADR-012: all dimensions in enriched contract must carry explanation — failed for: " + d.name()));
        }
    }

    // ── 2. Enriched metadata preserved through registry boundary ─────────────

    @Test
    void adr012_enrichedMetadata_survivesRegistryBoundary(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var c = registry.findByName("cib_var").orElseThrow();

        assertNotNull(c.explanation(),
                "ADR-012: explanation metadata must not be stripped at registry boundary");
        assertEquals("CIB Risk Technology", c.explanation().owner());
        assertNotNull(c.explanation().validGroupingPaths());
        assertFalse(c.explanation().validGroupingPaths().isEmpty());
    }

    @Test
    void adr012_measureExplanation_survivesRegistryBoundary(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var measures = registry.findByName("cib_var").orElseThrow().measures();

        measures.forEach(m ->
            assertNotNull(m.explanation(),
                    "ADR-012: measure explanation must survive registry boundary — failed for: " + m.name()));
    }

    @Test
    void adr012_dimensionExplanation_survivesRegistryBoundary(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var dimensions = registry.findByName("cib_var").orElseThrow().dimensions();

        dimensions.forEach(d ->
            assertNotNull(d.explanation(),
                    "ADR-012: dimension explanation must survive registry boundary — failed for: " + d.name()));
    }

    // ── 3. Missing optional metadata is handled safely (no NPE) ──────────────

    @Test
    void adr012_missingExplanation_isNull_notException(@TempDir Path tmp) throws Exception {
        var registry = loadMinimalRegistry(tmp);
        var c = registry.findByName("mxl_risk").orElseThrow();

        assertNull(c.explanation(),
                "ADR-012: minimal contract must have null explanation, not throw");
        // All downstream null checks must not throw
        assertDoesNotThrow(() -> {
            String owner = c.explanation() != null ? c.explanation().owner() : null;
            assertNull(owner);
        });
    }

    @Test
    void adr012_missingMeasureExplanation_isNull_notException(@TempDir Path tmp) throws Exception {
        var registry = loadMinimalRegistry(tmp);
        var measures = registry.findByName("mxl_risk").orElseThrow().measures();

        measures.forEach(m -> {
            assertNull(m.explanation(),
                    "ADR-012: measure in minimal contract must have null explanation — failed for: " + m.name());
            assertDoesNotThrow(() -> {
                String usage = m.explanation() != null ? m.explanation().intendedUsage() : null;
                assertNull(usage);
            });
        });
    }

    @Test
    void adr012_missingDimensionExplanation_isNull_notException(@TempDir Path tmp) throws Exception {
        var registry = loadMinimalRegistry(tmp);
        var dimensions = registry.findByName("mxl_risk").orElseThrow().dimensions();

        dimensions.forEach(d -> {
            assertNull(d.explanation(),
                    "ADR-012: dimension in minimal contract must have null explanation — failed for: " + d.name());
            assertDoesNotThrow(() -> {
                String desc = d.explanation() != null ? d.explanation().description() : null;
                assertNull(desc);
            });
        });
    }

    // ── 4–5. Specific metadata fields accessible ──────────────────────────────

    @Test
    void adr012_lineageHook_accessible_withHookId(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var hook = registry.findByName("cib_var").orElseThrow()
                .explanation().lineageHook();
        assertNotNull(hook);
        assertEquals("RISK-LIN-VAR-01", hook.hookId(),
                "ADR-012: lineage hook ID must be preserved for BCBS 239 compliance tracking");
    }

    @Test
    void adr012_entitlementBehavior_accessible_withMode(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var eb = registry.findByName("cib_var").orElseThrow()
                .explanation().entitlementBehavior();
        assertNotNull(eb);
        assertNotNull(eb.mode(),
                "ADR-012: entitlement behavior mode must be present as a placeholder seam");
    }

    @Test
    void adr012_validGroupingPaths_accessible(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var paths = registry.findByName("cib_var").orElseThrow()
                .explanation().validGroupingPaths();
        assertNotNull(paths);
        assertFalse(paths.isEmpty(),
                "ADR-012: validGroupingPaths must not be empty in enriched contract");
    }

    @Test
    void adr012_outputShapes_accessible(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var shapes = registry.findByName("cib_var").orElseThrow()
                .explanation().outputShapes();
        assertNotNull(shapes);
        assertFalse(shapes.isEmpty(),
                "ADR-012: outputShapes must not be empty in enriched contract");
    }

    // ── 6. Non-groupable dimensions correctly declared ────────────────────────

    @Test
    void adr012_nonGroupableDimension_declaredInContract(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var dimensions = registry.findByName("cib_var").orElseThrow().dimensions();

        var desk = dimensions.stream()
                .filter(d -> d.name().equals("desk"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("desk dimension not found"));

        assertFalse(desk.groupable(),
                "ADR-012: desk dimension must be non-groupable — " +
                "validates that governance metadata correctly restricts regulatory grouping");
        assertTrue(desk.filterable(),
                "ADR-012: desk must still be filterable even when non-groupable");
    }

    @Test
    void adr012_groupableDimension_declaredInContract(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var legalEntity = registry.findByName("cib_var").orElseThrow().dimensions().stream()
                .filter(d -> d.name().equals("legal_entity"))
                .findFirst().orElseThrow();
        assertTrue(legalEntity.groupable(),
                "ADR-012: legal_entity must be groupable for valid regulatory aggregation");
    }

    // ── 7. Enriched fixture covers required ADR-012 fields ───────────────────

    @Test
    void adr012_enrichedFixture_coversAllRequiredExplanationFields() throws Exception {
        var loader = JsonContractLoader.create();
        try (InputStream in = stream("/fixtures/enriched-contract.json")) {
            var c = loader.load(in);
            var exp = c.explanation();
            assertNotNull(exp, "explanation must be present");
            assertNotNull(exp.owner(),              "owner required");
            assertNotNull(exp.intendedUsage(),      "intendedUsage required");
            assertNotNull(exp.caveats(),            "caveats required");
            assertFalse(exp.caveats().isEmpty(),    "caveats must not be empty");
            assertNotNull(exp.grain(),              "grain required");
            assertNotNull(exp.outputShapes(),       "outputShapes required");
            assertFalse(exp.outputShapes().isEmpty(),"outputShapes must not be empty");
            assertNotNull(exp.lineageHook(),        "lineageHook required");
            assertNotNull(exp.lineageHook().hookId(),"lineageHook.hookId required");
            assertNotNull(exp.entitlementBehavior(),"entitlementBehavior required");
            assertNotNull(exp.entitlementBehavior().mode(),"entitlementBehavior.mode required");
            assertNotNull(exp.validGroupingPaths(), "validGroupingPaths required");
            assertFalse(exp.validGroupingPaths().isEmpty(),"validGroupingPaths must not be empty");
        }
    }

    @Test
    void adr012_enrichedFixture_measureExplanation_coversRequiredFields() throws Exception {
        var loader = JsonContractLoader.create();
        try (InputStream in = stream("/fixtures/enriched-contract.json")) {
            var c = loader.load(in);
            c.measures().forEach(m -> {
                assertNotNull(m.explanation(),            "measure explanation required — " + m.name());
                assertNotNull(m.explanation().intendedUsage(), "measure intendedUsage required — " + m.name());
                assertNotNull(m.explanation().caveats(),       "measure caveats required — " + m.name());
            });
        }
    }

    @Test
    void adr012_enrichedFixture_dimensionExplanation_coversRequiredFields() throws Exception {
        var loader = JsonContractLoader.create();
        try (InputStream in = stream("/fixtures/enriched-contract.json")) {
            var c = loader.load(in);
            c.dimensions().forEach(d -> {
                assertNotNull(d.explanation(),              "dimension explanation required — " + d.name());
                assertNotNull(d.explanation().description(), "dimension description required — " + d.name());
            });
        }
    }

    // ── 8. Minimal contracts remain backward compatible ───────────────────────

    @Test
    void adr012_minimalContract_remainsValid(@TempDir Path tmp) throws Exception {
        var registry = loadMinimalRegistry(tmp);
        assertTrue(registry.contains("mxl_risk"),
                "ADR-012: minimal contract without explanation metadata must remain valid");
        var c = registry.findByName("mxl_risk").orElseThrow();
        assertEquals(2, c.measures().size());
        assertEquals(3, c.dimensions().size());
    }

    @Test
    void adr012_minimalContractMeasures_haveNullExplanation(@TempDir Path tmp) throws Exception {
        var registry = loadMinimalRegistry(tmp);
        var c = registry.findByName("mxl_risk").orElseThrow();
        c.measures().forEach(m ->
            assertNull(m.explanation(),
                    "ADR-012: measures in minimal contract must have null explanation (backward compat)"));
    }

    @Test
    void adr012_inMemoryMinimalContract_noExplanationRequired() {
        // Proves SemanticContract can be constructed without explanation (backward compat)
        var entity = new SemanticEntity("test", "",
                new SemanticEndpoint("c", "s", "t"), List.of());
        assertDoesNotThrow(() -> new SemanticContract(
                "test", new SemanticContractVersion("1.0.0"), "", entity,
                List.of(new SemanticMeasure("m", "M", "SUM(m)", SemanticFieldType.DECIMAL, "", null)),
                List.of(new SemanticDimension("d", "d", SemanticFieldType.STRING, "D", true, true, null)),
                SemanticAccessPolicy.unrestricted(), SemanticCachePolicy.none(), null),
                "ADR-012: contract with null explanation must construct without error");
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private ContractRegistry loadEnrichedRegistry(Path tmp) throws Exception {
        Path f = copyFixture(tmp, "enriched-contract.json", "cib_var.json");
        var result = ContractRegistryPopulator.create().populateFromPaths(List.of(f));
        assertTrue(result.isSuccess(), "enriched fixture must pass validation");
        return result.registry();
    }

    private ContractRegistry loadMinimalRegistry(Path tmp) throws Exception {
        Path f = copyFixture(tmp, "sample-contract.json", "mxl_risk.json");
        var result = ContractRegistryPopulator.create().populateFromPaths(List.of(f));
        assertTrue(result.isSuccess(), "minimal fixture must pass validation");
        return result.registry();
    }

    private Path copyFixture(Path tmp, String resource, String destName) throws Exception {
        Path dest = tmp.resolve(destName);
        try (InputStream in = stream("/fixtures/" + resource)) {
            Files.copy(in, dest);
        }
        return dest;
    }

    private InputStream stream(String resource) {
        var in = getClass().getResourceAsStream(resource);
        assertNotNull(in, "fixture not found: " + resource);
        return in;
    }
}
