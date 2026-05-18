package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.loader.ContractLoader;
import org.iceforge.skadi.semantic.loader.JsonContractLoader;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Demo spike fixtures for issue #122 / epic #121.
 *
 * <p>Verifies that the two market-risk sensitivity demo contracts
 * ({@code risk_sensitivity_exposure_grid} and
 * {@code risk_sensitivity_historical_timeseries}) load correctly, that their
 * metadata is preserved, and that existing minimal contract fixtures remain
 * backward-compatible.
 *
 * <p>No Spring context, no Databricks, no S3, no network I/O, no SQL execution,
 * no skadi-sql-gateway changes.
 */
class DemoMarketRiskContractTest {

    private static final String EXPOSURE_GRID     = "/fixtures/demo-contract-risk-sensitivity-exposure-grid.json";
    private static final String HISTORY_TIMESERIES = "/fixtures/demo-contract-risk-sensitivity-historical-timeseries.json";
    private static final String SAMPLE_CONTRACT    = "/fixtures/sample-contract.json";

    private ContractLoader loader;

    @BeforeEach
    void setUp() {
        loader = JsonContractLoader.create();
    }

    // ── Test 1: Existing minimal contract fixture still loads (backward compat) ─

    @Test
    void existingSampleContract_stillLoads_backwardCompatible() throws Exception {
        try (InputStream in = stream(SAMPLE_CONTRACT)) {
            SemanticContract c = loader.load(in);
            assertEquals("mxl_risk", c.name());
            assertEquals("1.0.0",    c.version().value());
            assertEquals(2,          c.measures().size());
            assertEquals(3,          c.dimensions().size());
        }
    }

    // ── Test 2: Exposure-grid demo contract loads ──────────────────────────────

    @Test
    void exposureGridDemoContract_loads_successfully() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            SemanticContract c = loader.load(in);
            assertNotNull(c);
            assertEquals("risk_sensitivity_exposure_grid", c.name());
        }
    }

    // ── Test 3: Historical timeseries demo contract loads ─────────────────────

    @Test
    void historicalTimeseriesDemoContract_loads_successfully() throws Exception {
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            SemanticContract c = loader.load(in);
            assertNotNull(c);
            assertEquals("risk_sensitivity_historical_timeseries", c.name());
        }
    }

    // ── Test 4: ContractRegistry returns both demo contracts ──────────────────

    @Test
    void registry_populatedWithBothDemoContracts_containsBoth() throws Exception {
        SemanticContract grid;
        SemanticContract ts;
        try (InputStream in = stream(EXPOSURE_GRID)) {
            grid = loader.load(in);
        }
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            ts = loader.load(in);
        }

        var populator = ContractRegistryPopulator.create();
        var registry  = populator.populate(List.of(grid, ts));

        assertTrue(registry.contains("risk_sensitivity_exposure_grid"));
        assertTrue(registry.contains("risk_sensitivity_historical_timeseries"));
        assertEquals(2, registry.list().size());

        var found = registry.findByName("risk_sensitivity_exposure_grid");
        assertTrue(found.isPresent());
        assertEquals("risk_sensitivity_exposure_grid", found.get().name());
    }

    // ── Test 5: Contract-level metadata is preserved ──────────────────────────

    @Test
    void exposureGrid_contractMetadata_isPreserved() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            SemanticContract c = loader.load(in);
            assertEquals("risk_sensitivity_exposure_grid", c.name());
            assertEquals("1.0.0", c.version().value());
            assertFalse(c.description().isBlank(), "description must not be blank");
            assertTrue(c.description().contains("exposure grid"),
                    "description should mention 'exposure grid'");
            assertEquals("risk_sensitivity_exposure_grid", c.entity().name());
            assertFalse(c.entity().description().isBlank(), "entity description must not be blank");
        }
    }

    @Test
    void historicalTimeseries_contractMetadata_isPreserved() throws Exception {
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            SemanticContract c = loader.load(in);
            assertEquals("risk_sensitivity_historical_timeseries", c.name());
            assertEquals("1.0.0", c.version().value());
            assertFalse(c.description().isBlank(), "description must not be blank");
            assertTrue(c.description().contains("time-series"),
                    "description should mention 'time-series'");
            assertEquals("risk_sensitivity_historical_timeseries", c.entity().name());
            assertFalse(c.entity().description().isBlank(), "entity description must not be blank");
        }
    }

    // ── Test 6: Measure-level metadata is preserved ───────────────────────────

    @Test
    void exposureGrid_measureMetadata_isPreserved() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            SemanticContract c = loader.load(in);
            assertEquals(4, c.measures().size());

            var names = c.measures().stream().map(SemanticMeasure::name).toList();
            assertEquals(List.of("delta_exposure", "dv01", "vega", "gamma"), names);

            assertMeasure(c, "delta_exposure", "Delta Exposure",    "SUM(delta_exposure)", SemanticFieldType.DECIMAL);
            assertMeasure(c, "dv01",           "DV01",              "SUM(dv01)",           SemanticFieldType.DECIMAL);
            assertMeasure(c, "vega",           "Vega",              "SUM(vega)",           SemanticFieldType.DECIMAL);
            assertMeasure(c, "gamma",          "Gamma",             "SUM(gamma)",          SemanticFieldType.DECIMAL);
        }
    }

    @Test
    void historicalTimeseries_measureMetadata_isPreserved() throws Exception {
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            SemanticContract c = loader.load(in);
            assertEquals(4, c.measures().size());

            var names = c.measures().stream().map(SemanticMeasure::name).toList();
            assertEquals(List.of("delta_exposure", "dv01", "vega", "gamma"), names);

            assertMeasure(c, "delta_exposure", "Delta Exposure",    "SUM(delta_exposure)", SemanticFieldType.DECIMAL);
            assertMeasure(c, "dv01",           "DV01",              "SUM(dv01)",           SemanticFieldType.DECIMAL);
            assertMeasure(c, "vega",           "Vega",              "SUM(vega)",           SemanticFieldType.DECIMAL);
            assertMeasure(c, "gamma",          "Gamma",             "SUM(gamma)",          SemanticFieldType.DECIMAL);
        }
    }

    @Test
    void measures_descriptionIsPreserved_notBlank() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            SemanticContract c = loader.load(in);
            for (SemanticMeasure m : c.measures()) {
                assertFalse(m.description().isBlank(),
                        "measure '" + m.name() + "' description must not be blank");
            }
        }
    }

    // ── Test 7: Dimension-level metadata is preserved ─────────────────────────

    @Test
    void exposureGrid_dimensionMetadata_isPreserved() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            SemanticContract c = loader.load(in);
            assertEquals(8, c.dimensions().size());

            var names = c.dimensions().stream().map(SemanticDimension::name).toList();
            assertEquals(List.of(
                    "cob_date", "legal_entity", "business_unit", "desk",
                    "risk_class", "risk_factor", "currency", "product"), names);

            assertDimensionColumn(c, "cob_date",       "cob_date",       SemanticFieldType.DATE,   "Close of Business Date");
            assertDimensionColumn(c, "legal_entity",   "legal_entity",   SemanticFieldType.STRING, "Legal Entity");
            assertDimensionColumn(c, "business_unit",  "business_unit",  SemanticFieldType.STRING, "Business Unit");
            assertDimensionColumn(c, "desk",           "desk",           SemanticFieldType.STRING, "Trading Desk");
            assertDimensionColumn(c, "risk_class",     "risk_class",     SemanticFieldType.STRING, "Risk Class");
            assertDimensionColumn(c, "risk_factor",    "risk_factor",    SemanticFieldType.STRING, "Risk Factor");
            assertDimensionColumn(c, "currency",       "currency",       SemanticFieldType.STRING, "Currency");
            assertDimensionColumn(c, "product",        "product",        SemanticFieldType.STRING, "Product");
        }
    }

    @Test
    void historicalTimeseries_dimensionMetadata_isPreserved() throws Exception {
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            SemanticContract c = loader.load(in);
            assertEquals(8, c.dimensions().size());

            var names = c.dimensions().stream().map(SemanticDimension::name).toList();
            assertEquals(List.of(
                    "cob_date", "legal_entity", "business_unit", "desk",
                    "risk_class", "risk_factor", "currency", "product"), names);

            assertDimensionColumn(c, "cob_date", "cob_date", SemanticFieldType.DATE, "Close of Business Date");
            assertDimensionColumn(c, "desk",     "desk",     SemanticFieldType.STRING, "Trading Desk");
            assertDimensionColumn(c, "risk_class","risk_class", SemanticFieldType.STRING, "Risk Class");
        }
    }

    // ── Test 8: Groupable/filterable flags are preserved ──────────────────────

    @Test
    void exposureGrid_filterableGroupableFlags_areCorrect() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            SemanticContract c = loader.load(in);

            // cob_date — filterable and groupable (supports date-range filter AND grid grouping)
            assertFlags(c, "cob_date",      true,  true);
            // core groupable risk dimensions
            assertFlags(c, "legal_entity",  true,  true);
            assertFlags(c, "business_unit", true,  true);
            assertFlags(c, "desk",          true,  true);
            assertFlags(c, "risk_class",    true,  true);
            assertFlags(c, "risk_factor",   true,  true);
            assertFlags(c, "currency",      true,  true);
            // product — filterable but not groupable (too fine-grained for grid grouping)
            assertFlags(c, "product",       true,  false);
        }
    }

    @Test
    void historicalTimeseries_filterableGroupableFlags_areCorrect() throws Exception {
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            SemanticContract c = loader.load(in);

            // cob_date — time axis: filterable and groupable
            assertFlags(c, "cob_date",      true,  true);
            // groupable: legal_entity, desk, risk_class (common time-series slice keys)
            assertFlags(c, "legal_entity",  true,  true);
            assertFlags(c, "desk",          true,  true);
            assertFlags(c, "risk_class",    true,  true);
            // context filters only — not groupable in a time-series shape
            assertFlags(c, "business_unit", true,  false);
            assertFlags(c, "risk_factor",   true,  false);
            assertFlags(c, "currency",      true,  false);
            // product — not applicable for time-series shape
            assertFlags(c, "product",       false, false);
        }
    }

    // ── Test 9: Output shape metadata preserved where supported ───────────────
    // SemanticContract does not carry a SemanticOutputShape field — that lives in
    // SemanticQueryContract (query package, C3). Shape intent is documented in the
    // entity description field as a grain note.

    @Test
    void exposureGrid_entityDescription_containsGrainAndOutputShapeNote() throws Exception {
        try (InputStream in = stream(EXPOSURE_GRID)) {
            String entityDesc = loader.load(in).entity().description();
            assertTrue(entityDesc.contains("Grain:"),
                    "entity description should document grain");
            assertTrue(entityDesc.contains("Output shape:"),
                    "entity description should document output shape intent");
        }
    }

    @Test
    void historicalTimeseries_entityDescription_containsGrainAndOutputShapeNote() throws Exception {
        try (InputStream in = stream(HISTORY_TIMESERIES)) {
            String entityDesc = loader.load(in).entity().description();
            assertTrue(entityDesc.contains("Grain:"),
                    "entity description should document grain");
            assertTrue(entityDesc.contains("Output shape:"),
                    "entity description should document output shape intent");
        }
    }

    // ── Test 10: Missing optional metadata remains safe / backward-compatible ──

    @Test
    void existingSampleContract_withNoPolicyRuleRefs_loadsWithoutError() throws Exception {
        try (InputStream in = stream(SAMPLE_CONTRACT)) {
            SemanticContract c = loader.load(in);
            // access policy rule refs present (non-empty in sample-contract.json)
            assertNotNull(c.accessPolicy().ruleRefs());
            // cache policy rule refs present (empty list is fine)
            assertNotNull(c.cachePolicy().ruleRefs());
        }
    }

    @Test
    void demoContracts_emptyPrincipalsList_loadsSafely() throws Exception {
        for (String fixture : List.of(EXPOSURE_GRID, HISTORY_TIMESERIES)) {
            try (InputStream in = stream(fixture)) {
                SemanticContract c = loader.load(in);
                // allowedPrincipals is empty in demo fixtures — must not cause NPE or error
                assertNotNull(c.accessPolicy().allowedPrincipals());
                assertTrue(c.accessPolicy().allowedPrincipals().isEmpty(),
                        "demo fixture '" + c.name() + "' should have no explicit principals");
                assertThrows(UnsupportedOperationException.class,
                        () -> c.accessPolicy().allowedPrincipals().add(null),
                        "principals list must be unmodifiable");
            }
        }
    }

    @Test
    void demoContracts_cachePolicyIsTtl3600() throws Exception {
        for (String fixture : List.of(EXPOSURE_GRID, HISTORY_TIMESERIES)) {
            try (InputStream in = stream(fixture)) {
                var cache = loader.load(in).cachePolicy();
                assertEquals(SemanticCacheStrategy.TTL, cache.strategy());
                assertEquals(3600L, cache.ttlSeconds());
            }
        }
    }

    @Test
    void demoContracts_accessPolicyRoles_arePreserved() throws Exception {
        for (String fixture : List.of(EXPOSURE_GRID, HISTORY_TIMESERIES)) {
            try (InputStream in = stream(fixture)) {
                var policy = loader.load(in).accessPolicy();
                assertEquals(2, policy.allowedRoles().size());
                var roleNames = policy.allowedRoles().stream()
                        .map(r -> r.name()).toList();
                assertTrue(roleNames.contains("risk_analyst"));
                assertTrue(roleNames.contains("risk_viewer"));
            }
        }
    }

    @Test
    void demoContracts_measuresAndDimensions_areUnmodifiable() throws Exception {
        for (String fixture : List.of(EXPOSURE_GRID, HISTORY_TIMESERIES)) {
            try (InputStream in = stream(fixture)) {
                SemanticContract c = loader.load(in);
                assertThrows(UnsupportedOperationException.class,
                        () -> c.measures().add(null),
                        "measures must be unmodifiable for " + c.name());
                assertThrows(UnsupportedOperationException.class,
                        () -> c.dimensions().add(null),
                        "dimensions must be unmodifiable for " + c.name());
            }
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private InputStream stream(String classpathResource) {
        InputStream in = getClass().getResourceAsStream(classpathResource);
        assertNotNull(in, "classpath resource not found: " + classpathResource);
        return in;
    }

    private static void assertMeasure(SemanticContract c, String name, String label,
                                      String expression, SemanticFieldType type) {
        SemanticMeasure m = c.measures().stream()
                .filter(x -> x.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("measure not found: " + name));
        assertEquals(label,      m.label(),      "label for measure '" + name + "'");
        assertEquals(expression, m.expression(), "expression for measure '" + name + "'");
        assertEquals(type,       m.type(),       "type for measure '" + name + "'");
        assertFalse(m.description().isBlank(),   "description for measure '" + name + "' must not be blank");
    }

    private static void assertDimensionColumn(SemanticContract c, String name, String column,
                                              SemanticFieldType type, String label) {
        SemanticDimension d = c.dimensions().stream()
                .filter(x -> x.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("dimension not found: " + name));
        assertEquals(column, d.column(), "column for dimension '" + name + "'");
        assertEquals(type,   d.type(),   "type for dimension '" + name + "'");
        assertEquals(label,  d.label(),  "label for dimension '" + name + "'");
    }

    private static void assertFlags(SemanticContract c, String dimName,
                                    boolean expectedFilterable, boolean expectedGroupable) {
        SemanticDimension d = c.dimensions().stream()
                .filter(x -> x.name().equals(dimName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("dimension not found: " + dimName));
        assertEquals(expectedFilterable, d.filterable(),
                "filterable flag for '" + dimName + "'");
        assertEquals(expectedGroupable,  d.groupable(),
                "groupable flag for '" + dimName + "'");
    }
}
