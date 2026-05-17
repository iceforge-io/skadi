package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.validation.ContractValidationSeverity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ContractRegistryPopulator} and the read-only {@link LoadedContractRegistry}.
 *
 * <p>No Spring context, no network, no Databricks, no SQL execution, no entitlements.
 */
class ContractRegistryPopulatorTest {

    private ContractRegistryPopulator populator;

    @BeforeEach
    void setUp() {
        populator = ContractRegistryPopulator.create();
    }

    // ── Success path — multiple valid contracts ───────────────────────────────

    @Test
    void populateWithResult_validContracts_isSuccess() {
        var result = populator.populateWithResult(List.of(
                contract("alpha"), contract("beta"), contract("gamma")));
        assertTrue(result.isSuccess());
        assertFalse(result.validationResult().hasErrors());
    }

    @Test
    void populateWithResult_validContracts_registryContainsAll() {
        var result = populator.populateWithResult(List.of(
                contract("alpha"), contract("beta")));
        var registry = result.registry();
        assertNotNull(registry);
        assertTrue(registry.contains("alpha"));
        assertTrue(registry.contains("beta"));
        assertEquals(2, registry.list().size());
    }

    @Test
    void populateWithResult_validContracts_loadedCountMatchesInputSize() {
        var result = populator.populateWithResult(List.of(
                contract("a"), contract("b"), contract("c")));
        assertEquals(3, result.loadedContractCount());
    }

    @Test
    void populate_validContracts_returnsNonNullRegistry() {
        var registry = populator.populate(List.of(contract("alpha"), contract("beta")));
        assertNotNull(registry);
        assertEquals(2, registry.list().size());
    }

    // ── Empty list ────────────────────────────────────────────────────────────

    @Test
    void populateWithResult_emptyList_isSuccess() {
        var result = populator.populateWithResult(List.of());
        assertTrue(result.isSuccess());
        assertNotNull(result.registry());
        assertEquals(0, result.loadedContractCount());
        assertTrue(result.registry().list().isEmpty());
    }

    // ── Duplicate names — validation errors prevent population ────────────────

    @Test
    void populateWithResult_duplicateNames_isNotSuccess() {
        var result = populator.populateWithResult(List.of(
                contract("shared"), contract("shared")));
        assertFalse(result.isSuccess());
        assertTrue(result.validationResult().hasErrors());
    }

    @Test
    void populateWithResult_duplicateNames_registryIsNull() {
        var result = populator.populateWithResult(List.of(
                contract("shared"), contract("shared")));
        assertNull(result.registry(), "registry must be null when population fails");
    }

    @Test
    void populateWithResult_duplicateNames_loadedCountIsZero() {
        var result = populator.populateWithResult(List.of(
                contract("dup"), contract("dup")));
        assertEquals(0, result.loadedContractCount());
    }

    @Test
    void populate_duplicateNames_throwsContractRegistryPopulationException() {
        assertThrows(ContractRegistryPopulationException.class,
                () -> populator.populate(List.of(contract("dup"), contract("dup"))));
    }

    @Test
    void populationException_hasValidationResult() {
        var ex = assertThrows(ContractRegistryPopulationException.class,
                () -> populator.populate(List.of(contract("dup"), contract("dup"))));
        assertNotNull(ex.validationResult());
        assertTrue(ex.validationResult().hasErrors());
    }

    @Test
    void populationException_messageContainsErrorCount() {
        var ex = assertThrows(ContractRegistryPopulationException.class,
                () -> populator.populate(List.of(contract("dup"), contract("dup"))));
        assertTrue(ex.getMessage().contains("1"),
                "message should mention the error count; got: " + ex.getMessage());
    }

    // ── Warnings do not block population ─────────────────────────────────────

    @Test
    void populateWithResult_warningOnly_isSuccess() {
        // contract with no measures → CONTRACT_NO_MEASURES warning
        var result = populator.populateWithResult(List.of(contractNoMeasures("c")));
        assertTrue(result.isSuccess(), "warnings must not prevent population");
    }

    @Test
    void populateWithResult_warningOnly_registryIsNotNull() {
        var result = populator.populateWithResult(List.of(contractNoMeasures("c")));
        assertNotNull(result.registry());
    }

    @Test
    void populateWithResult_warningOnly_hasWarnings() {
        var result = populator.populateWithResult(List.of(contractNoMeasures("c")));
        assertTrue(result.hasWarnings());
        assertTrue(result.validationResult().hasWarnings());
    }

    @Test
    void populate_warningOnly_doesNotThrow() {
        assertDoesNotThrow(() -> populator.populate(List.of(contractNoMeasures("c"))));
    }

    @Test
    void populateWithResult_datasetVersionCacheStrategy_isSuccessWithWarning() {
        var result = populator.populateWithResult(
                List.of(contractWithDatasetVersionCache("c")));
        assertTrue(result.isSuccess());
        assertTrue(result.hasWarnings());
        assertTrue(result.validationResult().issues().stream()
                .anyMatch(i -> i.severity() == ContractValidationSeverity.WARNING));
    }

    // ── Registry ordering ─────────────────────────────────────────────────────

    @Test
    void registry_listOrder_matchesInputOrder() {
        var result = populator.populateWithResult(List.of(
                contract("gamma"), contract("alpha"), contract("beta")));
        var names = result.registry().list().stream()
                .map(SemanticContract::name).toList();
        assertEquals(List.of("gamma", "alpha", "beta"), names);
    }

    @Test
    void registry_listOrder_isDeterministic() {
        var contracts = List.of(contract("z"), contract("a"), contract("m"));
        var r1 = populator.populateWithResult(contracts).registry().list();
        var r2 = populator.populateWithResult(contracts).registry().list();
        assertEquals(r1.stream().map(SemanticContract::name).toList(),
                r2.stream().map(SemanticContract::name).toList());
    }

    // ── Read-only registry ────────────────────────────────────────────────────

    @Test
    void registry_register_throwsUnsupportedOperationException() {
        var registry = populator.populate(List.of(contract("a")));
        assertThrows(UnsupportedOperationException.class,
                () -> registry.register(contract("b")));
    }

    @Test
    void registry_remove_throwsUnsupportedOperationException() {
        var registry = populator.populate(List.of(contract("a")));
        assertThrows(UnsupportedOperationException.class,
                () -> registry.remove("a"));
    }

    @Test
    void registry_list_isUnmodifiable() {
        var registry = populator.populate(List.of(contract("a")));
        assertThrows(UnsupportedOperationException.class,
                () -> registry.list().add(contract("x")));
    }

    // ── Registry lookups ──────────────────────────────────────────────────────

    @Test
    void registry_findByName_known_returnsPresent() {
        var registry = populator.populate(List.of(contract("alpha")));
        var found = registry.findByName("alpha");
        assertTrue(found.isPresent());
        assertEquals("alpha", found.get().name());
    }

    @Test
    void registry_findByName_unknown_returnsEmpty() {
        var registry = populator.populate(List.of(contract("alpha")));
        assertTrue(registry.findByName("not-there").isEmpty());
    }

    @Test
    void registry_contains_knownName_returnsTrue() {
        var registry = populator.populate(List.of(contract("alpha")));
        assertTrue(registry.contains("alpha"));
    }

    @Test
    void registry_contains_unknownName_returnsFalse() {
        var registry = populator.populate(List.of(contract("alpha")));
        assertFalse(registry.contains("ghost"));
    }

    @Test
    void registry_forPrincipal_returnsAllContracts() {
        var registry = populator.populate(List.of(contract("a"), contract("b")));
        var result = registry.forPrincipal("alice");
        assertEquals(2, result.size());
    }

    @Test
    void registry_forPrincipal_isUnmodifiable() {
        var registry = populator.populate(List.of(contract("a")));
        assertThrows(UnsupportedOperationException.class,
                () -> registry.forPrincipal("alice").add(contract("x")));
    }

    @Test
    void registry_findByName_nullArg_throwsNullPointerException() {
        var registry = populator.populate(List.of(contract("a")));
        assertThrows(NullPointerException.class, () -> registry.findByName(null));
    }

    @Test
    void registry_forPrincipal_nullArg_throwsNullPointerException() {
        var registry = populator.populate(List.of(contract("a")));
        assertThrows(NullPointerException.class, () -> registry.forPrincipal(null));
    }

    // ── populateFromPaths — end-to-end D3 + D4 ───────────────────────────────

    @Test
    void populateFromPaths_validFiles_returnsSuccessResult(@TempDir Path tmp) throws Exception {
        Path f1 = copyFixture(tmp, "sample-contract.json", "mxl_risk.json");
        var result = populator.populateFromPaths(List.of(f1));
        assertTrue(result.isSuccess());
        assertNotNull(result.registry());
        assertEquals(1, result.loadedContractCount());
        assertTrue(result.registry().contains("mxl_risk"));
    }

    @Test
    void populateFromPaths_emptyList_returnsSuccess(@TempDir Path tmp) {
        var result = populator.populateFromPaths(List.of());
        assertTrue(result.isSuccess());
        assertEquals(0, result.loadedContractCount());
    }

    // ── Null guards ───────────────────────────────────────────────────────────

    @Test
    void populateWithResult_nullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> populator.populateWithResult(null));
    }

    @Test
    void populate_nullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> populator.populate(null));
    }

    @Test
    void populateFromPaths_nullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> populator.populateFromPaths(null));
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /** Minimal valid contract with one measure and one dimension — no validation warnings. */
    private static SemanticContract contract(String name) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"), List.of()),
                List.of(new SemanticMeasure("m", "M", "SUM(m)", SemanticFieldType.DECIMAL, "", null)),
                List.of(new SemanticDimension("d", "d", SemanticFieldType.STRING, "D", true, true, null)),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.none(),
                null);
    }

    private static SemanticContract contractNoMeasures(String name) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"), List.of()),
                List.of(),   // no measures → CONTRACT_NO_MEASURES warning
                List.of(),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.none(),
                null);
    }

    private static SemanticContract contractWithDatasetVersionCache(String name) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"), List.of()),
                List.of(), List.of(),
                SemanticAccessPolicy.unrestricted(),
                new SemanticCachePolicy(SemanticCacheStrategy.DATASET_VERSION, null, List.of()),
                null);
    }

    // ── Enriched fixture — explanation metadata through registry boundary ──────

    @Test
    void enriched_contractExplanation_survivesRegistryBoundary(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var c = registry.findByName("cib_var")
                .orElseThrow(() -> new AssertionError("cib_var not found in registry"));

        assertNotNull(c.explanation(), "contract explanation must not be stripped by registry");
        assertEquals("CIB Risk Technology", c.explanation().owner());
        assertNotNull(c.explanation().intendedUsage());
        assertTrue(c.explanation().intendedUsage().contains("Basel III"));
    }

    @Test
    void enriched_contractExplanation_caveats_survivesRegistryBoundary(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var exp = registry.findByName("cib_var")
                .orElseThrow().explanation();

        assertNotNull(exp.caveats());
        assertEquals(3, exp.caveats().size());
        assertTrue(exp.caveats().get(0).contains("8 AM UTC"));
    }

    @Test
    void enriched_contractExplanation_grain_survivesRegistryBoundary(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var grain = registry.findByName("cib_var").orElseThrow().explanation().grain();
        assertNotNull(grain);
        assertTrue(grain.contains("legal entity"));
    }

    @Test
    void enriched_contractExplanation_outputShapes_survivesRegistryBoundary(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var shapes = registry.findByName("cib_var").orElseThrow().explanation().outputShapes();
        assertNotNull(shapes);
        assertEquals(3, shapes.size());
        assertTrue(shapes.contains("var_by_legal_entity"));
    }

    @Test
    void enriched_contractExplanation_lineageHook_survivesRegistryBoundary(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var hook = registry.findByName("cib_var").orElseThrow().explanation().lineageHook();
        assertNotNull(hook);
        assertEquals("RISK-LIN-VAR-01", hook.hookId());
    }

    @Test
    void enriched_contractExplanation_entitlementBehavior_survivesRegistryBoundary(
            @TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var eb = registry.findByName("cib_var").orElseThrow().explanation().entitlementBehavior();
        assertNotNull(eb);
        assertEquals("ROW_FILTER", eb.mode());
        assertNotNull(eb.description());
    }

    @Test
    void enriched_contractExplanation_validGroupingPaths_survivesRegistryBoundary(
            @TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var paths = registry.findByName("cib_var").orElseThrow().explanation().validGroupingPaths();
        assertNotNull(paths);
        assertEquals(7, paths.size());
        assertTrue(paths.contains("cob_date,legal_entity,risk_class"));
    }

    @Test
    void enriched_measureExplanation_survivesRegistryBoundary(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var measures = registry.findByName("cib_var").orElseThrow().measures();

        SemanticMeasure var1d = measures.stream()
                .filter(m -> m.name().equals("var_1d"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("var_1d not found"));

        assertNotNull(var1d.explanation(), "measure explanation must not be stripped by registry");
        assertTrue(var1d.explanation().intendedUsage().contains("regulatory capital"));
        assertEquals(2, var1d.explanation().caveats().size());
    }

    @Test
    void enriched_measureExplanation_bothMeasuresRetainMetadata(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var measures = registry.findByName("cib_var").orElseThrow().measures();

        for (SemanticMeasure m : measures) {
            assertNotNull(m.explanation(),
                    "measure " + m.name() + " should carry explanation metadata");
            assertNotNull(m.explanation().intendedUsage());
            assertNotNull(m.explanation().caveats());
            assertFalse(m.explanation().caveats().isEmpty());
        }
    }

    @Test
    void enriched_dimensionExplanation_survivesRegistryBoundary(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var dimensions = registry.findByName("cib_var").orElseThrow().dimensions();

        SemanticDimension legalEntity = dimensions.stream()
                .filter(d -> d.name().equals("legal_entity"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("legal_entity not found"));

        assertNotNull(legalEntity.explanation(),
                "dimension explanation must not be stripped by registry");
        assertNotNull(legalEntity.explanation().description());
        assertTrue(legalEntity.explanation().description().contains("legal entity"));
    }

    @Test
    void enriched_dimensionExplanation_allDimensionsRetainMetadata(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var dimensions = registry.findByName("cib_var").orElseThrow().dimensions();

        for (SemanticDimension d : dimensions) {
            assertNotNull(d.explanation(),
                    "dimension " + d.name() + " should carry explanation metadata");
            assertNotNull(d.explanation().description());
        }
    }

    @Test
    void enriched_dimension_groupableAndFilterable_survivesRegistryBoundary(@TempDir Path tmp)
            throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var dimensions = registry.findByName("cib_var").orElseThrow().dimensions();

        SemanticDimension desk = dimensions.stream()
                .filter(d -> d.name().equals("desk"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("desk not found"));

        assertFalse(desk.groupable(), "desk must not be groupable");
        assertTrue(desk.filterable(), "desk must be filterable");
    }

    @Test
    void enriched_forPrincipal_returnsCibVarWithExplanation(@TempDir Path tmp) throws Exception {
        var registry = loadEnrichedRegistry(tmp);
        var accessible = registry.forPrincipal("alice");

        assertTrue(accessible.stream().anyMatch(c -> c.name().equals("cib_var")),
                "cib_var must appear in forPrincipal result");
        var cibVar = accessible.stream()
                .filter(c -> c.name().equals("cib_var"))
                .findFirst()
                .orElseThrow();
        assertNotNull(cibVar.explanation(), "explanation must be present via forPrincipal");
    }

    @Test
    void minimal_fixture_hasNullExplanation_afterRegistration(@TempDir Path tmp) throws Exception {
        Path f = copyFixture(tmp, "sample-contract.json", "mxl_risk.json");
        var registry = populator.populateFromPaths(List.of(f)).registry();
        var mxlRisk = registry.findByName("mxl_risk").orElseThrow();
        assertNull(mxlRisk.explanation(),
                "minimal fixture must not carry explanation metadata after registration");
    }

    /** Loads the enriched fixture into a registry via the full populateFromPaths path. */
    private ContractRegistry loadEnrichedRegistry(Path tmp) throws Exception {
        Path f = copyFixture(tmp, "enriched-contract.json", "cib_var.json");
        var result = populator.populateFromPaths(List.of(f));
        assertTrue(result.isSuccess(), "enriched fixture must pass validation");
        return result.registry();
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private Path copyFixture(Path tmp, String fixtureResource, String destName)
            throws Exception {
        Path dest = tmp.resolve(destName);
        try (InputStream in = getClass().getResourceAsStream("/fixtures/" + fixtureResource)) {
            assertNotNull(in, "fixture not found: " + fixtureResource);
            Files.copy(in, dest);
        }
        return dest;
    }
}
