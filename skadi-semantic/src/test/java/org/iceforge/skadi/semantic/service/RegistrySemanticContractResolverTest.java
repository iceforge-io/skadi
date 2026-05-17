package org.iceforge.skadi.semantic.service;

import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulator;
import org.iceforge.skadi.semantic.registry.InMemoryContractRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link RegistrySemanticContractResolver}.
 *
 * <p>No Spring context, no network, no Databricks, no SQL execution,
 * no file I/O, no entitlement enforcement.
 */
class RegistrySemanticContractResolverTest {

    private static final ExecutionContext ALICE = ExecutionContext.of("alice");
    private static final ExecutionContext ANON  = ExecutionContext.anonymous();

    private InMemoryContractRegistry registry;
    private RegistrySemanticContractResolver resolver;

    @BeforeEach
    void setUp() {
        registry = new InMemoryContractRegistry();
        resolver = RegistrySemanticContractResolver.of(registry);
    }

    // ── Found ─────────────────────────────────────────────────────────────────

    @Test
    void resolve_knownContract_resolvedIsTrue() {
        registry.register(contract("mxl_risk"));
        var result = resolver.resolve(request("mxl_risk", ALICE));
        assertTrue(result.resolved());
    }

    @Test
    void resolve_knownContract_returnsCorrectContract() {
        registry.register(contract("mxl_risk"));
        var result = resolver.resolve(request("mxl_risk", ALICE));
        assertNotNull(result.contract());
        assertEquals("mxl_risk", result.contract().name());
    }

    @Test
    void resolve_knownContract_errorMessageIsNull() {
        registry.register(contract("mxl_risk"));
        var result = resolver.resolve(request("mxl_risk", ALICE));
        assertNull(result.errorMessage());
    }

    @Test
    void resolve_knownContract_returnsExactSameInstance() {
        var c = contract("mxl_risk");
        registry.register(c);
        var result = resolver.resolve(request("mxl_risk", ALICE));
        assertSame(c, result.contract());
    }

    @Test
    void resolve_multipleContractsRegistered_returnsOnlyRequested() {
        registry.register(contract("alpha"));
        registry.register(contract("beta"));
        var result = resolver.resolve(request("alpha", ALICE));
        assertTrue(result.resolved());
        assertEquals("alpha", result.contract().name());
    }

    // ── Not found ─────────────────────────────────────────────────────────────

    @Test
    void resolve_missingContract_resolvedIsFalse() {
        var result = resolver.resolve(request("non_existent", ALICE));
        assertFalse(result.resolved());
    }

    @Test
    void resolve_missingContract_contractIsNull() {
        var result = resolver.resolve(request("non_existent", ALICE));
        assertNull(result.contract());
    }

    @Test
    void resolve_missingContract_hasErrorMessage() {
        var result = resolver.resolve(request("non_existent", ALICE));
        assertNotNull(result.errorMessage());
        assertFalse(result.errorMessage().isBlank());
    }

    @Test
    void resolve_missingContract_errorMessageContractsContractName() {
        var result = resolver.resolve(request("non_existent", ALICE));
        assertTrue(result.errorMessage().contains("non_existent"),
                "error message should contain the missing contract name");
    }

    @Test
    void resolve_missingContract_errorMessageContainsPrincipalName() {
        var result = resolver.resolve(request("non_existent", ALICE));
        assertTrue(result.errorMessage().contains("alice"),
                "error message should contain the principal name");
    }

    @Test
    void resolve_emptyRegistry_returnsNotFound() {
        var result = resolver.resolve(request("anything", ALICE));
        assertFalse(result.resolved());
    }

    // ── Case sensitivity ──────────────────────────────────────────────────────

    @Test
    void resolve_differentCase_returnsNotFound() {
        registry.register(contract("MxlRisk"));
        var result = resolver.resolve(request("mxlrisk", ALICE));
        assertFalse(result.resolved(), "contract name lookup must be case-sensitive");
    }

    @Test
    void resolve_exactCase_returnsFound() {
        registry.register(contract("MxlRisk"));
        var result = resolver.resolve(request("MxlRisk", ALICE));
        assertTrue(result.resolved());
    }

    // ── Principal context — preserved but not enforced ────────────────────────

    @Test
    void resolve_differentPrincipals_sameContractReturned() {
        registry.register(contract("mxl_risk"));
        var byAlice = resolver.resolve(request("mxl_risk", ALICE));
        var byAnon  = resolver.resolve(request("mxl_risk", ANON));
        assertTrue(byAlice.resolved());
        assertTrue(byAnon.resolved(),
                "access policy is not enforced in Lane D — all principals see all contracts");
        assertSame(byAlice.contract(), byAnon.contract());
    }

    @Test
    void resolve_anonymousPrincipal_resolves() {
        registry.register(contract("mxl_risk"));
        var result = resolver.resolve(request("mxl_risk", ANON));
        assertTrue(result.resolved());
    }

    @Test
    void resolve_anonymousPrincipal_notFound_errorMessageHandlesEmptyPrincipal() {
        var result = resolver.resolve(request("missing", ANON));
        assertFalse(result.resolved());
        assertNotNull(result.errorMessage());
    }

    // ── Registry not mutated ───────────────────────────────────────────────────

    @Test
    void resolve_doesNotMutateRegistry() {
        registry.register(contract("alpha"));
        int sizeBefore = registry.list().size();
        resolver.resolve(request("alpha", ALICE));
        resolver.resolve(request("missing", ALICE));
        assertEquals(sizeBefore, registry.list().size(),
                "resolver must not add or remove contracts from the registry");
    }

    // ── Works with LoadedContractRegistry from D4 ─────────────────────────────

    @Test
    void resolve_worksWithLoadedContractRegistry_found() {
        var populator = ContractRegistryPopulator.create();
        var loaded    = populator.populate(List.of(contract("mxl_risk"), contract("credit")));
        var r         = RegistrySemanticContractResolver.of(loaded);

        var result = r.resolve(request("mxl_risk", ALICE));
        assertTrue(result.resolved());
        assertEquals("mxl_risk", result.contract().name());
    }

    @Test
    void resolve_worksWithLoadedContractRegistry_notFound() {
        var populator = ContractRegistryPopulator.create();
        var loaded    = populator.populate(List.of(contract("mxl_risk")));
        var r         = RegistrySemanticContractResolver.of(loaded);

        var result = r.resolve(request("credit", ALICE));
        assertFalse(result.resolved());
        assertNotNull(result.errorMessage());
    }

    // ── Null and invalid argument guards ──────────────────────────────────────

    @Test
    void resolve_nullRequest_throwsNullPointerException() {
        assertThrows(NullPointerException.class, () -> resolver.resolve(null));
    }

    @Test
    void of_nullRegistry_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> RegistrySemanticContractResolver.of(null));
    }

    @Test
    void request_nullContractName_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> new SemanticResolutionRequest(null, ALICE));
    }

    @Test
    void request_blankContractName_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticResolutionRequest("  ", ALICE));
    }

    @Test
    void request_nullContext_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> new SemanticResolutionRequest("mxl_risk", null));
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SemanticResolutionRequest request(String contractName, ExecutionContext ctx) {
        return new SemanticResolutionRequest(contractName, ctx);
    }

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
}
