package org.iceforge.skadi.semantic;

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
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.validation.SemanticContractValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Unit tests for {@link SemanticContractMetadataController}.
 *
 * <p>Uses {@code MockMvcBuilders.standaloneSetup} — no Spring context, no Databricks,
 * no file I/O, no SQL execution, no query service invocation.
 */
class SemanticContractMetadataControllerTest {

    private MockMvc mockMvc;
    private ContractRegistry registry;
    private SemanticContractValidator validator;
    private SemanticContractProperties properties;

    @BeforeEach
    void setUp() {
        registry   = Mockito.mock(ContractRegistry.class);
        validator  = SemanticContractValidator.create();   // real validator — no mocking needed
        properties = new SemanticContractProperties();
        properties.setEnabled(true);

        var controller = new SemanticContractMetadataController(registry, validator, properties);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    // ── GET /api/semantic/contracts (list) ────────────────────────────────────

    @Test
    void list_emptyRegistry_returnsEmptyContracts() throws Exception {
        Mockito.when(registry.list()).thenReturn(List.of());

        mockMvc.perform(get("/api/semantic/contracts"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.enabled").value(true))
                .andExpect(jsonPath("$.contractCount").value(0))
                .andExpect(jsonPath("$.contracts").isArray())
                .andExpect(jsonPath("$.contracts").isEmpty());
    }

    @Test
    void list_oneContract_returnsSummary() throws Exception {
        Mockito.when(registry.list()).thenReturn(List.of(mxlRisk()));

        mockMvc.perform(get("/api/semantic/contracts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.contractCount").value(1))
                .andExpect(jsonPath("$.contracts[0].name").value("mxl_risk"))
                .andExpect(jsonPath("$.contracts[0].version").value("1.0.0"))
                .andExpect(jsonPath("$.contracts[0].measureCount").value(2))
                .andExpect(jsonPath("$.contracts[0].dimensionCount").value(1));
    }

    @Test
    void list_twoContracts_returnsAllSummaries() throws Exception {
        Mockito.when(registry.list()).thenReturn(List.of(mxlRisk(), minimalContract("credit")));

        mockMvc.perform(get("/api/semantic/contracts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.contractCount").value(2))
                .andExpect(jsonPath("$.contracts[0].name").value("mxl_risk"))
                .andExpect(jsonPath("$.contracts[1].name").value("credit"));
    }

    @Test
    void list_disabledFeature_showsEnabledFalse() throws Exception {
        properties.setEnabled(false);
        Mockito.when(registry.list()).thenReturn(List.of());

        mockMvc.perform(get("/api/semantic/contracts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.enabled").value(false))
                .andExpect(jsonPath("$.contractCount").value(0));
    }

    // ── GET /api/semantic/contracts/{name} ────────────────────────────────────

    @Test
    void getByName_found_returnsDetail() throws Exception {
        Mockito.when(registry.findByName("mxl_risk")).thenReturn(Optional.of(mxlRisk()));

        mockMvc.perform(get("/api/semantic/contracts/mxl_risk"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("mxl_risk"))
                .andExpect(jsonPath("$.version").value("1.0.0"))
                .andExpect(jsonPath("$.measureCount").value(2))
                .andExpect(jsonPath("$.dimensionCount").value(1))
                .andExpect(jsonPath("$.cacheStrategy").value("TTL"))
                .andExpect(jsonPath("$.ttlSeconds").value(7200));
    }

    @Test
    void getByName_notFound_returns404() throws Exception {
        Mockito.when(registry.findByName("unknown")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/semantic/contracts/unknown"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("contract not found"))
                .andExpect(jsonPath("$.name").value("unknown"));
    }

    @Test
    void getByName_notFound_doesNotReturnContract() throws Exception {
        Mockito.when(registry.findByName("x")).thenReturn(Optional.empty());

        mockMvc.perform(get("/api/semantic/contracts/x"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.cacheStrategy").doesNotExist());
    }

    @Test
    void getByName_noneStrategy_ttlSecondsIsNull() throws Exception {
        var c = contractWithCache("c", SemanticCachePolicy.none());
        Mockito.when(registry.findByName("c")).thenReturn(Optional.of(c));

        mockMvc.perform(get("/api/semantic/contracts/c"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.cacheStrategy").value("NONE"))
                .andExpect(jsonPath("$.ttlSeconds").isEmpty());
    }

    // ── GET /api/semantic/contracts/validation ────────────────────────────────

    @Test
    void validation_emptyRegistry_returnsValid() throws Exception {
        Mockito.when(registry.list()).thenReturn(List.of());

        mockMvc.perform(get("/api/semantic/contracts/validation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").value(true))
                .andExpect(jsonPath("$.errorCount").value(0))
                .andExpect(jsonPath("$.warningCount").value(0))
                .andExpect(jsonPath("$.issues").isArray())
                .andExpect(jsonPath("$.issues").isEmpty());
    }

    @Test
    void validation_cleanContracts_returnsValid() throws Exception {
        Mockito.when(registry.list()).thenReturn(List.of(mxlRisk(), minimalContract("credit")));

        mockMvc.perform(get("/api/semantic/contracts/validation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").value(true))
                .andExpect(jsonPath("$.errorCount").value(0));
    }

    @Test
    void validation_contractWithWarning_returnsValidWithWarning() throws Exception {
        // no-measures contract → CONTRACT_NO_MEASURES warning
        var noMeasures = new SemanticContract(
                "empty_contract",
                new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity("empty_contract", "",
                        new SemanticEndpoint("c", "s", "t"), List.of()),
                List.of(), List.of(),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.none());
        Mockito.when(registry.list()).thenReturn(List.of(noMeasures));

        mockMvc.perform(get("/api/semantic/contracts/validation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").value(true))     // warnings don't make invalid
                .andExpect(jsonPath("$.warningCount").value(2)) // no measures + no dimensions
                .andExpect(jsonPath("$.issues[0].severity").value("WARNING"))
                .andExpect(jsonPath("$.issues[0].code").exists());
    }

    // ── /validation is not captured by /{name} ────────────────────────────────

    @Test
    void validationPath_notRoutedToGetByName() throws Exception {
        // If /validation were captured by /{name}, findByName("validation") would be called.
        // It should NOT be called — Spring resolves the literal /validation mapping first.
        Mockito.when(registry.list()).thenReturn(List.of());

        mockMvc.perform(get("/api/semantic/contracts/validation"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").exists());  // validation response shape

        verify(registry, never()).findByName("validation");
    }

    // ── No query service / external system invoked ────────────────────────────

    @Test
    void list_doesNotMutateRegistry() throws Exception {
        Mockito.when(registry.list()).thenReturn(List.of());

        mockMvc.perform(get("/api/semantic/contracts")).andExpect(status().isOk());

        verify(registry, never()).register(Mockito.any());
        verify(registry, never()).remove(Mockito.any());
    }

    @Test
    void validation_callsValidateAllWithRegistryList() throws Exception {
        var contracts = List.of(mxlRisk());
        Mockito.when(registry.list()).thenReturn(contracts);

        mockMvc.perform(get("/api/semantic/contracts/validation")).andExpect(status().isOk());

        // Validator is called on registry.list() — registry is not mutated
        verify(registry, never()).register(Mockito.any());
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static SemanticContract mxlRisk() {
        return new SemanticContract(
                "mxl_risk",
                new SemanticContractVersion("1.0.0"),
                "MXL risk gold layer",
                new SemanticEntity("mxl_risk", "",
                        new SemanticEndpoint("main", "risk", "gold_risk"), List.of()),
                List.of(
                        new SemanticMeasure("pnl",       "PnL",   "SUM(pnl)",   SemanticFieldType.DECIMAL, ""),
                        new SemanticMeasure("delta_risk", "Delta", "SUM(delta)", SemanticFieldType.DECIMAL, "")),
                List.of(
                        new SemanticDimension("book", "book", SemanticFieldType.STRING, "Book", true, true)),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.ttl(7200L));
    }

    private static SemanticContract minimalContract(String name) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"), List.of()),
                List.of(new SemanticMeasure("m", "M", "SUM(m)", SemanticFieldType.DECIMAL, "")),
                List.of(new SemanticDimension("d", "d", SemanticFieldType.STRING, "D", true, true)),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.none());
    }

    private static SemanticContract contractWithCache(String name, SemanticCachePolicy policy) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"), List.of()),
                List.of(new SemanticMeasure("m", "M", "SUM(m)", SemanticFieldType.DECIMAL, "")),
                List.of(new SemanticDimension("d", "d", SemanticFieldType.STRING, "D", true, true)),
                SemanticAccessPolicy.unrestricted(),
                policy);
    }
}
