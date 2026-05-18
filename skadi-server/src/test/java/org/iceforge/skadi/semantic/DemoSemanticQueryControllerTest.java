package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.cache.CacheEntryState;
import org.iceforge.skadi.semantic.cache.CacheIdentity;
import org.iceforge.skadi.semantic.demo.DemoSemanticQueryRequest;
import org.iceforge.skadi.semantic.service.QueryExecutionResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * MockMvc standalone tests for {@link DemoSemanticQueryController}.
 *
 * <p>No Spring context, no Databricks, no S3, no real skadi-server.
 * Uses hand-rolled fakes for the execution service.
 */
class DemoSemanticQueryControllerTest {

    private static final String URL = "/api/semantic/v1/query/demo/execute";

    private static final Clock FIXED_CLOCK = Clock.fixed(
            LocalDate.of(2026, 5, 18).atStartOfDay(ZoneOffset.UTC).toInstant(),
            ZoneOffset.UTC);

    private MockMvc mockMvc;
    private ObjectMapper mapper;
    private DemoSemanticProperties enabledProps;

    @BeforeEach
    void setUp() {
        enabledProps = new DemoSemanticProperties();
        enabledProps.setEnabled(true);

        var facade = DemoSemanticQueryExecutionFacade.create(
                enabledProps, successService(), FIXED_CLOCK);

        mockMvc = MockMvcBuilders
                .standaloneSetup(new DemoSemanticQueryController(facade, enabledProps))
                .build();
        mapper = new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS);
    }

    // ── 1. Disabled endpoint ───────────────────────────────────────────────────

    @Test
    void disabled_returns404() throws Exception {
        var disabledProps = new DemoSemanticProperties();
        disabledProps.setEnabled(false);

        var facade = DemoSemanticQueryExecutionFacade.create(
                disabledProps, neverCalledService(), FIXED_CLOCK);
        var disabledMvc = MockMvcBuilders
                .standaloneSetup(new DemoSemanticQueryController(facade, disabledProps))
                .build();

        disabledMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show delta exposure by desk for CIB on 2026-05-15")))
                .andExpect(status().isNotFound());
    }

    // ── 2. Valid exposure-grid prompt ─────────────────────────────────────────

    @Test
    void exposureGrid_returns200() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15")))
                .andExpect(status().isOk())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON));
    }

    @Test
    void exposureGrid_responseContainsContractId() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15")))
                .andExpect(jsonPath("$.metadata.contractId")
                        .value("risk_sensitivity_exposure_grid"));
    }

    @Test
    void exposureGrid_responseValidationAllowed() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15")))
                .andExpect(jsonPath("$.validation.allowed").value(true));
    }

    @Test
    void exposureGrid_responseHasColumns() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15")))
                .andExpect(jsonPath("$.columns").isArray())
                .andExpect(jsonPath("$.columns.length()").value(org.hamcrest.Matchers.greaterThan(0)));
    }

    @Test
    void exposureGrid_requestTextPreservedInResponse() throws Exception {
        String text = "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15";
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(text)))
                .andExpect(jsonPath("$.requestText").value(text));
    }

    // ── 3. Valid time-series prompt ───────────────────────────────────────────

    @Test
    void timeseries_returns200() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show the last 30 days of rates DV01 by desk for CIB")))
                .andExpect(status().isOk());
    }

    @Test
    void timeseries_contractIdIsTimeseriesContract() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show the last 30 days of rates DV01 by desk for CIB")))
                .andExpect(jsonPath("$.metadata.contractId")
                        .value("risk_sensitivity_historical_timeseries"));
    }

    @Test
    void timeseries_firstColumnIsCobDate() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show the last 30 days of rates DV01 by desk for CIB")))
                .andExpect(jsonPath("$.columns[0].name").value("cob_date"));
    }

    // ── 4. Unsupported prompt: safe response, no execution ────────────────────

    @Test
    void unsupported_returns200WithDenied() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show me everything in the table")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.validation.allowed").value(false));
    }

    @Test
    void unsupported_columnsEmpty() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show me everything in the table")))
                .andExpect(jsonPath("$.columns").isEmpty());
    }

    // ── 5. Ambiguous prompt: safe response, no execution ─────────────────────

    @Test
    void ambiguous_returns200WithDenied() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show exposure")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.validation.allowed").value(false));
    }

    // ── 6. Execution failure: safe response ───────────────────────────────────

    @Test
    void executionUnavailable_returns200WithFailureMetadata() throws Exception {
        var unavailableFacade = DemoSemanticQueryExecutionFacade.create(
                enabledProps,
                req -> {
                    var id = new CacheIdentity(req.sql(), "demo", null);
                    return QueryExecutionResult.failed(req.context(), id,
                            org.iceforge.skadi.semantic.service.SemanticExecutionFailure.UNAVAILABLE.safeMessage());
                },
                FIXED_CLOCK);

        var unavailableMvc = MockMvcBuilders
                .standaloneSetup(new DemoSemanticQueryController(unavailableFacade, enabledProps))
                .build();

        unavailableMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show delta exposure by desk for CIB on 2026-05-15")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.metadata.executionStatus").value("FAILED"))
                .andExpect(jsonPath("$.metadata.failureClassification").value("UNAVAILABLE"));
    }

    // ── 7. No SQL in any response field ────────────────────────────────────────

    @Test
    void noSqlInResponse_metadata() throws Exception {
        var result = mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15")))
                .andExpect(status().isOk())
                .andReturn();

        String body = result.getResponse().getContentAsString();
        assertFalse(body.toUpperCase().contains("\"SELECT"),
                "response body must not contain SQL SELECT: " + body.substring(0, Math.min(200, body.length())));
    }

    // ── 8. Content-type negotiation ───────────────────────────────────────────

    @Test
    void endpoint_producesJson() throws Exception {
        mockMvc.perform(post(URL)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body("Show delta exposure by desk for CIB on 2026-05-15")))
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private String body(String text) throws Exception {
        return mapper.writeValueAsString(new DemoSemanticQueryRequest(text));
    }

    private static org.iceforge.skadi.semantic.service.QueryExecutionService successService() {
        return req -> {
            var id = new CacheIdentity(req.sql(), "demo", null);
            return QueryExecutionResult.completed(req.context(), id, CacheEntryState.ABSENT);
        };
    }

    private static org.iceforge.skadi.semantic.service.QueryExecutionService neverCalledService() {
        return req -> {
            throw new AssertionError("execution service must not be called when demo is disabled");
        };
    }

    private static void assertFalse(boolean condition, String message) {
        if (condition) throw new AssertionError(message);
    }
}
