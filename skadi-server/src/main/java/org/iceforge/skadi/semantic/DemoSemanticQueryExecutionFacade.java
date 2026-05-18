package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.cache.CacheContract;
import org.iceforge.skadi.semantic.demo.DemoSemanticExecutionMetadata;
import org.iceforge.skadi.semantic.demo.DemoSemanticFilter;
import org.iceforge.skadi.semantic.demo.DemoSemanticIntentAdapter;
import org.iceforge.skadi.semantic.demo.DemoSemanticInterpretationConfidence;
import org.iceforge.skadi.semantic.demo.DemoSemanticOutputShape;
import org.iceforge.skadi.semantic.demo.DemoSemanticQueryExecutionResponse;
import org.iceforge.skadi.semantic.demo.DemoSemanticQueryInterpretation;
import org.iceforge.skadi.semantic.demo.DemoSemanticQueryRequest;
import org.iceforge.skadi.semantic.demo.DemoSemanticRenderedSql;
import org.iceforge.skadi.semantic.demo.DemoSemanticResultColumn;
import org.iceforge.skadi.semantic.demo.DemoSemanticSqlRenderException;
import org.iceforge.skadi.semantic.demo.DemoSemanticSqlRenderer;
import org.iceforge.skadi.semantic.demo.DemoViewConfig;
import org.iceforge.skadi.semantic.demo.RuleBasedDemoSemanticIntentAdapter;
import org.iceforge.skadi.semantic.demo.WhitelistedDemoSemanticSqlRenderer;
import org.iceforge.skadi.semantic.request.SemanticValidationOutcome;
import org.iceforge.skadi.semantic.request.SemanticValidationReasonCode;
import org.iceforge.skadi.semantic.service.ExecutionContext;
import org.iceforge.skadi.semantic.service.ExecutionStatus;
import org.iceforge.skadi.semantic.service.QueryExecutionRequest;
import org.iceforge.skadi.semantic.service.QueryExecutionResult;
import org.iceforge.skadi.semantic.service.QueryExecutionService;
import org.iceforge.skadi.semantic.service.SemanticExecutionFailure;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Orchestrates the demo plain-English query pipeline:
 * interpret → validate (via renderer allowlists) → render SQL → execute → map result.
 *
 * <p>This class has no Spring annotations; it is instantiated by
 * {@link DemoSemanticConfiguration} and injected into
 * {@link DemoSemanticQueryController}.
 *
 * <h2>Pipeline steps</h2>
 * <ol>
 *   <li>Interpret the plain-English request via {@link DemoSemanticIntentAdapter}.</li>
 *   <li>If non-executable (UNSUPPORTED/AMBIGUOUS confidence), return a safe denied response
 *       without touching the renderer or execution service.</li>
 *   <li>Render SQL via {@link DemoSemanticSqlRenderer} (which enforces its own allowlists).
 *       A {@link DemoSemanticSqlRenderException} is mapped to a safe denied response
 *       without calling the execution service.</li>
 *   <li>Execute the rendered SQL through the injected {@link QueryExecutionService}.</li>
 *   <li>Map the {@link QueryExecutionResult} to a {@link DemoSemanticQueryExecutionResponse}.</li>
 * </ol>
 *
 * <p>No SQL is exposed in any response field. No credentials or stack traces are
 * propagated — failure classification uses only {@link SemanticExecutionFailure#safeMessage()}.
 */
public final class DemoSemanticQueryExecutionFacade {

    private static final String DEMO_PRINCIPAL   = "demo-spike";
    private static final String NOT_EXECUTED_ID  = "not-executed";
    private static final String SOURCE_SYSTEM    = "demo-api";

    private final DemoSemanticIntentAdapter intentAdapter;
    private final DemoSemanticSqlRenderer   sqlRenderer;
    private final QueryExecutionService     executionService;

    // ── Constructor ────────────────────────────────────────────────────────────

    /**
     * @param intentAdapter    the plain-English interpreter; must not be null
     * @param sqlRenderer      the whitelisted SQL renderer; must not be null
     * @param executionService the query execution seam; must not be null
     */
    public DemoSemanticQueryExecutionFacade(
            DemoSemanticIntentAdapter intentAdapter,
            DemoSemanticSqlRenderer   sqlRenderer,
            QueryExecutionService     executionService) {
        this.intentAdapter    = Objects.requireNonNull(intentAdapter,    "intentAdapter");
        this.sqlRenderer      = Objects.requireNonNull(sqlRenderer,      "sqlRenderer");
        this.executionService = Objects.requireNonNull(executionService,  "executionService");
    }

    // ── Factory ────────────────────────────────────────────────────────────────

    /**
     * Creates a facade wired from demo configuration.
     *
     * @param props            demo properties (view names, limit); must not be null
     * @param executionService the execution seam; must not be null
     * @param clock            clock for deterministic date expansion; must not be null
     */
    public static DemoSemanticQueryExecutionFacade create(
            DemoSemanticProperties props,
            QueryExecutionService  executionService,
            Clock                  clock) {
        Objects.requireNonNull(props,            "props");
        Objects.requireNonNull(executionService, "executionService");
        Objects.requireNonNull(clock,            "clock");

        var adapter   = RuleBasedDemoSemanticIntentAdapter.create();
        var viewConfig = new DemoViewConfig(props.getExposureView(), props.getHistoryView());
        var renderer  = WhitelistedDemoSemanticSqlRenderer.create(
                viewConfig, clock, props.getDefaultLimit());
        return new DemoSemanticQueryExecutionFacade(adapter, renderer, executionService);
    }

    // ── Main pipeline ──────────────────────────────────────────────────────────

    /**
     * Runs the full demo pipeline and returns a safe, structured response.
     * Never throws — all failure paths are mapped to a non-null response.
     *
     * @param request the plain-English query request; must not be null
     * @return the execution response; never null
     */
    public DemoSemanticQueryExecutionResponse execute(DemoSemanticQueryRequest request) {
        Objects.requireNonNull(request, "request must not be null");

        // Step 1: Interpret
        var interpretation = intentAdapter.interpret(request);

        // Step 2: Non-executable check
        if (!interpretation.isExecutable()) {
            return nonExecutableResponse(request.text(), interpretation);
        }

        // Step 3: Render SQL (also enforces allowlists)
        DemoSemanticRenderedSql rendered;
        try {
            rendered = sqlRenderer.render(interpretation);
        } catch (DemoSemanticSqlRenderException ex) {
            return renderFailureResponse(request.text(), interpretation, ex.getMessage());
        }

        // Step 4: Execute via the existing semantic execution seam
        var ctx     = new ExecutionContext(DEMO_PRINCIPAL, null, SOURCE_SYSTEM);
        var execReq = QueryExecutionRequest.sqlOnly(ctx, rendered.sql(), CacheContract.none());
        var result  = executionService.execute(execReq);

        // Step 5: Map to response
        return executionResultResponse(request.text(), interpretation, result);
    }

    // ── Response builders ──────────────────────────────────────────────────────

    private static DemoSemanticQueryExecutionResponse nonExecutableResponse(
            String requestText, DemoSemanticQueryInterpretation interp) {

        String warning = interp.warnings().isEmpty()
                ? "Interpretation is not executable (confidence=" + interp.confidence() + ")"
                : interp.warnings().get(0);

        var validation = SemanticValidationOutcome.denied(
                reasonCodeFor(interp.confidence()), warning);

        var metadata = new DemoSemanticExecutionMetadata(
                "demo", NOT_EXECUTED_ID, null, 0L, null, ExecutionStatus.FAILED, null);

        return new DemoSemanticQueryExecutionResponse(
                requestText, interp, validation, metadata, List.of(), List.of());
    }

    private static DemoSemanticQueryExecutionResponse renderFailureResponse(
            String requestText, DemoSemanticQueryInterpretation interp, String reason) {

        var validation = SemanticValidationOutcome.denied(
                SemanticValidationReasonCode.UNSUPPORTED_REQUEST,
                "SQL render failed: " + reason);

        String contractId = interp.contractId() != null ? interp.contractId() : "demo";
        var metadata = new DemoSemanticExecutionMetadata(
                contractId, NOT_EXECUTED_ID, null, 0L, null, ExecutionStatus.FAILED, null);

        return new DemoSemanticQueryExecutionResponse(
                requestText, interp, validation, metadata, List.of(), List.of());
    }

    private static DemoSemanticQueryExecutionResponse executionResultResponse(
            String requestText, DemoSemanticQueryInterpretation interp,
            QueryExecutionResult result) {

        boolean failed = result.status() == ExecutionStatus.FAILED;

        var validation = SemanticValidationOutcome.allowed(
                interp.contractId(),
                failed ? "Request was valid; execution failed." : "Executed successfully.");

        SemanticExecutionFailure failureClass = failed
                ? inferFailure(result.errorMessage())
                : null;

        String queryId = result.cacheIdentity().fingerprint().substring(0, 12);
        var metadata = new DemoSemanticExecutionMetadata(
                interp.contractId(),
                queryId,
                result.cacheState().name(),
                0L,
                null,
                result.status(),
                failureClass);

        List<DemoSemanticResultColumn> columns = failed ? List.of() : buildColumns(interp);

        return new DemoSemanticQueryExecutionResponse(
                requestText, interp, validation, metadata, columns, List.of());
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static SemanticValidationReasonCode reasonCodeFor(
            DemoSemanticInterpretationConfidence confidence) {
        return switch (confidence) {
            case AMBIGUOUS   -> SemanticValidationReasonCode.AMBIGUOUS_REQUEST;
            default          -> SemanticValidationReasonCode.UNSUPPORTED_REQUEST;
        };
    }

    private static SemanticExecutionFailure inferFailure(String errorMessage) {
        if (errorMessage == null) return SemanticExecutionFailure.UNEXPECTED;
        for (SemanticExecutionFailure f : SemanticExecutionFailure.values()) {
            if (f.safeMessage().equals(errorMessage)) return f;
        }
        return SemanticExecutionFailure.UNEXPECTED;
    }

    private static List<DemoSemanticResultColumn> buildColumns(DemoSemanticQueryInterpretation i) {
        var cols = new ArrayList<DemoSemanticResultColumn>();
        if (i.outputShape() == DemoSemanticOutputShape.TIMESERIES) {
            cols.add(new DemoSemanticResultColumn(
                    "cob_date", "Close of Business Date", "DATE", "TIME_AXIS"));
        }
        for (String dim : i.groupBy()) {
            cols.add(new DemoSemanticResultColumn(
                    dim, toDisplayName(dim), "STRING", "DIMENSION"));
        }
        cols.add(new DemoSemanticResultColumn(
                i.measure(), toDisplayName(i.measure()), "DECIMAL", "MEASURE"));
        return List.copyOf(cols);
    }

    private static String toDisplayName(String id) {
        return id.replace('_', ' ');
    }
}
