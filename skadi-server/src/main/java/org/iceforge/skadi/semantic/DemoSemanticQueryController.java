package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.demo.DemoSemanticQueryExecutionResponse;
import org.iceforge.skadi.semantic.demo.DemoSemanticQueryRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Config-gated demo endpoint for the plain-English market-risk sensitivity demo spike.
 *
 * <h2>Endpoint</h2>
 * <pre>POST /api/semantic/v1/query/demo/execute</pre>
 *
 * <p>Active only when {@code skadi.semantic.demo.enabled=true}; returns HTTP 404
 * otherwise. The endpoint must not be active in production without explicit opt-in.
 *
 * <h2>Example request</h2>
 * <pre>{@code
 * POST /api/semantic/v1/query/demo/execute
 * Content-Type: application/json
 *
 * {
 *   "text": "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15"
 * }
 * }</pre>
 *
 * <h2>Flow</h2>
 * <ol>
 *   <li>Disabled guard → 404 if {@code enabled=false}</li>
 *   <li>Interpret plain-English via {@link RuleBasedDemoSemanticIntentAdapter}</li>
 *   <li>Non-executable (UNSUPPORTED/AMBIGUOUS) → safe denied response, no SQL rendered</li>
 *   <li>Render whitelisted SQL template via {@link WhitelistedDemoSemanticSqlRenderer}</li>
 *   <li>Execute through existing {@link org.iceforge.skadi.semantic.service.QueryExecutionService}</li>
 *   <li>Map result to {@link DemoSemanticQueryExecutionResponse}</li>
 * </ol>
 *
 * <p>No raw SQL, credentials, or exception details are returned in any response field.
 */
@RestController
@RequestMapping(path = "/api/semantic/v1/query/demo", produces = MediaType.APPLICATION_JSON_VALUE)
public class DemoSemanticQueryController {

    private final DemoSemanticQueryExecutionFacade facade;
    private final DemoSemanticProperties           props;

    public DemoSemanticQueryController(DemoSemanticQueryExecutionFacade facade,
                                       DemoSemanticProperties props) {
        this.facade = facade;
        this.props  = props;
    }

    /**
     * Interprets, validates, renders, and executes a plain-English demo query.
     *
     * <p>Returns HTTP 404 when {@code skadi.semantic.demo.enabled=false}.
     * Returns HTTP 200 for all other outcomes (including UNSUPPORTED/AMBIGUOUS prompts
     * and execution failures); the {@link DemoSemanticQueryExecutionResponse} payload
     * carries the outcome details.
     */
    @PostMapping(value = "/execute", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DemoSemanticQueryExecutionResponse> execute(
            @RequestBody DemoSemanticQueryRequest request) {

        if (!props.isEnabled()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
        }

        DemoSemanticQueryExecutionResponse response = facade.execute(request);
        return ResponseEntity.ok(response);
    }
}
