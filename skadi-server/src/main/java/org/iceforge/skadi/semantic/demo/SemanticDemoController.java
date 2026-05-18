package org.iceforge.skadi.semantic.demo;

import org.iceforge.skadi.semantic.DemoSemanticProperties;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Config-gated demo endpoint for structured semantic JDBC execution.
 *
 * <h2>Endpoint</h2>
 * <pre>POST /api/semantic/v1/demo/query</pre>
 *
 * <p>Returns HTTP 404 when {@code skadi.semantic.demo.enabled=false} (the default).
 * The endpoint must not be active in production without explicit opt-in.
 *
 * <h2>Execution modes</h2>
 * <ul>
 *   <li>{@code DISABLED} — endpoint active but no execution; returns 200 with empty rows.</li>
 *   <li>{@code SKADI_SERVER_DELEGATED} — not yet implemented; returns 501.</li>
 *   <li>{@code JDBC_DIRECT} — executes via {@link DirectJdbcSemanticDemoExecutor}.</li>
 * </ul>
 *
 * <p>No credentials, raw exception messages, or stack traces are returned in any response.
 */
@RestController
@RequestMapping(path = "/api/semantic/v1/demo", produces = MediaType.APPLICATION_JSON_VALUE)
public class SemanticDemoController {

    private final DemoSemanticProperties      props;
    private final DirectJdbcSemanticDemoExecutor executor;

    public SemanticDemoController(DemoSemanticProperties props,
                                  DirectJdbcSemanticDemoExecutor executor) {
        this.props    = props;
        this.executor = executor;
    }

    /**
     * Executes a structured semantic demo query.
     *
     * <p>Returns HTTP 404 when {@code skadi.semantic.demo.enabled=false}.
     * Returns HTTP 200 for all active-mode outcomes — including DISABLED mode
     * (empty rows) and execution failures (safe error explanation).
     */
    @PostMapping(value = "/query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SemanticDemoResponse> query(@RequestBody SemanticDemoRequest request) {

        if (!props.isEnabled()) {
            return ResponseEntity.notFound().build();
        }

        SemanticDemoExecutionMode mode;
        try {
            mode = SemanticDemoExecutionMode.fromConfig(props.getExecutionMode());
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }

        return switch (mode) {
            case DISABLED -> ResponseEntity.ok(disabledResponse(request));
            case SKADI_SERVER_DELEGATED -> ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).build();
            case JDBC_DIRECT -> ResponseEntity.ok(executor.execute(request));
        };
    }

    private static SemanticDemoResponse disabledResponse(SemanticDemoRequest request) {
        return new SemanticDemoResponse(
                SemanticDemoExecutionMode.DISABLED.name(),
                request.contractId(),
                null,
                List.of(),
                List.of(),
                0,
                "Execution mode is DISABLED. Set skadi.semantic.demo.execution-mode=jdbc_direct to enable.");
    }
}
