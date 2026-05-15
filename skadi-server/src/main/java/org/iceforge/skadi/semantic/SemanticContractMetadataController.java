package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.validation.ContractValidationSeverity;
import org.iceforge.skadi.semantic.validation.ContractValidationIssue;
import org.iceforge.skadi.semantic.validation.SemanticContractValidator;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Read-only REST endpoints exposing loaded semantic contract metadata (Lane D, D7).
 *
 * <h2>Endpoints</h2>
 * <ul>
 *   <li>{@code GET /api/semantic/contracts} — list all loaded contracts (summary)</li>
 *   <li>{@code GET /api/semantic/contracts/validation} — validation status of loaded contracts</li>
 *   <li>{@code GET /api/semantic/contracts/{name}} — full detail for one contract;
 *       {@code 404} if not found</li>
 * </ul>
 *
 * <p>These endpoints are <strong>read-only</strong>. They do not:
 * <ul>
 *   <li>Execute SQL or generate SQL</li>
 *   <li>Call Databricks, S3, or any cache service</li>
 *   <li>Mutate contracts or the registry</li>
 *   <li>Enforce entitlements (access policy enforcement is deferred post-Lane D)</li>
 *   <li>Expose sensitive credentials</li>
 * </ul>
 *
 * <p>When the feature is disabled ({@code skadi.semantic.contracts.enabled=false})
 * or no contract files are configured, the registry is empty and the list endpoint
 * returns an empty {@code contracts} array.
 */
@RestController
@RequestMapping("/api/semantic/contracts")
public class SemanticContractMetadataController {

    private final ContractRegistry registry;
    private final SemanticContractValidator validator;
    private final SemanticContractProperties properties;

    public SemanticContractMetadataController(ContractRegistry registry,
                                              SemanticContractValidator validator,
                                              SemanticContractProperties properties) {
        this.registry   = registry;
        this.validator  = validator;
        this.properties = properties;
    }

    /**
     * Lists all loaded contracts as summaries.
     *
     * <p>Response shape:
     * <pre>{@code
     * {
     *   "enabled": true,
     *   "contractCount": 2,
     *   "contracts": [
     *     {"name": "mxl_risk", "version": "1.0.0", "description": "...",
     *      "measureCount": 2, "dimensionCount": 3},
     *     ...
     *   ]
     * }
     * }</pre>
     */
    @GetMapping(produces = "application/json")
    public Map<String, Object> list() {
        var contracts = registry.list();
        return Map.of(
                "enabled",       properties.isEnabled(),
                "contractCount", contracts.size(),
                "contracts",     contracts.stream().map(this::summary).toList()
        );
    }

    /**
     * Returns the validation status of all currently loaded contracts.
     *
     * <p>Response shape:
     * <pre>{@code
     * {
     *   "valid": true,
     *   "errorCount": 0,
     *   "warningCount": 0,
     *   "issues": []
     * }
     * }</pre>
     */
    @GetMapping(value = "/validation", produces = "application/json")
    public Map<String, Object> validation() {
        var result     = validator.validateAll(registry.list());
        long errors    = issueCount(result.issues(), ContractValidationSeverity.ERROR);
        long warnings  = issueCount(result.issues(), ContractValidationSeverity.WARNING);
        return Map.of(
                "valid",        result.isValid(),
                "errorCount",   errors,
                "warningCount", warnings,
                "issues",       result.issues().stream().map(this::issueDetail).toList()
        );
    }

    /**
     * Returns full detail for the named contract, or {@code 404} if not found.
     *
     * <p>Note: Spring MVC resolves the literal {@code /validation} mapping before this
     * path-variable mapping, so {@code GET /api/semantic/contracts/validation} is
     * always handled by {@link #validation()}.
     *
     * <p>Response shape (found):
     * <pre>{@code
     * {
     *   "name": "mxl_risk", "version": "1.0.0", "description": "...",
     *   "measureCount": 2, "dimensionCount": 3,
     *   "cacheStrategy": "TTL", "ttlSeconds": 7200
     * }
     * }</pre>
     */
    @GetMapping(value = "/{name}", produces = "application/json")
    public ResponseEntity<?> getByName(@PathVariable String name) {
        return registry.findByName(name)
                .<ResponseEntity<?>>map(c -> ResponseEntity.ok(detail(c)))
                .orElseGet(() -> ResponseEntity.status(404)
                        .body(Map.of("error", "contract not found", "name", name)));
    }

    // ── private helpers ────────────────────────────────────────────────────────

    private Map<String, Object> summary(SemanticContract c) {
        return Map.of(
                "name",           c.name(),
                "version",        c.version().value(),
                "description",    c.description(),
                "measureCount",   c.measures().size(),
                "dimensionCount", c.dimensions().size()
        );
    }

    private Map<String, Object> detail(SemanticContract c) {
        // LinkedHashMap required because ttlSeconds may be null (Map.of forbids null values)
        var m = new LinkedHashMap<String, Object>();
        m.put("name",           c.name());
        m.put("version",        c.version().value());
        m.put("description",    c.description());
        m.put("measureCount",   c.measures().size());
        m.put("dimensionCount", c.dimensions().size());
        m.put("cacheStrategy",  c.cachePolicy().strategy().name());
        m.put("ttlSeconds",     c.cachePolicy().ttlSeconds());
        return m;
    }

    private Map<String, Object> issueDetail(ContractValidationIssue i) {
        var m = new LinkedHashMap<String, Object>();
        m.put("severity",     i.severity().name());
        m.put("code",         i.code());
        m.put("message",      i.message());
        m.put("contractName", i.contractName());
        if (i.location() != null) m.put("location", i.location());
        return m;
    }

    private static long issueCount(List<ContractValidationIssue> issues,
                                   ContractValidationSeverity severity) {
        return issues.stream().filter(i -> i.severity() == severity).count();
    }
}
