package org.iceforge.skadi.semantic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.jdbc.SkadiJdbcProperties;
import org.iceforge.skadi.semantic.loader.ContractLoadException;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulationException;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulator;
import org.iceforge.skadi.semantic.service.QueryExecutionService;
import org.iceforge.skadi.semantic.service.RegistrySemanticContractResolver;
import org.iceforge.skadi.semantic.service.SemanticExecutionCircuitBreaker;
import org.iceforge.skadi.semantic.service.SkadiServerQueryExecutionService;
import org.iceforge.skadi.semantic.validation.ContractValidationSeverity;
import org.iceforge.skadi.semantic.validation.SemanticContractValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Clock;
import java.util.List;

/**
 * Spring configuration for the semantic contract metadata and execution features.
 *
 * <p>Creates four beans:
 * <ul>
 *   <li>{@code semanticContractRegistry} — read-only {@link ContractRegistry};
 *       empty when the feature is disabled or no locations are configured</li>
 *   <li>{@code semanticContractValidator} — stateless {@link SemanticContractValidator}</li>
 *   <li>{@code semanticContractResolver} — {@link RegistrySemanticContractResolver}
 *       backed by the registry</li>
 *   <li>{@code queryExecutionService} — Lane E E1: {@link SkadiServerQueryExecutionService}
 *       that delegates semantic query execution to skadi-server's {@code POST /api/v1/queries}
 *       endpoint (DQR-002 resolved, Option 4 — partial convergence)</li>
 *   <li>{@code semanticExecutionMetricsRegistry} — Lane E E2: {@link SemanticExecutionMetricsRegistry}
 *       wired into the execution service; counters exposed via
 *       {@code GET /api/semantic/v1/execution/status}</li>
 * </ul>
 *
 * <p>Safe-defaults: if {@code skadi.semantic.contracts.enabled} is {@code false}
 * (the default) or {@code locations} is empty, the registry is empty and server
 * startup is unaffected. File-load or validation errors are logged and result in
 * an empty registry rather than a startup failure; this behaviour keeps the
 * metadata endpoint operational even when contract files are temporarily unavailable.
 */
@Configuration
@EnableConfigurationProperties({SemanticContractProperties.class, SemanticExecutionProperties.class})
public class SemanticContractConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SemanticContractConfiguration.class);

    @Bean
    public ContractRegistry semanticContractRegistry(SemanticContractProperties props) {
        var populator = ContractRegistryPopulator.create();

        if (!props.isEnabled() || props.getLocations().isEmpty()) {
            log.debug("skadi.semantic.contracts: disabled or no locations configured; using empty registry");
            return populator.populate(List.of());
        }

        var paths = props.getLocations().stream().map(Path::of).toList();
        log.info("skadi.semantic.contracts: loading {} contract file(s)", paths.size());

        try {
            var result = populator.populateFromPaths(paths);

            if (result.hasWarnings()) {
                long warnCount = result.validationResult().issues().stream()
                        .filter(i -> i.severity() == ContractValidationSeverity.WARNING).count();
                log.warn("skadi.semantic.contracts: {} warning(s) loading contracts; "
                        + "check /api/semantic/contracts/validation for details", warnCount);
            }

            log.info("skadi.semantic.contracts: {} contract(s) loaded", result.loadedContractCount());
            return result.registry();

        } catch (ContractRegistryPopulationException ex) {
            long errCount = ex.validationResult().issues().stream()
                    .filter(i -> i.severity() == ContractValidationSeverity.ERROR).count();
            log.error("skadi.semantic.contracts: {} validation error(s); starting with empty registry. "
                    + "Fix contract files and restart. Run GET /api/semantic/contracts/validation after restart.",
                    errCount, ex);
            return populator.populate(List.of());

        } catch (ContractLoadException ex) {
            log.error("skadi.semantic.contracts: file load failed ({}); starting with empty registry",
                    ex.source(), ex);
            return populator.populate(List.of());
        }
    }

    @Bean
    public SemanticContractValidator semanticContractValidator() {
        return SemanticContractValidator.create();
    }

    @Bean
    public RegistrySemanticContractResolver semanticContractResolver(
            ContractRegistry semanticContractRegistry) {
        return RegistrySemanticContractResolver.of(semanticContractRegistry);
    }

    /**
     * Lane E — Semantic execution activation (DQR-002, Option 4).
     *
     * <p>Wires {@link SkadiServerQueryExecutionService} to delegate semantic query execution
     * to skadi-server's {@code POST /api/v1/queries} endpoint. The JDBC credentials forwarded
     * with each request are resolved from the datasource identified by
     * {@code skadi.semantic.execution.datasource-id}.
     *
     * <p>If the configured datasource does not exist in {@code skadi.jdbc.datasources},
     * empty strings are substituted and the execution service will report a connection error
     * at runtime — server startup is not affected.
     */
    /**
     * Lane E E2 — Semantic execution metrics registry.
     */
    @Bean
    public SemanticExecutionMetricsRegistry semanticExecutionMetricsRegistry() {
        return new SemanticExecutionMetricsRegistry();
    }

    /**
     * Lane F F1 — Semantic execution circuit breaker.
     *
     * <p>Guards {@link SkadiServerQueryExecutionService} against cascading failures.
     * When {@code skadi.semantic.execution.enabled=false} the circuit breaker is created
     * in DISABLED state and all calls are blocked. When
     * {@code skadi.semantic.execution.circuit-breaker.enabled=false} the circuit breaker
     * records failures but never opens — effectively disabling the tripping behaviour.
     */
    @Bean
    public SemanticExecutionCircuitBreaker semanticExecutionCircuitBreaker(
            SemanticExecutionProperties execProps) {
        var cb = execProps.getCircuitBreaker();
        boolean cbEnabled = execProps.isEnabled() && cb.isEnabled();
        log.info("skadi.semantic.execution: circuit-breaker enabled={} threshold={} openDurationMs={}",
                cbEnabled, cb.getFailureThreshold(), cb.getOpenDurationMs());
        return new SemanticExecutionCircuitBreaker(
                cbEnabled,
                cb.getFailureThreshold(),
                cb.getOpenDurationMs(),
                execProps.getServerUrl(),
                Clock.systemUTC());
    }

    /**
     * Lane E E1 + F F1 — Semantic execution service with resilience.
     *
     * <p>Wires {@link SkadiServerQueryExecutionService} with the circuit breaker and metrics
     * registry. JDBC credentials are resolved from the configured datasource.
     */
    @Bean
    public QueryExecutionService queryExecutionService(
            SemanticExecutionProperties execProps,
            SkadiJdbcProperties jdbcProps,
            ObjectMapper objectMapper,
            SemanticExecutionMetricsRegistry metricsRegistry,
            SemanticExecutionCircuitBreaker circuitBreaker) {

        var datasource = jdbcProps.getDatasources().get(execProps.getDatasourceId());
        String jdbcUrl      = datasource != null ? datasource.getJdbcUrl()  : "";
        String jdbcUsername = datasource != null ? datasource.getUsername()  : null;
        String jdbcPassword = datasource != null ? datasource.getPassword()  : null;

        log.info("skadi.semantic.execution: wiring SkadiServerQueryExecutionService "
                + "-> {} (datasource-id={} enabled={})",
                execProps.getServerUrl(), execProps.getDatasourceId(), execProps.isEnabled());

        return new SkadiServerQueryExecutionService(
                execProps.getServerUrl(), jdbcUrl, jdbcUsername, jdbcPassword,
                objectMapper, metricsRegistry, circuitBreaker);
    }
}
