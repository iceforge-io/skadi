package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.loader.ContractLoadException;
import org.iceforge.skadi.semantic.registry.ContractRegistry;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulationException;
import org.iceforge.skadi.semantic.registry.ContractRegistryPopulator;
import org.iceforge.skadi.semantic.service.RegistrySemanticContractResolver;
import org.iceforge.skadi.semantic.validation.ContractValidationSeverity;
import org.iceforge.skadi.semantic.validation.SemanticContractValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.List;

/**
 * Spring configuration for the semantic contract metadata feature (Lane D, D7).
 *
 * <p>Creates three beans:
 * <ul>
 *   <li>{@code semanticContractRegistry} — read-only {@link ContractRegistry};
 *       empty when the feature is disabled or no locations are configured</li>
 *   <li>{@code semanticContractValidator} — stateless {@link SemanticContractValidator}</li>
 *   <li>{@code semanticContractResolver} — {@link RegistrySemanticContractResolver}
 *       backed by the registry</li>
 * </ul>
 *
 * <p>Safe-defaults: if {@code skadi.semantic.contracts.enabled} is {@code false}
 * (the default) or {@code locations} is empty, the registry is empty and server
 * startup is unaffected. File-load or validation errors are logged and result in
 * an empty registry rather than a startup failure; this behaviour keeps the
 * metadata endpoint operational even when contract files are temporarily unavailable.
 */
@Configuration
@EnableConfigurationProperties(SemanticContractProperties.class)
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
}
