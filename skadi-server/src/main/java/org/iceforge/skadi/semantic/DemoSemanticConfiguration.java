package org.iceforge.skadi.semantic;

import org.iceforge.skadi.semantic.service.QueryExecutionService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Spring configuration for the demo semantic query pipeline.
 *
 * <p>Wires {@link DemoSemanticQueryExecutionFacade} from {@link DemoSemanticProperties}
 * and the existing {@link QueryExecutionService} bean (provided by
 * {@link SemanticContractConfiguration}).
 *
 * <p>The {@link DemoSemanticQueryController} bean is picked up by Spring component
 * scanning and delegates to the facade.
 *
 * <p>The demo endpoint returns HTTP 404 when
 * {@code skadi.semantic.demo.enabled=false} (the default). No Databricks calls,
 * SQL gateway changes, or LLM integrations are introduced.
 */
@Configuration
@EnableConfigurationProperties(DemoSemanticProperties.class)
public class DemoSemanticConfiguration {

    @Bean
    public DemoSemanticQueryExecutionFacade demoSemanticQueryExecutionFacade(
            DemoSemanticProperties demoProps,
            QueryExecutionService  queryExecutionService) {
        return DemoSemanticQueryExecutionFacade.create(
                demoProps, queryExecutionService, Clock.systemDefaultZone());
    }
}
