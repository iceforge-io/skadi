package org.iceforge.skadi.semantic;

import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.iceforge.skadi.semantic.demo.DirectJdbcSemanticDemoExecutor;
import org.iceforge.skadi.semantic.demo.SemanticDemoSqlRenderer;
import org.iceforge.skadi.semantic.service.QueryExecutionService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Spring configuration for the demo semantic query pipelines.
 *
 * <p>Wires:
 * <ul>
 *   <li>{@link DemoSemanticQueryExecutionFacade} — plain-English demo
 *       ({@code POST /api/semantic/v1/query/demo/execute})</li>
 *   <li>{@link DirectJdbcSemanticDemoExecutor} — structured JDBC demo
 *       ({@code POST /api/semantic/v1/demo/query})</li>
 * </ul>
 *
 * <p>Both endpoints return HTTP 404 when {@code skadi.semantic.demo.enabled=false}
 * (the default). No Databricks calls, SQL gateway changes, or LLM integrations
 * are introduced by this configuration.
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

    @Bean
    public SemanticDemoSqlRenderer semanticDemoSqlRenderer(DemoSemanticProperties props) {
        return SemanticDemoSqlRenderer.create(props.getMarketRiskView(), props.getMaxRows());
    }

    @Bean
    public DirectJdbcSemanticDemoExecutor directJdbcSemanticDemoExecutor(
            JdbcClientFactory factory,
            SemanticDemoSqlRenderer renderer,
            DemoSemanticProperties props) {
        int timeoutSeconds = props.getQueryTimeoutMs() > 0
                ? props.getQueryTimeoutMs() / 1000
                : 0;
        return DirectJdbcSemanticDemoExecutor.create(
                factory, renderer, props.getDatasourceId(), timeoutSeconds);
    }
}
