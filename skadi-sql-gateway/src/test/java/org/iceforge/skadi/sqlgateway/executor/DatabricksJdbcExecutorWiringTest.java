package org.iceforge.skadi.sqlgateway.executor;

import org.iceforge.skadi.sqlgateway.config.DatabricksDataSourceConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@SpringBootTest(
        properties = {
                "spring.main.allow-bean-definition-overriding=true",
                "skadi.sql-gateway.databricks.enabled=true",
                "skadi.sql-gateway.databricks.host=example.databricks.com",
                "skadi.sql-gateway.databricks.http-path=/sql/1.0/warehouses/abc",
                "skadi.sql-gateway.databricks.token=dummy",
                "skadi.sql-gateway.databricks.max-pool-size=1"
        },
        classes = {DatabricksDataSourceConfig.class, DatabricksJdbcExecutorWiringTest.TestConfig.class}
)
class DatabricksJdbcExecutorWiringTest {

    @TestConfiguration
    static class TestConfig {
        @Bean(name = "databricksDataSource")
        @Primary
        DataSource databricksDataSource() {
            return mock(DataSource.class);
        }
    }

    @Test
    void executorBeanIsCreatedWhenEnabled(ApplicationContext ctx) {
        Object bean = ctx.getBean("databricksSqlExecutor");
        assertThat(bean).isInstanceOf(DatabricksJdbcExecutor.class);
    }
}
