package org.iceforge.skadi.sqlgateway.executor;

import org.iceforge.skadi.sqlgateway.SkadiSqlGatewayApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = SkadiSqlGatewayApplication.class,
        properties = {
                "skadi.sql-gateway.databricks.enabled=true",
                "skadi.sql-gateway.databricks.host=example.databricks.com",
                "skadi.sql-gateway.databricks.http-path=/sql/1.0/warehouses/abc",
                "skadi.sql-gateway.databricks.token=dummy",
                "skadi.sql-gateway.databricks.max-pool-size=1"
        }
)
class DatabricksJdbcExecutorWiringTest {

    @Test
    void executorBeanIsCreatedWhenEnabled(ApplicationContext ctx) {
        assertThat(ctx.getBeansOfType(SqlExecutor.class)).isNotEmpty();
    }
}

