package org.iceforge.skadi.sqlgateway.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.iceforge.skadi.sqlgateway.executor.DatabricksJdbcExecutor;
import org.iceforge.skadi.sqlgateway.executor.SqlExecutor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;

@Configuration
@EnableConfigurationProperties(DatabricksProperties.class)
public class DatabricksDataSourceConfig {

    @Bean
    @ConditionalOnProperty(prefix = "skadi.sql-gateway.databricks", name = "enabled", havingValue = "true")
    public DataSource databricksDataSource(DatabricksProperties props) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName("dbx-warehouse");
        cfg.setJdbcUrl(jdbcUrl(props));
        cfg.setMaximumPoolSize(props.maxPoolSize() <= 0 ? 5 : props.maxPoolSize());

        Duration ct = props.connectTimeout() == null ? Duration.ofSeconds(10) : props.connectTimeout();
        cfg.setConnectionTimeout(ct.toMillis());

        // Databricks recommends token auth via UID/PWD; avoid logging token.
        cfg.setUsername("token");
        cfg.setPassword(props.token());

        return new HikariDataSource(cfg);
    }

    @Bean
    @ConditionalOnProperty(prefix = "skadi.sql-gateway.databricks", name = "enabled", havingValue = "true")
    public SqlExecutor databricksSqlExecutor(DataSource databricksDataSource) {
        return new DatabricksJdbcExecutor(databricksDataSource);
    }

    private static String jdbcUrl(DatabricksProperties props) {
        String host = props.host();
        String httpPath = props.httpPath();

        StringJoiner sj = new StringJoiner(";");
        sj.add("jdbc:databricks://" + host + ":443");
        if (httpPath != null && !httpPath.isBlank()) {
            sj.add("HttpPath=" + httpPath);
        }
        sj.add("AuthMech=3");
        sj.add("SSL=1");

        Map<String, String> extras = props.jdbcProperties();
        if (extras != null) {
            extras.forEach((k, v) -> {
                if (k != null && !k.isBlank() && v != null) {
                    sj.add(k + "=" + v);
                }
            });
        }
        return sj.toString();
    }
}

