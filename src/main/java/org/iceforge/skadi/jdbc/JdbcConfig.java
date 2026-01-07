package org.iceforge.skadi.jdbc;

import org.iceforge.skadi.jdbc.spi.DefaultDriverManagerJdbcConnectionProvider;
import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.iceforge.skadi.jdbc.spi.JdbcConnectionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class JdbcConfig {

    /**
     * Default provider (DriverManager). Corporate deployments can override by providing their own beans.
     */
    @Bean
    public JdbcConnectionProvider defaultJdbcConnectionProvider() {
        return new DefaultDriverManagerJdbcConnectionProvider();
    }

    @Bean
    public JdbcClientFactory jdbcClientFactory(SkadiJdbcProperties props, List<JdbcConnectionProvider> providers) {
        return new JdbcClientFactory(props, providers);
    }
}
