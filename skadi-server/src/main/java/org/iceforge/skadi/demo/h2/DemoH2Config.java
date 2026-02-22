package org.iceforge.skadi.demo.h2;

import org.h2.jdbcx.JdbcDataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

@Configuration
@ConditionalOnProperty(prefix = "skadi.demo.h2", name = "enabled", havingValue = "true")
@EnableConfigurationProperties(DemoH2Properties.class)
public class DemoH2Config {

    @Bean(name = "demoH2DataSource")
    public DataSource demoH2DataSource(DemoH2Properties props) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(DemoH2Util.jdbcUrl(props));
        ds.setUser(props.getUsername());
        ds.setPassword(props.getPassword());
        return ds;
    }

    @Bean
    public DemoH2Service demoH2Service(DataSource demoH2DataSource, DemoH2Properties props) {
        return new DemoH2Service(demoH2DataSource, props);
    }
}
