package org.iceforge.skadi.sqlgateway;

import org.iceforge.skadi.sqlgateway.config.SqlGatewayProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({SqlGatewayProperties.class})
public class SkadiSqlGatewayApplication {
  public static void main(String[] args) {
    SpringApplication.run(SkadiSqlGatewayApplication.class, args);
  }
}
