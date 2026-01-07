package org.iceforge.skadi;

import org.iceforge.skadi.aws.SkadiAwsProperties;
import org.iceforge.skadi.jdbc.SkadiJdbcProperties;
import org.iceforge.skadi.query.QueryCacheProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties({SkadiAwsProperties.class, QueryCacheProperties.class, SkadiJdbcProperties.class})
public class SkadiApplication {

	public static void main(String[] args) {
		SpringApplication.run(SkadiApplication.class, args);
	}
}
