package com.dkay229.skadi;

import com.dkay229.skadi.aws.SkadiAwsProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(SkadiAwsProperties.class)
public class SkadiApplication {

	public static void main(String[] args) {
		SpringApplication.run(SkadiApplication.class, args);
	}

}
