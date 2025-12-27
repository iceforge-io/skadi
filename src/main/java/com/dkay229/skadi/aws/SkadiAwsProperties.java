package com.dkay229.skadi.aws;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "skadi.aws")
public record SkadiAwsProperties(
        String region,
        S3Properties s3
) {
    public record S3Properties(
            String endpoint,
            boolean pathStyleAccess
    ) {}
}

