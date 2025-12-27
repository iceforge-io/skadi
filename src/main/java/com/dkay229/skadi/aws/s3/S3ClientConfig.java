package com.dkay229.skadi.aws.s3;

import com.dkay229.skadi.aws.SkadiAwsProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.net.URI;
/** * Configuration class for AWS S3 client and presigner.
 */
@Configuration
public class S3ClientConfig {
    @Value("${skadi.aws.region}")
    private String region;
    @Bean
    public S3Client s3Client(SkadiAwsProperties props) {
        S3ClientBuilder b = S3Client.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(props.region()))
                .serviceConfiguration(
                        S3Configuration.builder()
                                .pathStyleAccessEnabled(props.s3().pathStyleAccess())
                                .build()
                );

        if (props.s3().endpoint() != null && !props.s3().endpoint().isBlank()) {
            b = b.endpointOverride(URI.create(props.s3().endpoint()));
        }

        return b.build();
    }

    @Bean
    public S3Presigner s3Presigner(SkadiAwsProperties props) {
        S3Presigner.Builder b = S3Presigner.builder()
                .credentialsProvider(DefaultCredentialsProvider.create())
                .region(Region.of(props.region()));

        if (props.s3().endpoint() != null && !props.s3().endpoint().isBlank()) {
            b = b.endpointOverride(URI.create(props.s3().endpoint()));
        }

        return b.build();
    }
}
