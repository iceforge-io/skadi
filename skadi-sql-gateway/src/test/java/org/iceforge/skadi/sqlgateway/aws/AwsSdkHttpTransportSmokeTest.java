package org.iceforge.skadi.sqlgateway.aws;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Smoke-test to ensure that excluding the AWS Netty HTTP client still leaves us with a usable
 * synchronous HTTP transport (Apache) on the classpath.
 *
 * <p>This avoids surprises at runtime when S3-backed cache is enabled.
 */
class AwsSdkHttpTransportSmokeTest {

    @Test
    void canCreateS3ClientWithApacheHttpClient() {
        try (SdkHttpClient http = ApacheHttpClient.builder().build();
             S3Client s3 = S3Client.builder()
                     .httpClient(http)
                     // Dummy endpoint; we're only validating wiring/classpath.
                     .endpointOverride(URI.create("http://localhost:0"))
                     .build()) {

            assertThat(s3).isNotNull();
        }
    }
}

