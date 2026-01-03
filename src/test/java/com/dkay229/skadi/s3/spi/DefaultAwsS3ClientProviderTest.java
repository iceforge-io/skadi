package com.dkay229.skadi.s3.spi;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class DefaultAwsS3ClientProviderTest {

    @Test
    void id_returns_default() {
        var provider = new DefaultAwsS3ClientProvider();
        assertEquals("default", provider.id());
    }

    @Test
    void supports_returns_true_for_any_context() {
        var provider = new DefaultAwsS3ClientProvider();
        var ctx = Mockito.mock(S3ClientContext.class);
        assertTrue(provider.supports(ctx));
    }

    @Test
    void s3Client_builds_with_no_overrides() {
        var provider = new DefaultAwsS3ClientProvider();
        var ctx = Mockito.mock(S3ClientContext.class);
        when(ctx.region()).thenReturn(Optional.empty());
        when(ctx.endpointOverride()).thenReturn(Optional.empty());

        S3Client client = provider.s3Client(ctx);
        assertNotNull(client);
    }

    @Test
    void s3Client_applies_region_and_endpoint_override() {
        var provider = new DefaultAwsS3ClientProvider();
        var ctx = Mockito.mock(S3ClientContext.class);
        when(ctx.region()).thenReturn(Optional.of("us-east-1"));
        when(ctx.endpointOverride()).thenReturn(Optional.of(URI.create("http://localhost:9000")));

        S3Client client = provider.s3Client(ctx);
        assertNotNull(client);
        // SDK v2 S3Client does not expose region/endpoint directly; ensure no exceptions and client is created
    }
}