// java
package org.iceforge.skadi.s3.spi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class S3ClientFactoryTest {

    S3ClientProvider providerA;
    S3ClientProvider providerB;
    S3ClientFactory factory;

    @BeforeEach
    void setUp() {
        providerA = mock(S3ClientProvider.class);
        when(providerA.id()).thenReturn("providerA");
        when(providerA.supports(any())).thenReturn(true);
        when(providerA.s3Client(any())).thenReturn(mock(S3Client.class));
        when(providerA.presigner(any())).thenReturn(Optional.of(mock(S3Presigner.class)));

        providerB = mock(S3ClientProvider.class);
        when(providerB.id()).thenReturn("providerB");
        when(providerB.supports(any())).thenReturn(true);
        when(providerB.s3Client(any())).thenReturn(mock(S3Client.class));
        when(providerB.presigner(any())).thenReturn(Optional.empty());

        factory = new S3ClientFactory(List.of(providerA, providerB));
    }

    @Test
    void forcedProviderIsUsed() {
        S3ProviderConfig cfg = new S3ProviderConfig();
        cfg.setProvider("providerB");
        cfg.setRegion("us-east-1");
        cfg.setEndpointOverride(URI.create("http://localhost:9000"));
        cfg.setTags(Map.of("env", "test"));
        cfg.setApiTimeout(Duration.ofSeconds(5));

        S3ClientFactory.ResolvedS3 resolved = factory.resolve(cfg);

        assertEquals("providerB", resolved.providerId());
        verify(providerB, times(1)).s3Client(any(S3ClientContext.class));
        verify(providerB, times(1)).presigner(any(S3ClientContext.class));
        verify(providerA, never()).s3Client(any());
    }

    @Test
    void picksLexicographicallyWhenMultipleSupport() {
        S3ProviderConfig cfg = new S3ProviderConfig(); // no forced provider
        cfg.setRegion("us-west-2");

        S3ClientFactory.ResolvedS3 resolved = factory.resolve(cfg);

        assertEquals("providerA", resolved.providerId());
        verify(providerA, times(1)).s3Client(any(S3ClientContext.class));
        verify(providerB, never()).s3Client(any());
    }

    @Test
    void throwsWhenNoProvidersSupport() {
        S3ClientProvider rejecting = mock(S3ClientProvider.class);
        when(rejecting.id()).thenReturn("rejecting");
        when(rejecting.supports(any())).thenReturn(false);

        S3ClientFactory localFactory = new S3ClientFactory(List.of(rejecting));
        S3ProviderConfig cfg = new S3ProviderConfig();

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> localFactory.resolve(cfg));
        assertTrue(ex.getMessage().contains("No S3ClientProvider supports"));
    }

    @Test
    void nullPresignerAllowed() {
        S3ProviderConfig cfg = new S3ProviderConfig();
        cfg.setProvider("providerB");

        S3ClientFactory.ResolvedS3 resolved = factory.resolve(cfg);

        assertNull(resolved.presignerOrNull());
    }
}