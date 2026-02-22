// java
package org.iceforge.skadi.s3.spi;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class S3ClientProviderContractTest {

    @Test
    void presigner_returns_empty_by_default() {
        var provider = new DefaultAwsS3ClientProvider();
        var ctx = Mockito.mock(S3ClientContext.class);
        assertTrue(provider.presigner(ctx).isEmpty());
    }

    @Test
    void s3Client_is_non_null_and_can_be_requested_multiple_times() {
        var provider = new DefaultAwsS3ClientProvider();
        var ctx = Mockito.mock(S3ClientContext.class);

        when(ctx.region()).thenReturn(Optional.empty());
        when(ctx.endpointOverride()).thenReturn(Optional.empty());

        S3Client first = provider.s3Client(ctx);
        S3Client second = provider.s3Client(ctx);

        assertNotNull(first);
        assertNotNull(second);
    }
}