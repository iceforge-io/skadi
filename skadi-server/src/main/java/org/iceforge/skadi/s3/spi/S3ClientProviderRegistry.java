package org.iceforge.skadi.s3.spi;

import software.amazon.awssdk.services.s3.S3Client;

import java.util.Comparator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Load providers via Java ServiceLoader (true plugin)
 *<br>
 * Corporate teams can ship a separate JAR with their implementation, and drop it on the classpath.
 * <br>
 * Corporate plugin JAR includes:
 *<pre>
 * META-INF/services/com.dkay229.skadi.s3.spi.S3ClientProvider
 *</pre>
 * with one line:
 *<pre>
 * com.mycorp.aws.MyCorpS3ClientProvider
 *</pre>
 * That’s the standard, clean “SPI plugin” approach.
 */

public final class S3ClientProviderRegistry {

    private final List<S3ClientProvider> providers;

    public S3ClientProviderRegistry() {
        this.providers = ServiceLoader.load(S3ClientProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();
    }

    public S3Client resolve(S3ClientContext ctx) {
        return providers.stream()
                .filter(p -> p.supports(ctx))
                // optional: deterministic tie-breaker
                .sorted(Comparator.comparing(S3ClientProvider::id))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "No S3ClientProvider found for context: " + ctx + " providers=" + providers))
                .s3Client(ctx);
    }
}
