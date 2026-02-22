package org.iceforge.skadi.s3.spi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.util.*;
import java.util.stream.Collectors;

public final class S3ClientFactory {
    private static final Logger log = LoggerFactory.getLogger(S3ClientFactory.class);

    private final List<S3ClientProvider> providers;

    public S3ClientFactory(Collection<S3ClientProvider> springProviders) {
        // Prefer Spring beans if present, plus ServiceLoader providers.
        List<S3ClientProvider> fromSpring = springProviders == null ? List.of() : List.copyOf(springProviders);

        List<S3ClientProvider> fromServiceLoader = ServiceLoader.load(S3ClientProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();

        // Merge by id, Spring wins if same id
        Map<String, S3ClientProvider> merged = new LinkedHashMap<>();
        for (S3ClientProvider p : fromServiceLoader) merged.put(p.id(), p);
        for (S3ClientProvider p : fromSpring) merged.put(p.id(), p);

        this.providers = List.copyOf(merged.values());

        log.info("Discovered S3ClientProviders: {}", this.providers.stream()
                .map(S3ClientProvider::id).collect(Collectors.toList()));
    }

    public ResolvedS3 resolve(S3ProviderConfig cfg) {
        S3ClientContext ctx = new S3ClientContext(
                Optional.ofNullable(cfg.getRegion()),
                Optional.ofNullable(cfg.getEndpointOverride()),
                cfg.getTags() == null ? Map.of() : Map.copyOf(cfg.getTags()),
                Optional.ofNullable(cfg.getApiTimeout())
        );

        String forced = cfg.getProvider();
        if (forced != null && !forced.isBlank()) {
            S3ClientProvider p = providers.stream()
                    .filter(x -> forced.equals(x.id()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Forced S3 provider '" + forced + "' not found. Available: " + ids()));

            log.info("Using forced S3 provider id='{}' with ctx={}", p.id(), safeCtx(ctx));
            return new ResolvedS3(p.id(), p.s3Client(ctx), p.presigner(ctx).orElse(null));
        }

        List<S3ClientProvider> matching = providers.stream()
                .filter(p -> p.supports(ctx))
                .toList();

        if (matching.isEmpty()) {
            throw new IllegalStateException("No S3ClientProvider supports ctx=" + safeCtx(ctx) + " providers=" + ids());
        }

        // Deterministic tie-break: if multiple support, pick lexicographically by id.
        S3ClientProvider chosen = matching.stream()
                .sorted(Comparator.comparing(S3ClientProvider::id))
                .findFirst()
                .orElseThrow();

        log.info("Using S3 provider id='{}' (matched {}) with ctx={}",
                chosen.id(), matching.stream().map(S3ClientProvider::id).toList(), safeCtx(ctx));

        return new ResolvedS3(chosen.id(), chosen.s3Client(ctx), chosen.presigner(ctx).orElse(null));
    }

    private List<String> ids() {
        return providers.stream().map(S3ClientProvider::id).sorted().toList();
    }

    private static String safeCtx(S3ClientContext ctx) {
        // Avoid logging anything sensitive; ctx is already designed to be non-secret.
        return "region=" + ctx.region().orElse("<default>")
                + ", endpointOverride=" + ctx.endpointOverride().map(Object::toString).orElse("<none>")
                + ", tags=" + ctx.tags();
    }

    public record ResolvedS3(String providerId, S3Client s3, S3Presigner presignerOrNull) {}
}
