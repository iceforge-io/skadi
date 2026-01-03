package com.dkay229.skadi.s3.spi;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.util.Optional;

/**
 * Pluggable “S3 client provider” interface, so Skadi never “knows” how credentials/roles/proxies/VPC endpoints are obtained. It just asks: “give me an S3Client (and optionally an S3Presigner) configured for this environment.”
 * <br>
 * Implementations supply fully-configured AWS SDK clients for the current runtime.
 * Skadi treats these as opaque and just uses them.
 * <br>
 * Define a very small SPI (plugin interface)
 *<br>
 * Make this live in a tiny skadi-s3-spi module (so corporate teams can depend on it without pulling all of Skadi).
 */
public interface S3ClientProvider {

    /** A stable ID for logging/diagnostics (e.g., "default", "corp-iam", "corp-bridge"). */
    String id();

    /** Return true if this provider should be used for the given context. */
    boolean supports(S3ClientContext context);

    /** Create or return an S3Client. Provider owns its caching/lifecycle strategy. */
    S3Client s3Client(S3ClientContext context);

    /** Optional: if Skadi uses presigned URLs. */
    default Optional<S3Presigner> presigner(S3ClientContext context) {
        return Optional.empty();
    }
}
