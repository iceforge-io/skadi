package com.dkay229.skadi.s3.spi;

import software.amazon.awssdk.services.s3.S3Client;

/** Default provider (ships with Skadi)
 * <br>
 * Uses standard AWS SDK credential resolution (environment, profile, EC2/ECS roles, etc).
 */
public final class DefaultAwsS3ClientProvider implements S3ClientProvider {
    @Override public String id() { return "default"; }

    /**
     * Corporate plugin can implement supports() to only activate when tags indicate corporate mode, or when a specific system property is present.
     * @param context
     * @return
     */
    @Override
    public boolean supports(S3ClientContext context) {
        // default provider supports everything unless a more specific one claims it
        return true;
    }

    @Override
    public S3Client s3Client(S3ClientContext ctx) {
        var b = S3Client.builder();
        ctx.region().ifPresent(r -> b.region(software.amazon.awssdk.regions.Region.of(r)));
        ctx.endpointOverride().ifPresent(b::endpointOverride);
        // optional timeouts go on the HTTP client / override config
        return b.build();
    }
}

