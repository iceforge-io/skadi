package com.dkay229.skadi.s3.spi;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/** Default provider (ships with Skadi)
 * <br>
 * Uses standard AWS SDK credential resolution (environment, profile, EC2/ECS roles, etc).
 */
public final class DefaultAwsS3ClientProvider implements S3ClientProvider {
    private static final Region DEFAULT_REGION = Region.US_EAST_1;

    @Override public String id() { return "default"; }

    @Override
    public boolean supports(S3ClientContext context) {
        return true;
    }

    @Override
    public S3Client s3Client(S3ClientContext ctx) {
        var b = S3Client.builder();

        var region = ctx.region()
                .map(Region::of)
                .orElse(DEFAULT_REGION);

        b.region(region);
        ctx.endpointOverride().ifPresent(b::endpointOverride);

        return b.build();
    }
}
