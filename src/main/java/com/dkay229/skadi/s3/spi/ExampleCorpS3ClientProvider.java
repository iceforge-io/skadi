package com.dkay229.skadi.s3.spi;

public class ExampleCorpS3ClientProvider implements S3ClientProvider {
    @Override
    public String id() {
        return "example-corp";
    }

    @Override
    public boolean supports(S3ClientContext context) {
        return "example-corp".equals(context.tags().get("corpProfile"));
    }

    @Override
    public software.amazon.awssdk.services.s3.S3Client s3Client(S3ClientContext ctx) {
        var builder = software.amazon.awssdk.services.s3.S3Client.builder();

        // ExampleCorp specific configuration
        ctx.region().ifPresent(r -> builder.region(software.amazon.awssdk.regions.Region.of(r)));
        ctx.endpointOverride().ifPresent(builder::endpointOverride);

        // Add any ExampleCorp specific settings here
        // Example: corp network stack (proxy/custom TLS/etc.)
        // b.httpClient(myCorpHttpClient());
        return builder.build();
    }
}
