package org.iceforge.skadi.s3.spi;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * A context object so you can pass “what Skadi needs” without Skadi depending on corporate details.
 * <br>
 * <em>Key point:</em> the corporate plugin can interpret tags any way it wants (e.g., "corpProfile"="prod-grid"), but Skadi itself doesn’t.
 * @param region
 * @param endpointOverride
 * @param tags
 * @param apiTimeout
 */
public record S3ClientContext(
        Optional<String> region,          // sometimes provided, sometimes not
        Optional<URI> endpointOverride,   // for S3-compatible or private endpoints
        Map<String, String> tags,         // arbitrary selectors (env, app, etc.)
        Optional<Duration> apiTimeout
) {}
