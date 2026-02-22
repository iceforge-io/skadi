package org.iceforge.skadi.s3.spi;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "skadi.s3")
public class S3ProviderConfig {
    /**
     * Optional explicit provider id. If set, Skadi will use ONLY that provider id (or fail).
     * Example: "corp-aws"
     */
    private String provider;

    private String region;
    private URI endpointOverride;
    private Duration apiTimeout;

    /** Arbitrary selector tags passed to providers (corpAws=true, env=prod, etc.) */
    private Map<String, String> tags = new HashMap<>();

    public String getProvider() { return provider; }
    public void setProvider(String provider) { this.provider = provider; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    public URI getEndpointOverride() { return endpointOverride; }
    public void setEndpointOverride(URI endpointOverride) { this.endpointOverride = endpointOverride; }

    public Duration getApiTimeout() { return apiTimeout; }
    public void setApiTimeout(Duration apiTimeout) { this.apiTimeout = apiTimeout; }

    public Map<String, String> getTags() { return tags; }
    public void setTags(Map<String, String> tags) { this.tags = tags; }
}
