package org.iceforge.skadi.aws.s3;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class CacheMetadataController {
    private final CachedAwsSdkS3AccessLayer cachedLayer;

    public CacheMetadataController(CachedAwsSdkS3AccessLayer cachedLayer) {
        this.cachedLayer = cachedLayer;
    }

    @GetMapping("/cache/metadata")
    public Map<String, CacheMetadata> getCacheMetadata() {
        return cachedLayer.getMetadataMap().entrySet().stream()
                .collect(Collectors.toMap(
                        entry -> entry.getKey().toString(),
                        Map.Entry::getValue
                ));
    }
}
