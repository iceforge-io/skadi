package org.iceforge.skadi.aws.s3;


import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class CacheMetadataControllerTest {

    private CachedAwsSdkS3AccessLayer cachedLayer;
    private CacheMetadataController controller;

    @BeforeEach
    void setUp() {
        cachedLayer = Mockito.mock(CachedAwsSdkS3AccessLayer.class);
        controller = new CacheMetadataController(cachedLayer);
    }

    @Test
    void testGetCacheMetadata() {
        // Arrange
        ConcurrentHashMap<Path, CacheMetadata> metadataMap = new ConcurrentHashMap<>();
        metadataMap.put(Path.of("key1"), new CacheMetadata());
        metadataMap.put(Path.of("key2"), new CacheMetadata());
        when(cachedLayer.getMetadataMap()).thenReturn(metadataMap);

        // Act
        Map<String, CacheMetadata> result = controller.getCacheMetadata();

        // Assert
        assertEquals(2, result.size());
        assertEquals(metadataMap.get(Path.of("key1")), result.get("key1"));
        assertEquals(metadataMap.get(Path.of("key2")), result.get("key2"));
        verify(cachedLayer, times(1)).getMetadataMap();
    }
}