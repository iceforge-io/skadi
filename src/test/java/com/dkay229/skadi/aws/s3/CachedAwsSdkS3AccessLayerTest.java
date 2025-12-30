package com.dkay229.skadi.aws.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = {
        "skadi.local.cacheRootDir=test/cache/dir",
        "skadi.local.cacheMaxSize=1Mb"
})
class CachedAwsSdkS3AccessLayerTest {

    private AwsSdkS3AccessLayer mockDelegate;
    private CachedAwsSdkS3AccessLayer cachedLayer;
    private Path cacheDir;

    @BeforeEach
    void setUp() throws Exception {
        mockDelegate = mock(AwsSdkS3AccessLayer.class);
        cacheDir = Files.createTempDirectory("cache");
        cachedLayer = new CachedAwsSdkS3AccessLayer(mockDelegate,"10Mb",cacheDir.toString()); // 1 MB max capacity
    }

    @Test
    void testGetBytes_CacheHit() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("testGetBytes_CacheHit-bucket", "test-key");
        Path cacheFile = cacheDir.resolve("test-bucket_test-key");
        byte[] cachedData = "cached-data".getBytes();
        Files.write(cacheFile, cachedData);
        when(mockDelegate.getBytes(ref)).thenReturn("cached-data".getBytes());
        byte[] result = cachedLayer.getBytes(ref);

        assertArrayEquals(cachedData, result);
    }

    @Test
    void testGetBytes_CacheMiss() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("test-bucket", "test-key2");
        byte[] s3Data = "s3-data".getBytes();
        when(mockDelegate.getBytes(any())).thenReturn(s3Data);

        byte[] result = cachedLayer.getBytes(ref);

        assertArrayEquals(s3Data, result);
        assertTrue(Files.exists(cacheDir.resolve("test-bucket_test-key2")));
        verify(mockDelegate, times(1)).getBytes(ref);
    }

    @Test
    void testEviction_WhenCacheExceedsCapacity() throws Exception {
        String cacheDirPrefix = "testEviction_WhenCacheExceedsCapacity-cacheRoot";
        Path cacheDir2 = Files.createTempDirectory(cacheDirPrefix);
        CachedAwsSdkS3AccessLayer cachedLayer2 =new CachedAwsSdkS3AccessLayer(mockDelegate,"9",cacheDir2.toString()); // 10 bytes max capacity
        S3Models.ObjectRef ref1 = new S3Models.ObjectRef("test-bucket", "key3");
        S3Models.ObjectRef ref2 = new S3Models.ObjectRef("test-bucket", "key4");
        when(mockDelegate.getBytes(ref1)).thenReturn("data3".getBytes());
        when(mockDelegate.getBytes(ref2)).thenReturn("data4".getBytes());
        cachedLayer2.getBytes(ref1); // Cache "data1"
        cachedLayer2.getBytes(ref2); // Cache "data2", evict "data1"

        assertFalse(Files.exists(cacheDir2.resolve("test-bucket_key3")));
        assertTrue(Files.exists(cacheDir2.resolve("test-bucket_key4")));
    }

    @Test
    void testDelete_RemovesFromCache() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("test-bucket", "test-key");
        Path cacheFile = cacheDir.resolve("test-bucket_test-key");
        Files.write(cacheFile, "cached-data".getBytes());

        cachedLayer.delete(ref);

        assertFalse(Files.exists(cacheFile));
        verify(mockDelegate, times(1)).delete(ref);
    }

    @Test
    void testDelegateMethods() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("test-bucket", "test-key");
        cachedLayer.exists(ref);
        verify(mockDelegate, times(1)).exists(ref);

        cachedLayer.head(ref);
        verify(mockDelegate, times(1)).head(ref);

        cachedLayer.putBytes(ref, "data".getBytes(), "text/plain", Map.of());
        verify(mockDelegate, times(1)).putBytes(ref, "data".getBytes(), "text/plain", Map.of());

        cachedLayer.getStream(ref);
        verify(mockDelegate, times(1)).getStream(ref);
    }
}