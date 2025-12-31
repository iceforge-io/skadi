package com.dkay229.skadi.aws.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class CachedAwsSdkS3AccessLayerTest {

    private AwsSdkS3AccessLayer delegate;
    private PeerCacheClient peerClient;

    private CachedAwsSdkS3AccessLayer cachedLayer;

    @TempDir
    Path cacheDir;

    @BeforeEach
    void setUp() {
        delegate = mock(AwsSdkS3AccessLayer.class);
        peerClient = mock(PeerCacheClient.class);

        // Use the Spring-style ctor so we can also test peer-cache logic.
        cachedLayer = new CachedAwsSdkS3AccessLayer(delegate, peerClient);

        // Configure @Value fields manually, then call init()
        setField(cachedLayer, "cacheMaxSize", "10Mb");
        setField(cachedLayer, "cacheRootDir", cacheDir.toString());
        cachedLayer.init();
    }

    // ----------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------

    private static void setField(Object target, String fieldName, Object value) {
        try {
            Field f = target.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException("Failed setting field " + fieldName, e);
        }
    }

    private static Path expectedCachePath(Path root, S3Models.ObjectRef ref) {
        String id = CacheKeyUtil.cacheId(ref.bucket(), ref.key());
        return root.resolve(id.substring(0, 2)).resolve(id + ".bin");
    }

    private static Path expectedMetaPath(Path root, S3Models.ObjectRef ref) {
        Path bin = expectedCachePath(root, ref);
        String s = bin.toString();
        return Path.of(s.substring(0, s.length() - 4) + ".meta");
    }

    private static void seedLocalCache(Path root, S3Models.ObjectRef ref, byte[] bytes, String source) throws Exception {
        Path bin = expectedCachePath(root, ref);
        Path meta = expectedMetaPath(root, ref);

        Files.createDirectories(bin.getParent());
        Files.write(bin, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

        CacheEntryMeta m = new CacheEntryMeta(ref.bucket(), ref.key(), bytes.length, Instant.now(), source);
        Files.writeString(meta, CacheMetaCodec.encode(m), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
    }

    // ----------------------------------------------------------------------
    // Cache HIT / MISS
    // ----------------------------------------------------------------------

    @Test
    void getBytes_cacheHit_readsLocalAndDoesNotCallDelegate() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("hit-bucket", "hit-key");
        byte[] cachedData = "cached-data".getBytes(StandardCharsets.UTF_8);

        seedLocalCache(cacheDir, ref, cachedData, "TEST");

        byte[] result = cachedLayer.getBytes(ref);

        assertArrayEquals(cachedData, result);

        // Prove it was really a hit (no S3 fallback)
        verify(delegate, never()).getStream(any());
        verify(delegate, never()).getBytes(any());
    }

    @Test
    void getBytes_cacheMiss_streamsFromDelegate_writesBinAndMeta() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("miss-bucket", "miss-key");
        byte[] s3Data = "s3-data".getBytes(StandardCharsets.UTF_8);

        when(delegate.getStream(ref)).thenReturn(new ByteArrayInputStream(s3Data));

        byte[] result = cachedLayer.getBytes(ref);

        assertArrayEquals(s3Data, result);

        Path bin = expectedCachePath(cacheDir, ref);
        Path meta = expectedMetaPath(cacheDir, ref);

        assertTrue(Files.exists(bin), "Expected cached .bin file to exist after miss");
        assertTrue(Files.exists(meta), "Expected cached .meta file to exist after miss");

        // Delegate stream should be used exactly once
        verify(delegate, times(1)).getStream(ref);

        // Validate meta can be read and is consistent
        Optional<CacheEntryMeta> m = cachedLayer.readLocalMeta(ref);
        assertTrue(m.isPresent(), "Expected readLocalMeta to succeed after miss");
        assertEquals(ref.bucket(), m.get().bucket());
        assertEquals(ref.key(), m.get().key());
        assertEquals(s3Data.length, m.get().sizeBytes());
        assertEquals("S3", m.get().source());
    }

    @Test
    void localPathIfCached_returnsEmptyWhenNotCached() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("nope-bucket", "nope-key");
        assertTrue(cachedLayer.localPathIfCached(ref).isEmpty());
    }

    @Test
    void localPathIfCached_returnsPathWhenCached() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("local-bucket", "local-key");
        seedLocalCache(cacheDir, ref, "x".getBytes(StandardCharsets.UTF_8), "TEST");

        Optional<Path> p = cachedLayer.localPathIfCached(ref);

        assertTrue(p.isPresent());
        assertEquals(expectedCachePath(cacheDir, ref), p.get());
    }

    // ----------------------------------------------------------------------
    // Delete
    // ----------------------------------------------------------------------

    @Test
    void delete_removesLocalBinAndMeta_andCallsDelegateDelete() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("del-bucket", "del-key");
        byte[] bytes = "to-delete".getBytes(StandardCharsets.UTF_8);

        // create via miss path to mirror real behavior
        when(delegate.getStream(ref)).thenReturn(new ByteArrayInputStream(bytes));
        assertArrayEquals(bytes, cachedLayer.getBytes(ref));

        Path bin = expectedCachePath(cacheDir, ref);
        Path meta = expectedMetaPath(cacheDir, ref);
        assertTrue(Files.exists(bin));
        assertTrue(Files.exists(meta));

        cachedLayer.delete(ref);

        assertFalse(Files.exists(bin), "Expected cached .bin to be deleted");
        assertFalse(Files.exists(meta), "Expected cached .meta to be deleted");

        verify(delegate, times(1)).delete(ref);
    }

    // ----------------------------------------------------------------------
    // Eviction
    // ----------------------------------------------------------------------

    @Test
    void eviction_whenCacheExceedsCapacity_evictsOldestBinAndMeta() throws Exception {
        // Create a tiny cache instance so we can force eviction quickly
        CachedAwsSdkS3AccessLayer tiny = new CachedAwsSdkS3AccessLayer(delegate, peerClient);
        setField(tiny, "cacheMaxSize", "9"); // 9 bytes
        setField(tiny, "cacheRootDir", cacheDir.toString());
        tiny.init();

        S3Models.ObjectRef ref1 = new S3Models.ObjectRef("evict-bucket", "k1");
        S3Models.ObjectRef ref2 = new S3Models.ObjectRef("evict-bucket", "k2");

        byte[] data1 = "12345".getBytes(StandardCharsets.UTF_8); // 5 bytes
        byte[] data2 = "67890".getBytes(StandardCharsets.UTF_8); // 5 bytes => total 10 > 9 => evict oldest

        when(delegate.getStream(ref1)).thenReturn(new ByteArrayInputStream(data1));
        when(delegate.getStream(ref2)).thenReturn(new ByteArrayInputStream(data2));

        assertArrayEquals(data1, tiny.getBytes(ref1));

        // Make ref1 definitively "older" than ref2 for eviction order
        Path bin1 = expectedCachePath(cacheDir, ref1);
        Path meta1 = expectedMetaPath(cacheDir, ref1);
        Files.setLastModifiedTime(bin1, FileTime.from(Instant.now().minusSeconds(10)));
        Files.setLastModifiedTime(meta1, FileTime.from(Instant.now().minusSeconds(10)));

        assertArrayEquals(data2, tiny.getBytes(ref2));

        Path bin2 = expectedCachePath(cacheDir, ref2);
        Path meta2 = expectedMetaPath(cacheDir, ref2);

        assertTrue(Files.exists(bin2), "Newest .bin should remain");
        assertTrue(Files.exists(meta2), "Newest .meta should remain");

        assertFalse(Files.exists(bin1), "Oldest .bin should be evicted");
        assertFalse(Files.exists(meta1), "Oldest .meta should be evicted");
    }

    // ----------------------------------------------------------------------
    // Peer cache path
    // ----------------------------------------------------------------------

    @Test
    void getBytes_peerHit_pullsFromPeer_writesLocalBinAndMeta_withoutCallingDelegate() {
        // Enable peer cache in this instance
        setField(cachedLayer, "peerEnabled", true);
        setField(cachedLayer, "peerBaseUrls", List.of("http://peer-a", "http://peer-b"));
        setField(cachedLayer, "maxPeersToTry", 1);
        setField(cachedLayer, "headTimeoutMs", 250L);
        setField(cachedLayer, "getTimeoutMs", 8000L);

        setField(cachedLayer, "peerKeyId", "k1");
        setField(cachedLayer, "peerSecrets", Map.of("k1", "secret1"));

        S3Models.ObjectRef ref = new S3Models.ObjectRef("peer-bucket", "peer-key");
        byte[] peerBytes = "from-peer".getBytes(StandardCharsets.UTF_8);

        // HEAD reports size, then GET streams to tmp path
        when(peerClient.headLength(eq("http://peer-a"), eq(ref.bucket()), eq(ref.key()), any(), any()))
                .thenReturn(Optional.of((long) peerBytes.length));

        when(peerClient.streamToFile(eq("http://peer-a"), eq(ref.bucket()), eq(ref.key()), any(), any(Path.class), any()))
                .thenAnswer(inv -> {
                    Path tmp = inv.getArgument(4, Path.class);
                    Files.createDirectories(tmp.getParent());
                    Files.write(tmp, peerBytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                    return true;
                });

        byte[] result = cachedLayer.getBytes(ref);

        assertArrayEquals(peerBytes, result);

        // Should have cached locally
        Path bin = expectedCachePath(cacheDir, ref);
        Path meta = expectedMetaPath(cacheDir, ref);
        assertTrue(Files.exists(bin), "Expected local .bin after peer pull");
        assertTrue(Files.exists(meta), "Expected local .meta after peer pull");

        // Meta should show PEER source
        Optional<CacheEntryMeta> m = cachedLayer.readLocalMeta(ref);
        assertTrue(m.isPresent());
        assertTrue(m.get().source().startsWith("PEER:"), "Expected meta source to start with PEER:");

        // Critical: peer hit must not fall back to S3 delegate
        verify(delegate, never()).getStream(any());
        verify(delegate, never()).getBytes(any());
    }

    @Test
    void getBytes_peerHeadOk_butStreamFails_fallsBackToDelegate() {
        setField(cachedLayer, "peerEnabled", true);
        setField(cachedLayer, "peerBaseUrls", List.of("http://peer-a"));
        setField(cachedLayer, "maxPeersToTry", 1);
        setField(cachedLayer, "peerKeyId", "k1");
        setField(cachedLayer, "peerSecrets", Map.of("k1", "secret1"));

        S3Models.ObjectRef ref = new S3Models.ObjectRef("peer-bucket", "peer-key2");
        byte[] s3Bytes = "from-s3".getBytes(StandardCharsets.UTF_8);

        when(peerClient.headLength(eq("http://peer-a"), eq(ref.bucket()), eq(ref.key()), any(), any()))
                .thenReturn(Optional.of(999L));

        when(peerClient.streamToFile(eq("http://peer-a"), eq(ref.bucket()), eq(ref.key()), any(), any(Path.class), any()))
                .thenReturn(false);

        when(delegate.getStream(ref)).thenReturn(new ByteArrayInputStream(s3Bytes));

        byte[] result = cachedLayer.getBytes(ref);

        assertArrayEquals(s3Bytes, result);
        verify(delegate, times(1)).getStream(ref);
    }

    // ----------------------------------------------------------------------
    // Delegate passthrough methods (non-caching)
    // ----------------------------------------------------------------------

    @Test
    void passthrough_methods_callDelegate() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        S3Models.ObjectRef ref2 = new S3Models.ObjectRef("bucket", "key2");

        when(delegate.exists(ref)).thenReturn(true);
        when(delegate.head(ref)).thenReturn(Optional.empty());
        when(delegate.putBytes(eq(ref), any(byte[].class), eq("text/plain"), anyMap())).thenReturn("etag-1");
        when(delegate.copy(ref, ref2)).thenReturn("etag-2");
        when(delegate.list("bucket", "pre", 10)).thenReturn(List.of());

        assertTrue(cachedLayer.exists(ref));
        assertTrue(cachedLayer.head(ref).isEmpty());
        assertEquals("etag-1", cachedLayer.putBytes(ref, "data".getBytes(StandardCharsets.UTF_8), "text/plain", Map.of()));
        assertEquals("etag-2", cachedLayer.copy(ref, ref2));
        assertNotNull(cachedLayer.list("bucket", "pre", 10));

        verify(delegate, times(1)).exists(ref);
        verify(delegate, times(1)).head(ref);
        verify(delegate, times(1)).putBytes(eq(ref), any(byte[].class), eq("text/plain"), anyMap());
        verify(delegate, times(1)).copy(ref, ref2);
        verify(delegate, times(1)).list("bucket", "pre", 10);
    }
}
