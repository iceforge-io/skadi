package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ResultSetToS3ChunkWriterLocalCacheTightTest {

    @TempDir
    Path cacheRoot;

    // -------------------------------------------------------------------------
    // Shared helpers
    // -------------------------------------------------------------------------

    private static Path expectedCachePath(Path cacheRoot, S3Models.ObjectRef ref) {
        String id = CacheKeyUtil.cacheId(ref.bucket(), ref.key());
        return cacheRoot.resolve(id.substring(0, 2)).resolve(id + ".bin");
    }

    private static Path expectedMetaPath(Path cacheRoot, S3Models.ObjectRef ref) {
        String id = CacheKeyUtil.cacheId(ref.bucket(), ref.key());
        return cacheRoot.resolve(id.substring(0, 2)).resolve(id + ".meta");
    }

    private static Connection makeDbWithRows(String dbName, int rows, int payloadChars) throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1");
        try (Statement st = conn.createStatement()) {
            st.execute("create table t(id int primary key, v varchar(2000))");
        }

        conn.setAutoCommit(false);
        try (PreparedStatement ps = conn.prepareStatement("insert into t(id, v) values (?, ?)")) {
            String payload = "x".repeat(payloadChars);
            for (int i = 1; i <= rows; i++) {
                ps.setInt(1, i);
                ps.setString(2, payload);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
        conn.setAutoCommit(true);
        return conn;
    }

    private static ResultSetToS3ChunkWriter.StreamOptions smallChunkOptions() {
        // Keep it deterministic and fast:
        // - 1 uploader thread
        // - tiny targetChunkBytes to force multiple chunks
        // - large maxInFlightBytes to prevent deadlock
        return new ResultSetToS3ChunkWriter.StreamOptions(
                500,               // jdbcFetchSize
                1,                 // uploadThreads
                8,                 // maxInFlightChunks
                128 * 1024 * 1024, // maxInFlightBytes (huge)
                4 * 1024,          // targetChunkBytes (4KB)
                true,              // compress
                new ResultSetToS3ChunkWriter.JsonLinesRowEncoder(),
                new ResultSetToS3ChunkWriter.DefaultManifestWriter(),
                3,
                5*1024 * 1024 // 5MB memory limit
        );
    }

    // -------------------------------------------------------------------------
    // 1) Assert .meta exists and says source=PUT
    // -------------------------------------------------------------------------

    @Test
    void writesMetaFilesWithPutSource() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            try (Connection conn = makeDbWithRows("skadi_meta", 120, 200)) {
                AwsSdkS3AccessLayer delegate = mock(AwsSdkS3AccessLayer.class);
                when(delegate.putBytes(any(), any(), anyString(), anyMap())).thenReturn("etag");

                CachedAwsSdkS3AccessLayer cached =
                        new CachedAwsSdkS3AccessLayer(delegate, "512MB", cacheRoot.toString());
                ResultSetToS3ChunkWriter writer = new ResultSetToS3ChunkWriter(cached);

                var opt = smallChunkOptions();
                var plan = new ResultSetToS3ChunkWriter.S3WritePlan("test-bucket", "skadi/results", "run-meta");

                ResultSetToS3ChunkWriter.S3ResultSetRef ref =
                        writer.write(conn, "select * from t order by id", plan, opt);

                // Manifest meta
                S3Models.ObjectRef manifestRef = new S3Models.ObjectRef(ref.bucket(), ref.manifestKey());
                Path manifestMetaPath = expectedMetaPath(cacheRoot, manifestRef);

                assertTrue(Files.exists(expectedCachePath(cacheRoot, manifestRef)), "manifest .bin missing");
                assertTrue(Files.exists(manifestMetaPath), "manifest .meta missing");

                CacheEntryMeta manifestMeta = CacheMetaCodec.decode(Files.readString(manifestMetaPath));
                assertEquals("PUT", manifestMeta.source());
                assertEquals(manifestRef.bucket(), manifestMeta.bucket());
                assertEquals(manifestRef.key(), manifestMeta.key());

                // One chunk meta (enough)
                String ext = opt.rowEncoder().fileExtension(opt.compress());
                S3Models.ObjectRef chunk1 = new S3Models.ObjectRef(
                        ref.bucket(),
                        ref.prefix() + "/" + ref.runId() + "/part-" + String.format("%06d", 1) + ext
                );
                Path chunk1MetaPath = expectedMetaPath(cacheRoot, chunk1);

                assertTrue(Files.exists(expectedCachePath(cacheRoot, chunk1)), "chunk1 .bin missing");
                assertTrue(Files.exists(chunk1MetaPath), "chunk1 .meta missing");

                CacheEntryMeta c1 = CacheMetaCodec.decode(Files.readString(chunk1MetaPath));
                assertEquals("PUT", c1.source());
                assertEquals(chunk1.bucket(), c1.bucket());
                assertEquals(chunk1.key(), c1.key());
            }
        });
    }

    // -------------------------------------------------------------------------
    // 2) Negative test: if S3 PUT fails, cache must NOT publish
    // -------------------------------------------------------------------------

    @Test
    void ifS3PutFails_cacheIsNotPublished() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            try (Connection conn = makeDbWithRows("skadi_fail", 5, 10)) {
                AwsSdkS3AccessLayer delegate = mock(AwsSdkS3AccessLayer.class);
                when(delegate.putBytes(any(), any(), anyString(), anyMap()))
                        .thenThrow(new RuntimeException("boom"));

                CachedAwsSdkS3AccessLayer cached =
                        new CachedAwsSdkS3AccessLayer(delegate, "512MB", cacheRoot.toString());
                ResultSetToS3ChunkWriter writer = new ResultSetToS3ChunkWriter(cached);

                var opt = smallChunkOptions();
                var plan = new ResultSetToS3ChunkWriter.S3WritePlan("test-bucket", "skadi/results", "run-fail");

                assertThrows(Exception.class, () ->
                        writer.write(conn, "select * from t order by id", plan, opt)
                );

                // Because the first PUT failed, nothing should have been published locally for manifest.
                S3Models.ObjectRef manifestRef = plan.manifestRef();
                assertTrue(Files.notExists(expectedCachePath(cacheRoot, manifestRef)),
                        "Manifest should not be cached if S3 PUT failed");
                assertTrue(Files.notExists(expectedMetaPath(cacheRoot, manifestRef)),
                        "Manifest meta should not be cached if S3 PUT failed");
            }
        });
    }

    // -------------------------------------------------------------------------
    // 3) Overwrite same key: ensure cache file size reflects new content and doesn't grow
    // -------------------------------------------------------------------------

    @Test
    void overwritingSameKeyUpdatesCacheFileSize() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            AwsSdkS3AccessLayer delegate = mock(AwsSdkS3AccessLayer.class);
            when(delegate.putBytes(any(), any(), anyString(), anyMap())).thenReturn("etag");

            CachedAwsSdkS3AccessLayer cached =
                    new CachedAwsSdkS3AccessLayer(delegate, "10MB", cacheRoot.toString());

            S3Models.ObjectRef ref = new S3Models.ObjectRef("b", "k");

            cached.putBytes(ref, "x".repeat(10_000).getBytes(), "text/plain", Map.of());
            long size1 = Files.size(expectedCachePath(cacheRoot, ref));

            cached.putBytes(ref, "x".repeat(10).getBytes(), "text/plain", Map.of());
            long size2 = Files.size(expectedCachePath(cacheRoot, ref));

            assertTrue(size1 > size2, "Expected overwrite with smaller content to reduce file size");
            assertEquals(10, size2, "Expected file size to match new bytes length");
        });
    }

    // -------------------------------------------------------------------------
    // 4) Peer-serving ready: controller can serve cached chunks
    // -------------------------------------------------------------------------

    @Test
    void peerControllerCanServeCachedObject() {
        assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
            try (Connection conn = makeDbWithRows("skadi_peer", 120, 200)) {
                AwsSdkS3AccessLayer delegate = mock(AwsSdkS3AccessLayer.class);
                when(delegate.putBytes(any(), any(), anyString(), anyMap())).thenReturn("etag");

                CachedAwsSdkS3AccessLayer cached =
                        new CachedAwsSdkS3AccessLayer(delegate, "512MB", cacheRoot.toString());
                ResultSetToS3ChunkWriter writer = new ResultSetToS3ChunkWriter(cached);

                var opt = smallChunkOptions();
                var plan = new ResultSetToS3ChunkWriter.S3WritePlan("test-bucket", "skadi/results", "run-peer");

                ResultSetToS3ChunkWriter.S3ResultSetRef ref =
                        writer.write(conn, "select * from t order by id", plan, opt);

                // Build ref for chunk 1 and prove it exists in cache
                String ext = opt.rowEncoder().fileExtension(opt.compress());
                S3Models.ObjectRef chunk1 = new S3Models.ObjectRef(
                        ref.bucket(),
                        ref.prefix() + "/" + ref.runId() + "/part-" + String.format("%06d", 1) + ext
                );
                Path chunk1Path = expectedCachePath(cacheRoot, chunk1);
                assertTrue(Files.exists(chunk1Path), "chunk1 not cached locally");

                // PeerAuth stub: allow all requests
                PeerAuth allowAll = new PeerAuth() {
                    @Override
                    public void verify(String method, String path, String query,
                                       String keyId, String ts, String nonce, String sigB64Url) {
                        // no-op
                    }
                };

                PeerCacheController controller = new PeerCacheController(cached, allowAll);

                // HEAD should return length
                ResponseEntity<Void> head = controller.head(
                        chunk1.bucket(), chunk1.key(),
                        null, null, null, null
                );
                assertEquals(200, head.getStatusCode().value());
                assertEquals(String.valueOf(Files.size(chunk1Path)),
                        head.getHeaders().getFirst("Content-Length"));

                // GET should return bytes
                ResponseEntity<InputStreamResource> get = controller.get(
                        chunk1.bucket(), chunk1.key(),
                        null, null, null, null
                );
                assertEquals(200, get.getStatusCode().value());
                assertNotNull(get.getBody());

                byte[] served;
                try (var in = get.getBody().getInputStream()) {
                    served = in.readAllBytes();
                }
                assertEquals(Files.size(chunk1Path), served.length);
            }
        });
    }

    // -------------------------------------------------------------------------
    // 5) Deadlock guard: invalid StreamOptions should fail fast
    // -------------------------------------------------------------------------

    @Test
    void invalidStreamOptionsFailFast() {
        assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            try (Connection conn = makeDbWithRows("skadi_opts", 5, 10)) {
                AwsSdkS3AccessLayer delegate = mock(AwsSdkS3AccessLayer.class);
                when(delegate.putBytes(any(), any(), anyString(), anyMap())).thenReturn("etag");

                CachedAwsSdkS3AccessLayer cached =
                        new CachedAwsSdkS3AccessLayer(delegate, "512MB", cacheRoot.toString());
                ResultSetToS3ChunkWriter writer = new ResultSetToS3ChunkWriter(cached);



                var plan = new ResultSetToS3ChunkWriter.S3WritePlan("test-bucket", "skadi/results", "run-bad");

                assertThrows(IllegalArgumentException.class, () ->
                        writer.write(conn, "select * from t", plan, new ResultSetToS3ChunkWriter.StreamOptions(
                                100,
                                1,
                                2,
                                1024,     // maxInFlightBytes
                                4096,     // targetChunkBytes (bigger!)
                                true,
                                new ResultSetToS3ChunkWriter.JsonLinesRowEncoder(),
                                new ResultSetToS3ChunkWriter.DefaultManifestWriter(),
                                3,
                                10*1024*1024
                        ))
                );
            }
        });
    }
}
