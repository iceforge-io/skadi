package com.dkay229.skadi.aws.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ResultSetToS3ChunkWriterLocalCacheTest {

    @TempDir
    Path cacheRoot;

    @Test
    void writerShouldWarmLocalCacheForManifestAndAllChunks() throws Exception {
        // --- arrange: in-memory DB with enough rows to produce multiple chunks (or at least 1)
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:skadi;DB_CLOSE_DELAY=-1")) {
            try (Statement st = conn.createStatement()) {
                st.execute("create table t(id int primary key, v varchar(2000))");
                // Insert rows with some payload so chunks are non-trivial
                for (int i = 1; i <= 2000; i++) {
                    st.execute("insert into t(id, v) values (" + i + ", '" + "x".repeat(500) + "')");
                }
            }

            // --- arrange: mock delegate S3 layer
            AwsSdkS3AccessLayer delegate = mock(AwsSdkS3AccessLayer.class, withSettings().lenient());

            // Cached layer uses delegate.putBytes(...) and returns the etag.
            when(delegate.putBytes(any(S3Models.ObjectRef.class), any(byte[].class), anyString(), anyMap()))
                    .thenReturn("etag");

            // Use a large cache max size so eviction doesn't interfere with the test.
            CachedAwsSdkS3AccessLayer cached = new CachedAwsSdkS3AccessLayer(
                    delegate,
                    "512MB",
                    cacheRoot.toString()
            );

            // Writer depends on S3AccessLayer (your updated code)
            ResultSetToS3ChunkWriter writer = new ResultSetToS3ChunkWriter(cached);

            // Use smaller chunks so we are likely to get multiple parts quickly
            ResultSetToS3ChunkWriter.StreamOptions opt = new ResultSetToS3ChunkWriter.StreamOptions(
                    1000,          // jdbcFetchSize
                    2,             // uploadThreads
                    8,             // maxInFlightChunks
                    32 * 1024 * 1024, // maxInFlightBytes
                    64 * 1024,     // targetChunkBytes (64KB => multiple chunks likely)
                    true,          // compress
                    new ResultSetToS3ChunkWriter.JsonLinesRowEncoder(),
                    new ResultSetToS3ChunkWriter.DefaultManifestWriter()
            );

            ResultSetToS3ChunkWriter.S3WritePlan plan =
                    new ResultSetToS3ChunkWriter.S3WritePlan("test-bucket", "skadi/results", "run-1");

            // --- act
            ResultSetToS3ChunkWriter.S3ResultSetRef ref =
                    writer.write(conn, "select * from t order by id", plan, opt);

            // --- assert: manifest is cached locally
            S3Models.ObjectRef manifestRef = new S3Models.ObjectRef(ref.bucket(), ref.manifestKey());
            assertTrue(Files.exists(expectedCachePath(cacheRoot, manifestRef)),
                    "Expected manifest to be cached locally");

            // --- assert: each chunk is cached locally
            // Keys are deterministic based on plan/runId/part and encoder extension.
            String ext = opt.rowEncoder().fileExtension(opt.compress());
            for (int part = 1; part <= ref.chunkCount(); part++) {
                S3Models.ObjectRef chunkRef = new S3Models.ObjectRef(
                        ref.bucket(),
                        ref.prefix() + "/" + ref.runId() + "/part-" + String.format("%06d", part) + ext
                );

                assertTrue(Files.exists(expectedCachePath(cacheRoot, chunkRef)),
                        "Expected chunk part " + part + " to be cached locally: " + chunkRef);
            }

            // --- optional: verify S3 PUTs happened (chunks + manifest)
            verify(delegate, atLeastOnce()).putBytes(any(S3Models.ObjectRef.class), any(byte[].class), anyString(), anyMap());
        }
    }

    private static Path expectedCachePath(Path cacheRoot, S3Models.ObjectRef ref) {
        String id = CacheKeyUtil.cacheId(ref.bucket(), ref.key());
        return cacheRoot.resolve(id.substring(0, 2)).resolve(id + ".bin");
    }
}
