package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

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
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:skadi;DB_CLOSE_DELAY=-1")) {
            try (Statement st = conn.createStatement()) {
                st.execute("create table t(id int primary key, v varchar(2000))");
                for (int i = 1; i <= 2000; i++) {
                    st.execute("insert into t(id, v) values (" + i + ", '" + "x".repeat(500) + "')");
                }
            }

            AwsSdkS3AccessLayer delegate = Mockito.mock(AwsSdkS3AccessLayer.class);
            when(delegate.putBytes(any(S3Models.ObjectRef.class), any(byte[].class), anyString(), anyMap()))
                    .thenReturn("etag");

            CachedAwsSdkS3AccessLayer cached = new CachedAwsSdkS3AccessLayer(
                    delegate,
                    "512MB",
                    cacheRoot.toString()
            );

            ResultSetToS3ChunkWriter writer = new ResultSetToS3ChunkWriter(cached);

            ResultSetToS3ChunkWriter.StreamOptions opt =
                    new ResultSetToS3ChunkWriter.StreamOptions(
                            1_000,                                  // jdbcFetchSize
                            1,                                      // uploadThreads (simpler to reason about)
                            8,                                      // maxInFlightChunks
                            256 * 1024 * 1024,                      // maxInFlightBytes: 256MB, >> any chunk
                            64 * 1024,                              // targetChunkBytes
                            true,                                   // compress
                            new ResultSetToS3ChunkWriter.JsonLinesRowEncoder(),
                            new ResultSetToS3ChunkWriter.DefaultManifestWriter(),
                            1,                                      // uploadRetries
                            5 * 1024 * 1024                         // multipartThresholdBytes 5MB
                    );

            ResultSetToS3ChunkWriter.S3WritePlan plan =
                    new ResultSetToS3ChunkWriter.S3WritePlan("test-bucket", "skadi/results", "run-1");

            ResultSetToS3ChunkWriter.S3ResultSetRef ref =
                    writer.write(conn, "select * from t order by id", plan, opt);

            S3Models.ObjectRef manifestRef = new S3Models.ObjectRef(ref.bucket(), ref.manifestKey());
            assertTrue(Files.exists(expectedCachePath(cacheRoot, manifestRef)),
                    "Expected manifest to be cached locally");

            String ext = opt.rowEncoder().fileExtension(opt.compress());
            for (int part = 1; part <= ref.chunkCount(); part++) {
                S3Models.ObjectRef chunkRef = new S3Models.ObjectRef(
                        ref.bucket(),
                        ref.prefix() + "/" + ref.runId() + "/part-" + String.format("%06d", part) + ext
                );
                assertTrue(Files.exists(expectedCachePath(cacheRoot, chunkRef)),
                        "Expected chunk part " + part + " to be cached locally: " + chunkRef);
            }

            verify(delegate, atLeastOnce()).putBytes(any(S3Models.ObjectRef.class), any(byte[].class), anyString(), anyMap());
        }
    }


    private static Path expectedCachePath(Path cacheRoot, S3Models.ObjectRef ref) {
        String id = CacheKeyUtil.cacheId(ref.bucket(), ref.key());
        return cacheRoot.resolve(id.substring(0, 2)).resolve(id + ".bin");
    }
}
