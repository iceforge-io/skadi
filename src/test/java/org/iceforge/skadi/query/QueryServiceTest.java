package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.ResultSetToS3ChunkWriter;
import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.aws.s3.S3Models;
import org.iceforge.skadi.jdbc.SkadiJdbcProperties;
import org.iceforge.skadi.jdbc.spi.DefaultDriverManagerJdbcConnectionProvider;
import org.iceforge.skadi.jdbc.spi.JdbcClientFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

class QueryServiceTest {

    private final ExecutorService exec = Executors.newSingleThreadExecutor();

    @AfterEach
    void tearDown() {
        exec.shutdownNow();
    }

    @Test
    void submit_returnsHitWhenManifestExists() throws Exception {
        QueryCacheProperties props = new QueryCacheProperties();
        props.setBucket("test-bucket");
        props.setPrefix("results");

        ResultSetToS3ChunkWriter writer = Mockito.mock(ResultSetToS3ChunkWriter.class);
        S3AccessLayer s3 = Mockito.mock(S3AccessLayer.class);
        S3LockService lock = Mockito.mock(S3LockService.class);
        QueryRegistry registry = new QueryRegistry();
        ManifestReader mr = Mockito.mock(ManifestReader.class);

        JdbcClientFactory jdbcFactory = new JdbcClientFactory(new SkadiJdbcProperties(),
                List.of(new DefaultDriverManagerJdbcConnectionProvider()));

        QueryService svc = new QueryService(props, writer, s3, lock, registry, mr, exec, jdbcFactory);

        QueryModels.QueryRequest req = new QueryModels.QueryRequest(
                "my-key",
                new QueryModels.QueryRequest.Jdbc("jdbc:h2:mem:x", null, null, "select 1", List.of(), null, null),
                new QueryModels.QueryRequest.Format("ndjson", true),
                null,
                null
        );

        String qid = QueryKeyUtil.queryId(req);
        S3Models.ObjectRef manifestRef = new S3Models.ObjectRef("test-bucket", "results/" + qid + "/manifest.json");
        Mockito.when(s3.exists(manifestRef)).thenReturn(true);

        ResultSetToS3ChunkWriter.Manifest m = new ResultSetToS3ChunkWriter.Manifest(
                qid, "test-bucket", "results", true, 10L, 100L,
                List.of(new ResultSetToS3ChunkWriter.ChunkDescriptor(1, "k1", 1L, 1L, "e"))
        );
        Mockito.when(mr.read("test-bucket", "results/" + qid + "/manifest.json")).thenReturn(m);

        QueryModels.QueryResponse resp = svc.submit(req);
        assertEquals(QueryModels.Status.HIT, resp.status());
        assertEquals(qid, resp.queryId());
        assertNotNull(resp.ref());
        assertEquals(10L, resp.ref().rowCount());
        assertEquals(1, resp.ref().chunkCount());
    }

    @Test
    void submit_returnsRunningWhenLockNotAcquired() throws Exception {
        QueryCacheProperties props = new QueryCacheProperties();
        props.setBucket("test-bucket");
        props.setPrefix("results");

        ResultSetToS3ChunkWriter writer = Mockito.mock(ResultSetToS3ChunkWriter.class);
        S3AccessLayer s3 = Mockito.mock(S3AccessLayer.class);
        S3LockService lock = Mockito.mock(S3LockService.class);
        QueryRegistry registry = new QueryRegistry();
        ManifestReader mr = Mockito.mock(ManifestReader.class);

        JdbcClientFactory jdbcFactory = new JdbcClientFactory(new SkadiJdbcProperties(),
                List.of(new DefaultDriverManagerJdbcConnectionProvider()));

        QueryService svc = new QueryService(props, writer, s3, lock, registry, mr, exec, jdbcFactory);

        QueryModels.QueryRequest req = new QueryModels.QueryRequest(
                "my-key",
                new QueryModels.QueryRequest.Jdbc("jdbc:h2:mem:x", null, null, "select 1", List.of(), null, null),
                new QueryModels.QueryRequest.Format("ndjson", true),
                null,
                null
        );

        String qid = QueryKeyUtil.queryId(req);
        Mockito.when(s3.exists(new S3Models.ObjectRef("test-bucket", "results/" + qid + "/manifest.json"))).thenReturn(false);
        Mockito.when(lock.tryAcquire(Mockito.eq("test-bucket"), Mockito.anyString(), Mockito.anyString(), Mockito.anyLong())).thenReturn(false);

        QueryModels.QueryResponse resp = svc.submit(req);
        assertEquals(QueryModels.Status.RUNNING, resp.status());
        assertEquals(qid, resp.queryId());
    }
}
