package org.iceforge.skadi.query;

import org.iceforge.skadi.api.QueryController;
import org.iceforge.skadi.aws.s3.CacheFetchContext;
import org.iceforge.skadi.aws.s3.ResultSetToS3ChunkWriter;
import org.iceforge.skadi.aws.s3.S3AccessLayer;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

class QueryControllerTest {

    private QueryService queryService;
    private ManifestReader manifestReader;
    private S3AccessLayer s3;
    private QueryStatsRegistry stats;
    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        queryService = mock(QueryService.class);
        manifestReader = mock(ManifestReader.class);
        s3 = mock(S3AccessLayer.class);
        stats = mock(QueryStatsRegistry.class);

        QueryController controller =
                new QueryController(queryService, manifestReader, s3, stats);

        mockMvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    /* ---------- POST /v1/query ---------- */

    @Test
    void submit_hit_returns200() throws Exception {

        QueryModels.QueryResponse resp =
                new QueryModels.QueryResponse(
                        QueryModels.Status.HIT,                 // status
                        "q1",                                   // queryId
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",                        // bucket
                                "prefix",                        // prefix
                                "run-1",                         // runId
                                "manifest",                      // manifestKey
                                100L,                             // rowCount
                                1                                 // chunkCount
                        ),
                        Map.of()                                // meta
                );

        when(queryService.submit(any())).thenReturn(resp);

        mockMvc.perform(post("/v1/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sql\":\"select 1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("HIT"))
                .andExpect(jsonPath("$.queryId").value("q1"));
    }

    @Test
    void submit_running_returns202() throws Exception {
               QueryModels.QueryResponse resp =
                new QueryModels.QueryResponse(
                        QueryModels.Status.RUNNING,                 // status
                        "q1",                                   // queryId
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",                        // bucket
                                "prefix",                        // prefix
                                "run-1",                         // runId
                                "manifest",                      // manifestKey
                                100L,                             // rowCount
                                1                                 // chunkCount
                        ),
                        Map.of()                                // meta
                );
        when(queryService.submit(any())).thenReturn(resp);

        mockMvc.perform(post("/v1/query")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"sql\":\"select 1\"}"))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.status").value("RUNNING"));
    }

    /* ---------- GET /v1/query/{id} ---------- */

    @Test
    void status_passthrough() throws Exception {

        QueryModels.QueryStatusResponse st =
                new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,                 // status
                        "q1",                                   // queryId
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",                        // bucket
                                "prefix",                        // prefix
                                "run-1",                         // runId
                                "manifest",                      // manifestKey
                                100L,                             // rowCount
                                1                                 // chunkCount
                        ),
                        null,                                   // error
                        Instant.now()                           // updatedAt
                );

        when(queryService.status("q1")).thenReturn(st);

        mockMvc.perform(get("/v1/query/q1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("HIT"))
                .andExpect(jsonPath("$.queryId").value("q1"));
    }




    @Test
    void manifest_success() throws Exception {

        QueryModels.QueryStatusResponse st =
                new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,                 // status
                        "q1",                                   // queryId
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",                        // bucket
                                "prefix",                        // prefix
                                "run-1",                         // runId
                                "manifest",                      // manifestKey
                                100L,                             // rowCount
                                1                                 // chunkCount
                        ),
                        null,                                   // error
                        Instant.now()                           // updatedAt
                );

        ResultSetToS3ChunkWriter.Manifest manifest =
                new ResultSetToS3ChunkWriter.Manifest(
                        "run-1",                               // runId
                        "bucket",                              // bucket
                        "prefix",                              // prefix
                        false,                                 // compressed
                        100L,                                  // totalRows
                        456L,                                  // totalUncompressedBytes
                        List.of(
                                new ResultSetToS3ChunkWriter.ChunkDescriptor(
                                        0,                     // part
                                        "k1",                  // key
                                        123L,                  // bytes
                                        456L,                  // uncompressedBytes
                                        "etag-1"               // etag
                                )
                        )
                );

        when(queryService.status("q1")).thenReturn(st);
        when(manifestReader.read("bucket", "manifest")).thenReturn(manifest);

        mockMvc.perform(get("/v1/query/q1/manifest"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.chunks.length()").value(1));
    }


    @Test
    void manifest_unknownQuery_throws() throws Exception {
        when(queryService.status("q1"))
                .thenReturn(new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,
                        "q1",
                        null,          // unknown -> controller throws IllegalArgumentException
                        null,
                        Instant.now()
                ));

        ServletException se = assertThrows(ServletException.class, () ->
                mockMvc.perform(get("/v1/query/q1/manifest"))
        );

        Throwable root = se;
        while (root.getCause() != null) {
            root = root.getCause();
        }

        assertEquals(IllegalArgumentException.class, root.getClass());
        assertEquals("Unknown queryId: q1", root.getMessage());
    }


    @Test
    void manifest_manifestReadFailure_throwsIOException() throws Exception {
        when(queryService.status("q1"))
                .thenReturn(new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,
                        "q1",
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket", "prefix", "run-1", "manifest", 100L, 1
                        ),
                        null,
                        Instant.now()
                ));

        when(manifestReader.read("bucket", "manifest"))
                .thenThrow(new IOException("boom"));

        IOException ex = assertThrows(IOException.class, () ->
                mockMvc.perform(get("/v1/query/q1/manifest"))
        );

        assertEquals("boom", ex.getMessage());
    }



    /* ---------- GET /chunk ---------- */

    @Test
    void chunk_success_streams() throws Exception {
        byte[] data = "hello".getBytes(StandardCharsets.UTF_8);

        QueryModels.QueryStatusResponse st =
                new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,
                        "q1",
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",
                                "prefix",
                                "run-1",
                                "manifest",
                                100L,
                                1
                        ),
                        null,
                        Instant.now()
                );

        ResultSetToS3ChunkWriter.Manifest manifest =
                new ResultSetToS3ChunkWriter.Manifest(
                        "run-1",
                        "bucket",
                        "prefix",
                        false,
                        100L,
                        456L,
                        List.of(
                                new ResultSetToS3ChunkWriter.ChunkDescriptor(
                                        0,
                                        "k1",
                                        123L,
                                        456L,
                                        "etag-1"
                                )
                        )
                );

        when(queryService.status("q1")).thenReturn(st);
        when(manifestReader.read("bucket", "manifest")).thenReturn(manifest);

        when(s3.getStream(argThat(ref ->
                "bucket".equals(ref.bucket()) && "k1".equals(ref.key())
        ))).thenReturn(new ByteArrayInputStream(data));

        var mvcResult = mockMvc.perform(get("/v1/query/q1/chunk/0"))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/x-ndjson"))
                .andExpect(content().bytes(data));

        verify(stats).recordServe(eq("q1"), anyLong(), any(CacheFetchContext.Source.class));
    }


    @Test
    void chunk_missingPart_returns404() throws Exception {
        QueryModels.QueryStatusResponse st =
                new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,                 // status
                        "q1",                                   // queryId
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",                        // bucket
                                "prefix",                        // prefix
                                "run-1",                         // runId
                                "manifest",                      // manifestKey
                                100L,                             // rowCount
                                1                                 // chunkCount
                        ),
                        null,                                   // error
                        Instant.now()                           // updatedAt
                );

        ResultSetToS3ChunkWriter.Manifest manifest =
                new ResultSetToS3ChunkWriter.Manifest(
                        "run-1",                               // runId
                        "bucket",                              // bucket
                        "prefix",                              // prefix
                        false,                                 // compressed
                        100L,                                  // totalRows
                        456L,                                  // totalUncompressedBytes
                        List.of(
                                new ResultSetToS3ChunkWriter.ChunkDescriptor(
                                        0,                     // part
                                        "k1",                  // key
                                        123L,                  // bytes
                                        456L,                  // uncompressedBytes
                                        "etag-1"               // etag
                                )
                        )
                );



        when(queryService.status("q1")).thenReturn(st);
        when(manifestReader.read("bucket", "manifest")).thenReturn(manifest);

        mockMvc.perform(get("/v1/query/q1/chunk/99"))
                .andExpect(status().isNotFound());
    }

    /* ---------- GET /stream ---------- */

    @Test
    void streamAll_success() throws Exception {
        byte[] data = "x".getBytes(StandardCharsets.UTF_8);

        QueryModels.QueryStatusResponse st =
                new QueryModels.QueryStatusResponse(
                        QueryModels.Status.HIT,                 // status
                        "q1",                                   // queryId
                        new ResultSetToS3ChunkWriter.S3ResultSetRef(
                                "bucket",                        // bucket
                                "prefix",                        // prefix
                                "run-1",                         // runId
                                "manifest",                      // manifestKey
                                100L,                             // rowCount
                                1                                 // chunkCount
                        ),
                        null,                                   // error
                        Instant.now()                           // updatedAt
                );

        ResultSetToS3ChunkWriter.Manifest manifest =
                new ResultSetToS3ChunkWriter.Manifest(
                        "run-1",                               // runId
                        "bucket",                              // bucket
                        "prefix",                              // prefix
                        false,                                 // compressed
                        100L,                                  // totalRows
                        456L,                                  // totalUncompressedBytes
                        List.of(
                                new ResultSetToS3ChunkWriter.ChunkDescriptor(
                                        0,                     // part
                                        "k1",                  // key
                                        123L,                  // bytes
                                        456L,                  // uncompressedBytes
                                        "etag-1"               // etag
                                )
                        )
                );

        when(queryService.status("q1")).thenReturn(st);
        when(manifestReader.read("bucket", "manifest")).thenReturn(manifest);
        when(s3.getStream(any())).thenReturn(new ByteArrayInputStream(data));

        mockMvc.perform(get("/v1/query/q1/stream"))
                .andExpect(status().isOk());
    }
}
