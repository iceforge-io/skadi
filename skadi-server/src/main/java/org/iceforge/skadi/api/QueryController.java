package org.iceforge.skadi.api;

import org.iceforge.skadi.aws.s3.CacheFetchContext;
import org.iceforge.skadi.aws.s3.ResultSetToS3ChunkWriter;
import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.aws.s3.S3Models;
import org.iceforge.skadi.query.ManifestReader;
import org.iceforge.skadi.query.QueryModels;
import org.iceforge.skadi.query.QueryService;
import org.iceforge.skadi.query.QueryStatsRegistry;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

@RestController
@RequestMapping("/v1/query")
public class QueryController {

    private final QueryService queryService;
    private final ManifestReader manifestReader;
    private final S3AccessLayer s3;
    private final QueryStatsRegistry stats;


    public QueryController(QueryService queryService, ManifestReader manifestReader, S3AccessLayer s3,QueryStatsRegistry stats) {
        this.queryService = Objects.requireNonNull(queryService);
        this.manifestReader = Objects.requireNonNull(manifestReader);
        this.s3 = Objects.requireNonNull(s3);
        this.stats = Objects.requireNonNull(stats);
    }

    @PostMapping
    public ResponseEntity<QueryModels.QueryResponse> submit(@RequestBody QueryModels.QueryRequest req) throws Exception {
        QueryModels.QueryResponse resp = queryService.submit(req);
        // HIT can be 200; otherwise 202
        return (resp.status() == QueryModels.Status.HIT)
                ? ResponseEntity.ok(resp)
                : ResponseEntity.accepted().body(resp);
    }

    @GetMapping("/{queryId}")
    public QueryModels.QueryStatusResponse status(@PathVariable String queryId) {
        return queryService.status(queryId);
    }

    @GetMapping("/{queryId}/manifest")
    public ResultSetToS3ChunkWriter.Manifest manifest(@PathVariable String queryId) throws Exception {
        QueryModels.QueryStatusResponse st = queryService.status(queryId);
        if (st.ref() == null) {
            throw new IllegalArgumentException("Unknown queryId: " + queryId);
        }
        return manifestReader.read(st.ref().bucket(), st.ref().manifestKey());
    }

    @GetMapping("/{queryId}/chunk/{part}")
    public ResponseEntity<StreamingResponseBody> chunk(@PathVariable String queryId, @PathVariable int part) throws Exception {
        QueryModels.QueryStatusResponse st = queryService.status(queryId);
        if (st.ref() == null) {
            return ResponseEntity.notFound().build();
        }
        ResultSetToS3ChunkWriter.Manifest m = manifestReader.read(st.ref().bucket(), st.ref().manifestKey());
        ResultSetToS3ChunkWriter.ChunkDescriptor cd = m.chunks().stream()
                .filter(c -> c.part() == part)
                .findFirst()
                .orElse(null);
        if (cd == null) {
            return ResponseEntity.notFound().build();
        }
        StreamingResponseBody body = out -> {
            try {
                streamObject(st.ref().bucket(), cd.key(), out,queryId);
            } catch (Exception e) {
                throw new IOException("Streaming failed", e);
            }
        };

        MediaType mt = m.compressed()
                ? MediaType.parseMediaType("application/x-ndjson+gzip")
                : MediaType.parseMediaType("application/x-ndjson");
        return ResponseEntity.ok().contentType(mt).body(body);
    }

    @GetMapping("/{queryId}/stream")
    public ResponseEntity<StreamingResponseBody> streamAll(@PathVariable String queryId) throws Exception {
        QueryModels.QueryStatusResponse st = queryService.status(queryId);
        if (st.ref() == null) {
            return ResponseEntity.notFound().build();
        }
        ResultSetToS3ChunkWriter.Manifest m = manifestReader.read(st.ref().bucket(), st.ref().manifestKey());

        StreamingResponseBody body = out -> {
            for (ResultSetToS3ChunkWriter.ChunkDescriptor cd : m.chunks()) {
                streamObject(st.ref().bucket(), cd.key(), out,queryId); // now only throws IOException
            }
        };

        MediaType mt = m.compressed()
                ? MediaType.parseMediaType("application/x-ndjson+gzip")
                : MediaType.parseMediaType("application/x-ndjson");
        return ResponseEntity.ok().contentType(mt).body(body);
    }


    private void streamObject(String bucket, String key, OutputStream out,String queryId) throws IOException {
        CacheFetchContext.Source src = CacheFetchContext.Source.UNKNOWN;
        long bytes = 0;
        try (InputStream in = s3.getStream(new S3Models.ObjectRef(bucket, key))) {
            src = CacheFetchContext.getAndClear();
            byte[] buf = new byte[1024 * 128];
            int r;
            while ((r = in.read(buf)) >= 0) {
                out.write(buf, 0, r);
            }
        } catch (IOException e) {
            throw e; // keep IOExceptions as-is
        } catch (RuntimeException e) {
            throw e; // propagate runtime
        } catch (Exception e) {
            // Any checked exception from s3.getStream gets converted to IOException
            throw new IOException("Failed streaming s3://" + bucket + "/" + key, e);
        } finally {
            stats.recordServe(queryId, bytes, src);
        }
    }

}
