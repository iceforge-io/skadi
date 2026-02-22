package org.iceforge.skadi.query;

import org.iceforge.skadi.aws.s3.ResultSetToS3ChunkWriter;
import org.iceforge.skadi.aws.s3.S3AccessLayer;
import org.iceforge.skadi.aws.s3.S3Models;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Objects;

@Service
public class ManifestReader {

    private final S3AccessLayer s3;
    private final ObjectMapper mapper;

    public ManifestReader(S3AccessLayer s3, ObjectMapper mapper) {
        this.s3 = Objects.requireNonNull(s3);
        this.mapper = Objects.requireNonNull(mapper);
    }

    public ResultSetToS3ChunkWriter.Manifest read(String bucket, String manifestKey) throws Exception {
        Objects.requireNonNull(bucket, "bucket");
        Objects.requireNonNull(manifestKey, "manifestKey");
        try (InputStream in = s3.getStream(new S3Models.ObjectRef(bucket, manifestKey))) {
            return mapper.readValue(in, ResultSetToS3ChunkWriter.Manifest.class);
        }
    }
}
