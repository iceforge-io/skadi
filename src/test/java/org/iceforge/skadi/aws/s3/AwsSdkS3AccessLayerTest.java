package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AwsSdkS3AccessLayerTest {

    @Mock private S3Client s3Client;
    @Mock private S3Presigner s3Presigner;

    private AwsSdkS3AccessLayer s3AccessLayer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        s3AccessLayer = new AwsSdkS3AccessLayer(s3Client, s3Presigner);
    }

    @Test
    void putBytes_success_setsContentTypeAndMetadata() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        byte[] data = "data".getBytes();

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("etag").build());

        String eTag = s3AccessLayer.putBytes(ref, data, "text/plain", Map.of("k", "v"));

        assertEquals("etag", eTag);

        ArgumentCaptor<PutObjectRequest> reqCap = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(s3Client).putObject(reqCap.capture(), any(RequestBody.class));
        PutObjectRequest req = reqCap.getValue();

        assertEquals("bucket", req.bucket());
        assertEquals("key", req.key());
        assertEquals("text/plain", req.contentType());
        assertEquals("v", req.metadata().get("k"));
    }

    @Test
    void putBytes_failure_wrapsS3Exception() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().message("S3 error").build());

        S3AccessException ex = assertThrows(S3AccessException.class, () ->
                s3AccessLayer.putBytes(ref, "x".getBytes(), "text/plain", Map.of())
        );

        assertTrue(ex.getMessage().contains("S3 putBytes failed"));
    }

    @Test
    void putStream_success() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        byte[] data = "hello".getBytes();

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(PutObjectResponse.builder().eTag("etag2").build());

        String eTag = s3AccessLayer.putStream(
                ref,
                new ByteArrayInputStream(data),
                data.length,
                "application/octet-stream",
                Map.of("a", "b")
        );

        assertEquals("etag2", eTag);
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void getBytes_success_readsAll() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        byte[] data = "data".getBytes();

        ResponseInputStream<GetObjectResponse> responseStream =
                new ResponseInputStream<>(GetObjectResponse.builder().build(), new ByteArrayInputStream(data));

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

        byte[] result = s3AccessLayer.getBytes(ref);

        assertArrayEquals(data, result);
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void getBytes_noSuchKey_throwsS3CacheMissException_withCacheMissMessage() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "missing");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(NoSuchKeyException.builder().message("nope").build());

        S3CacheMissException ex = assertThrows(S3CacheMissException.class, () ->
                s3AccessLayer.getBytes(ref)
        );

        assertTrue(ex.getMessage().contains("S3 cache miss: s3://bucket/missing"));
    }

    @Test
    void getStream_success_returnsStream() throws Exception {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        byte[] data = "abc".getBytes();
        ResponseInputStream<GetObjectResponse> responseStream =
                new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream(data)
                );

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenReturn(responseStream);

        InputStream out = s3AccessLayer.getStream(ref);

        assertNotNull(out);
        assertArrayEquals(data, out.readAllBytes());

        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void getStream_failure_wrapsS3Exception() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("boom").build());

        S3AccessException ex = assertThrows(S3AccessException.class, () ->
                s3AccessLayer.getStream(ref)
        );

        assertTrue(ex.getMessage().contains("S3 getStream failed"));
    }

    @Test
    void head_success_returnsMetadata() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        HeadObjectResponse head = HeadObjectResponse.builder()
                .contentLength(123L)
                .eTag("etag")
                .contentType("text/plain")
                .lastModified(Instant.now())
                .metadata(Map.of("m", "v"))
                .build();

        when(s3Client.headObject(any(HeadObjectRequest.class))).thenReturn(head);

        Optional<S3Models.ObjectMetadata> md = s3AccessLayer.head(ref);

        assertTrue(md.isPresent());
        assertEquals(123L, md.get().contentLength());
        assertEquals("etag", md.get().eTag());
        assertEquals("text/plain", md.get().contentType());
        assertEquals("v", md.get().userMetadata().get("m"));
    }

    @Test
    void head_404S3Exception_returnsEmptyOptional() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "missing");

        S3Exception e404 = mock(S3Exception.class);
        when(e404.statusCode()).thenReturn(404);
        when(e404.getMessage()).thenReturn("not found");

        doThrow(e404)
                .when(s3Client)
                .headObject(any(HeadObjectRequest.class));

        Optional<S3Models.ObjectMetadata> md = s3AccessLayer.head(ref);

        assertTrue(md.isEmpty());
    }

    @Test
    void exists_delegatesToHead() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        when(s3Client.headObject(any(HeadObjectRequest.class)))
                .thenReturn(HeadObjectResponse.builder().contentLength(1L).build());

        assertTrue(s3AccessLayer.exists(ref));
    }

    @Test
    void delete_failure_wrapsS3Exception() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        when(s3Client.deleteObject(any(DeleteObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("boom").build());

        S3AccessException ex = assertThrows(S3AccessException.class, () ->
                s3AccessLayer.delete(ref)
        );

        assertTrue(ex.getMessage().contains("S3 delete failed"));
    }

    @Test
    void list_success_mapsContents() {
        ListObjectsV2Response resp = ListObjectsV2Response.builder()
                .contents(
                        S3Object.builder().key("a").size(1L).eTag("e1").lastModified(Instant.now()).build(),
                        S3Object.builder().key("b").size(2L).eTag("e2").lastModified(Instant.now()).build()
                )
                .build();

        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(resp);

        List<S3Models.ListItem> items = s3AccessLayer.list("bucket", "p/", 10);

        assertEquals(2, items.size());
        assertEquals("a", items.get(0).key());
        assertEquals(1L, items.get(0).size());
    }

    @Test
    void copy_success_returnsEtagOrEmptyString() {
        CopyObjectResponse resp = CopyObjectResponse.builder()
                .copyObjectResult(CopyObjectResult.builder().eTag("etag-copy").build())
                .build();

        when(s3Client.copyObject(any(CopyObjectRequest.class))).thenReturn(resp);

        String etag = s3AccessLayer.copy(
                new S3Models.ObjectRef("b1", "k1"),
                new S3Models.ObjectRef("b2", "k2")
        );

        assertEquals("etag-copy", etag);
    }
}
