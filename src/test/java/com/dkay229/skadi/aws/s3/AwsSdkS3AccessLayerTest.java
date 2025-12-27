package com.dkay229.skadi.aws.s3;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class AwsSdkS3AccessLayerTest {

    @Mock
    private S3Client s3Client;

    @Mock
    private S3Presigner s3Presigner;

    private AwsSdkS3AccessLayer s3AccessLayer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        s3AccessLayer = new AwsSdkS3AccessLayer(s3Client, s3Presigner);
    }

    @Test
    void testPutBytesSuccess() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        byte[] data = "data".getBytes();
        PutObjectResponse response = PutObjectResponse.builder().eTag("etag").build();

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class))).thenReturn(response);

        String eTag = s3AccessLayer.putBytes(ref, data, "text/plain", Map.of("key", "value"));

        assertEquals("etag", eTag);
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testPutBytesFailure() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        byte[] data = "data".getBytes();

        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(S3Exception.builder().message("S3 error").build());

        S3AccessException exception = assertThrows(S3AccessException.class, () ->
                s3AccessLayer.putBytes(ref, data, "text/plain", Map.of("key", "value"))
        );

        assertTrue(exception.getMessage().contains("S3 putBytes failed"));
        verify(s3Client).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void testGetBytesSuccess() throws IOException {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");
        byte[] data = "data".getBytes();
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        ResponseInputStream<GetObjectResponse> responseStream = new ResponseInputStream<>(
                GetObjectResponse.builder().build(),
                byteArrayInputStream
        );

        when(s3Client.getObject(any(GetObjectRequest.class))).thenReturn(responseStream);

        byte[] result = s3AccessLayer.getBytes(ref);

        assertArrayEquals(data, result);
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }

    @Test
    void testGetBytesFailure() {
        S3Models.ObjectRef ref = new S3Models.ObjectRef("bucket", "key");

        when(s3Client.getObject(any(GetObjectRequest.class)))
                .thenThrow(S3Exception.builder().message("S3 error").build());

        S3AccessException exception = assertThrows(S3AccessException.class, () ->
                s3AccessLayer.getBytes(ref)
        );

        assertTrue(exception.getMessage().contains("S3 getBytes failed"));
        verify(s3Client).getObject(any(GetObjectRequest.class));
    }
}
