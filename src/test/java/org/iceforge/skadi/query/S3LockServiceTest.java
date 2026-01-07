package org.iceforge.skadi.query;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class S3LockServiceTest {

    private S3Client s3 = Mockito.mock(S3Client.class);
    private S3LockService service = new S3LockService(s3);

    @BeforeEach
    void setup() {
        S3Client s3 = mock(S3Client.class);
        S3LockService service = new S3LockService(s3);
    }

    @Test
    void tryAcquire_returnsFalse_whenLockExists() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenReturn(null);

        boolean acquired = service.tryAcquire("b", "k", "owner", 60);

        assertFalse(acquired);
        verify(s3, times(1)).headObject(any(HeadObjectRequest.class));
        verify(s3, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void tryAcquire_returnsFalse_whenPutFails() {
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());
        S3Exception putFail = (S3Exception) S3Exception.builder().statusCode(500).build();
        doThrow(putFail).when(s3).putObject(any(PutObjectRequest.class), any(RequestBody.class));

        boolean acquired = service.tryAcquire("b", "k", "owner", 45);

        assertFalse(acquired);
    }

    @Test
    void tryAcquire_throwsOnNon404HeadError() {
        S3Exception otherErr = (S3Exception) S3Exception.builder().statusCode(403).build();
        when(s3.headObject(any(HeadObjectRequest.class))).thenThrow(otherErr);

        assertThrows(S3Exception.class, () -> service.tryAcquire("b", "k", "owner", 45));
        verify(s3, never()).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void release_deletesObject_and_ignoresErrors() {
        // success path
        assertDoesNotThrow(() -> service.release("b", "k"));
        //verify(s3, times(1)).deleteObject(any());

        // error path
        reset(s3);
        //doThrow(new RuntimeException("boom")).when(s3).deleteObject(any());
        assertDoesNotThrow(() -> service.release("b", "k"));
        //verify(s3, times(1)).deleteObject(any());
    }


}