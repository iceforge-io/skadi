package org.iceforge.skadi.aws.s3;

import org.springframework.core.io.InputStreamResource;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

@RestController
@RequestMapping("/internal/cache")
public class PeerCacheController {

    private final CachedAwsSdkS3AccessLayer cache;
    private final PeerAuth peerAuth;

    public PeerCacheController(CachedAwsSdkS3AccessLayer cache, PeerAuth peerAuth) {
        this.cache = cache;
        this.peerAuth = peerAuth;
    }

    private void authorize(String method, String path, String query,
                           String keyId, String ts, String nonce, String sig) {
        try {
            peerAuth.verify(method, path, query, keyId, ts, nonce, sig);
        } catch (PeerAuth.Unauthorized e) {
            throw new PeerUnauthorizedException();
        }
    }

    @RequestMapping(value = "/object", method = RequestMethod.HEAD)
    public ResponseEntity<Void> head(
            @RequestParam("bucket") String bucket,
            @RequestParam("key") String key,
            @RequestHeader(value = "X-Skadi-KeyId", required = false) String keyId,
            @RequestHeader(value = "X-Skadi-Ts", required = false) String ts,
            @RequestHeader(value = "X-Skadi-Nonce", required = false) String nonce,
            @RequestHeader(value = "X-Skadi-Signature", required = false) String sig
    ) throws IOException {
        String query = "bucket=" + bucket + "&key=" + key;
        authorize("HEAD", "/internal/cache/object", query, keyId, ts, nonce, sig);

        Optional<Path> p = cache.localPathIfCached(new S3Models.ObjectRef(bucket, key));
        if (p.isEmpty() || !Files.exists(p.get())) return ResponseEntity.status(HttpStatus.NOT_FOUND).build();

        long size = Files.size(p.get());
        return ResponseEntity.ok().header(HttpHeaders.CONTENT_LENGTH, String.valueOf(size)).build();
    }

    @GetMapping(value = "/object", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<InputStreamResource> get(
            @RequestParam("bucket") String bucket,
            @RequestParam("key") String key,
            @RequestHeader(value = "X-Skadi-KeyId", required = false) String keyId,
            @RequestHeader(value = "X-Skadi-Ts", required = false) String ts,
            @RequestHeader(value = "X-Skadi-Nonce", required = false) String nonce,
            @RequestHeader(value = "X-Skadi-Signature", required = false) String sig
    ) throws IOException {
        String query = "bucket=" + bucket + "&key=" + key;
        authorize("GET", "/internal/cache/object", query, keyId, ts, nonce, sig);

        Optional<Path> p = cache.localPathIfCached(new S3Models.ObjectRef(bucket, key));
        if (p.isEmpty() || !Files.exists(p.get())) return ResponseEntity.status(HttpStatus.NOT_FOUND).build();

        Path file = p.get();
        long size = Files.size(file);
        InputStream in = Files.newInputStream(file);

        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .contentLength(size)
                .body(new InputStreamResource(in));
    }

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    private static class PeerUnauthorizedException extends RuntimeException {}

    @GetMapping(value = "/meta", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<CacheEntryMeta> meta(
            @RequestParam("bucket") String bucket,
            @RequestParam("key") String key,
            @RequestHeader(value = "X-Skadi-KeyId", required = false) String keyId,
            @RequestHeader(value = "X-Skadi-Ts", required = false) String ts,
            @RequestHeader(value = "X-Skadi-Nonce", required = false) String nonce,
            @RequestHeader(value = "X-Skadi-Signature", required = false) String sig
    ) {
        String query = "bucket=" + bucket + "&key=" + key;
        authorize("GET", "/internal/cache/meta", query, keyId, ts, nonce, sig);

        return cache.readLocalMeta(new S3Models.ObjectRef(bucket, key))
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND).build());
    }

}
