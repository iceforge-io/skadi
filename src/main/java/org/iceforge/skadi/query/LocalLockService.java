package org.iceforge.skadi.query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process lock implementation for local object-store mode.
 *
 * <p>This is not distributed and only provides single-JVM semantics. That's fine for
 * integration tests and "get started" demos.
 */
@Service
@ConditionalOnProperty(prefix = "skadi.query-cache", name = "store", havingValue = "local")
public class LocalLockService implements LockService {

    private static final Logger log = LoggerFactory.getLogger(LocalLockService.class);

    private record Lock(String owner, Instant expiresAt) {}

    private final ConcurrentHashMap<String, Lock> locks = new ConcurrentHashMap<>();

    @Override
    public boolean tryAcquire(String bucket, String key, String owner, long ttlSeconds) {
        Objects.requireNonNull(bucket);
        Objects.requireNonNull(key);
        String fullKey = bucket + ":" + key;
        Instant now = Instant.now();
        Instant exp = now.plusSeconds(Math.max(1, ttlSeconds));

        for (int i = 0; i < 3; i++) {
            Lock existing = locks.get(fullKey);
            if (existing == null) {
                if (locks.putIfAbsent(fullKey, new Lock(owner, exp)) == null) return true;
                continue;
            }
            if (existing.expiresAt().isBefore(now)) {
                if (locks.replace(fullKey, existing, new Lock(owner, exp))) return true;
                continue;
            }
            return false;
        }

        log.debug("Local lock contention for {}", fullKey);
        return false;
    }

    @Override
    public void release(String bucket, String key) {
        String fullKey = bucket + ":" + key;
        locks.remove(fullKey);
    }
}
