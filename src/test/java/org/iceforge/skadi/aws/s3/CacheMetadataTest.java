package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CacheMetadataTest {

    @Test
    void testCreationTime() throws InterruptedException {
        CacheMetadata cacheMetadata = new CacheMetadata();
        Instant creationTime = cacheMetadata.getCreationTime();
        Thread.sleep(5);
        assertNotNull(creationTime, "Creation time should not be null");
        assertTrue(creationTime.isBefore(Instant.now()), "Creation time should be in the past");
    }

    @Test
    void testAddAccessTime() throws InterruptedException {
        CacheMetadata cacheMetadata = new CacheMetadata();
        cacheMetadata.addAccessTime();
        Thread.sleep(5);
        List<Instant> accessTimes = cacheMetadata.getAccessTimes();
        assertEquals(1, accessTimes.size(), "Access times should contain one entry");
        assertTrue(accessTimes.get(0).isBefore(Instant.now()), "Access time should be in the past");
    }

    @Test
    void testGetAccessTimes() throws InterruptedException {
        CacheMetadata cacheMetadata = new CacheMetadata();
        cacheMetadata.addAccessTime();
        cacheMetadata.addAccessTime();
        Thread.sleep(5);
        List<Instant> accessTimes = cacheMetadata.getAccessTimes();
        assertEquals(2, accessTimes.size(), "Access times should contain two entries");

        // Ensure the returned list is a copy
        accessTimes.add(Instant.now());
        assertEquals(2, cacheMetadata.getAccessTimes().size(), "Original access times list should not be modified");
    }
}