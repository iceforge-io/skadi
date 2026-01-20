package org.iceforge.skadi.api.v1;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class QueryV1RegistryTest {

    @Test
    void create_requiresNonNullRequest() {
        QueryV1Registry registry = new QueryV1Registry();
        assertThrows(NullPointerException.class, () -> registry.create(null));
    }

    @Test
    void create_returnsEntry_withInitialStateQueued_andMetricsZero() {
        QueryV1Registry registry = new QueryV1Registry();
        QueryV1Models.SubmitQueryRequest req = new QueryV1Models.SubmitQueryRequest(null, "select 1", null, null, null, null, null);

        QueryVRegistryAssertions.assertCreateAndInitialState(registry, req);
    }

    @Test
    void get_and_remove_behaviour() {
        QueryV1Registry registry = new QueryV1Registry();
        QueryV1Models.SubmitQueryRequest req = new QueryV1Models.SubmitQueryRequest(null, "select 1", null, null, null, null, null);

        QueryV1Registry.Entry e = registry.create(req);
        String id = e.queryId();
        assertNotNull(id);
        Optional<QueryV1Registry.Entry> fetched = registry.get(id);
        assertTrue(fetched.isPresent());
        assertSame(e, fetched.get());

        registry.remove(id);
        assertFalse(registry.get(id).isPresent());
    }

    @Test
    void entry_state_transitions_and_metrics() {
        QueryV1Registry registry = new QueryV1Registry();
        QueryV1Models.SubmitQueryRequest req = new QueryV1Models.SubmitQueryRequest(null, "select 1", null, null, null, null, null);

        QueryV1Registry.Entry e = registry.create(req);

        Instant before = e.updatedAt();

        // requestCancel
        e.requestCancel();
        assertTrue(e.cancelRequested());
        Instant afterCancel = e.updatedAt();
        assertTrue(afterCancel.equals(before) || afterCancel.isAfter(before));

        // markRunning
        Instant prev = e.updatedAt();
        e.markRunning();
        assertEquals(QueryV1Models.State.RUNNING, e.state());
        assertNotNull(e.startedAt());
        Instant afterRunning = e.updatedAt();
        assertTrue(afterRunning.equals(prev) || afterRunning.isAfter(prev));

        // add rows/bytes
        e.addRows(5);
        e.addBytes(1024);
        assertEquals(5L, e.rowsProduced());
        assertEquals(1024L, e.bytesProduced());

        // markSucceeded
        Instant beforeSucceeded = e.updatedAt();
        e.markSucceeded();
        assertEquals(QueryV1Models.State.SUCCEEDED, e.state());
        assertTrue(e.updatedAt().equals(beforeSucceeded) || e.updatedAt().isAfter(beforeSucceeded));

        // markFailed
        e.markFailed("ERR", "something went wrong");
        assertEquals(QueryV1Models.State.FAILED, e.state());
        assertEquals("ERR", e.errorCode());
        assertEquals("something went wrong", e.message());

        // markCanceled
        e.markCanceled();
        assertEquals(QueryV1Models.State.CANCELED, e.state());
    }

    // small helper to centralize some assertions used above
    static class QueryVRegistryAssertions {
        static void assertCreateAndInitialState(QueryV1Registry registry, QueryV1Models.SubmitQueryRequest req) {
            QueryV1Registry.Entry e = registry.create(req);
            assertNotNull(e);
            assertEquals(req, e.request());
            assertNotNull(e.queryId());
            assertEquals(QueryV1Models.State.QUEUED, e.state());
            assertFalse(e.cancelRequested());
            assertEquals(0L, e.rowsProduced());
            assertEquals(0L, e.bytesProduced());
            assertNull(e.startedAt());
            assertNotNull(e.updatedAt());
            // ensure registry get returns same entry
            Optional<QueryV1Registry.Entry> fetched = registry.get(e.queryId());
            assertTrue(fetched.isPresent());
            assertSame(e, fetched.get());
        }
    }
}