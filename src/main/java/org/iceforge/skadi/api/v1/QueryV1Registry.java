package org.iceforge.skadi.api.v1;

import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class QueryV1Registry {

    public static final class Entry {
        private final String queryId;
        private final QueryV1Models.SubmitQueryRequest request;
        private volatile QueryV1Models.State state;
        private final AtomicBoolean cancelRequested = new AtomicBoolean(false);
        private final AtomicLong rowsProduced = new AtomicLong(0);
        private final AtomicLong bytesProduced = new AtomicLong(0);
        private volatile Instant startedAt;
        private volatile Instant updatedAt;
        private volatile String errorCode;
        private volatile String message;

        private Entry(String queryId, QueryV1Models.SubmitQueryRequest request) {
            this.queryId = queryId;
            this.request = request;
            this.state = QueryV1Models.State.QUEUED;
            this.updatedAt = Instant.now();
        }

        public String queryId() { return queryId; }
        public QueryV1Models.SubmitQueryRequest request() { return request; }
        public QueryV1Models.State state() { return state; }
        public boolean cancelRequested() { return cancelRequested.get(); }

        public void requestCancel() {
            cancelRequested.set(true);
            updatedAt = Instant.now();
        }

        public void markRunning() {
            state = QueryV1Models.State.RUNNING;
            if (startedAt == null) startedAt = Instant.now();
            updatedAt = Instant.now();
        }

        public void markSucceeded() {
            state = QueryV1Models.State.SUCCEEDED;
            updatedAt = Instant.now();
        }

        public void markFailed(String code, String msg) {
            state = QueryV1Models.State.FAILED;
            errorCode = code;
            message = msg;
            updatedAt = Instant.now();
        }

        public void markCanceled() {
            state = QueryV1Models.State.CANCELED;
            updatedAt = Instant.now();
        }

        public void addRows(long n) { rowsProduced.addAndGet(n); }
        public void addBytes(long n) { bytesProduced.addAndGet(n); }

        public long rowsProduced() { return rowsProduced.get(); }
        public long bytesProduced() { return bytesProduced.get(); }
        public Instant startedAt() { return startedAt; }
        public Instant updatedAt() { return updatedAt; }
        public String errorCode() { return errorCode; }
        public String message() { return message; }
    }

    private final ConcurrentMap<String, Entry> entries = new ConcurrentHashMap<>();

    public Entry create(QueryV1Models.SubmitQueryRequest req) {
        Objects.requireNonNull(req);
        String id = UUID.randomUUID().toString();
        Entry e = new Entry(id, req);
        entries.put(id, e);
        return e;
    }

    public Optional<Entry> get(String queryId) {
        return Optional.ofNullable(entries.get(queryId));
    }

    public void remove(String queryId) {
        entries.remove(queryId);
    }
}
