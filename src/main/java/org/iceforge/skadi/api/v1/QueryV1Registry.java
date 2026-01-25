package org.iceforge.skadi.api.v1;

import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
        // Failure diagnostics (optional; populated only when FAILED)
        private volatile String exceptionClass;
        private volatile String rootCauseMessage;
        private volatile String stacktrace;

        // Materialized result location (Option A).
        private volatile String resultBucket;
        private volatile String resultKey;
        private volatile String resultContentType;

        // Completes when the query reaches a terminal state.
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

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
            completion.complete(null);
        }

        public void markFailed(String code, Throwable ex) {
            this.state = QueryV1Models.State.FAILED;
            this.errorCode = code;

            if (ex != null) {
                this.message = ex.getMessage();
                this.exceptionClass = ex.getClass().getName();

                Throwable rc = rootCause(ex);
                this.rootCauseMessage = (rc == null ? null : rc.getMessage());

                this.stacktrace = stackTraceToString(ex);
            } else {
                this.message = null;
                this.exceptionClass = null;
                this.rootCauseMessage = null;
                this.stacktrace = null;
            }

            this.updatedAt = java.time.Instant.now();
            this.completion.complete(null);
        }




        public void markCanceled() {
            state = QueryV1Models.State.CANCELED;
            updatedAt = Instant.now();
            completion.complete(null);
        }

        public void addRows(long n) { rowsProduced.addAndGet(n); }
        public void addBytes(long n) { bytesProduced.addAndGet(n); }

        public long rowsProduced() { return rowsProduced.get(); }
        public long bytesProduced() { return bytesProduced.get(); }
        public Instant startedAt() { return startedAt; }
        public Instant updatedAt() { return updatedAt; }
        public String errorCode() { return errorCode; }
        public String message() { return message; }
        public String exceptionClass() { return exceptionClass; }
        public String rootCauseMessage() { return rootCauseMessage; }
        public String stacktrace() { return stacktrace; }

        public CompletableFuture<Void> completion() { return completion; }

        public String resultBucket() { return resultBucket; }
        public String resultKey() { return resultKey; }
        public String resultContentType() { return resultContentType; }

        public void setResultLocation(String bucket, String key, String contentType) {
            this.resultBucket = bucket;
            this.resultKey = key;
            this.resultContentType = contentType;
            this.updatedAt = Instant.now();
        }
        private final java.util.concurrent.atomic.AtomicBoolean started = new java.util.concurrent.atomic.AtomicBoolean(false);

        public boolean tryStart() {
            return started.compareAndSet(false, true);
        }

    }

    private final ConcurrentMap<String, Entry> entries = new ConcurrentHashMap<>();

    public Entry create(QueryV1Models.SubmitQueryRequest req) {
        Objects.requireNonNull(req);
        String id =  QueryV1KeyUtil.queryId(req);;
        return entries.computeIfAbsent(id, ignored -> new Entry(id, req));
    }

    public Entry getOrCreate(String queryId, QueryV1Models.SubmitQueryRequest req) {
        return entries.computeIfAbsent(queryId, id -> new Entry(id, req));
    }

    public Optional<Entry> get(String queryId) {
        return Optional.ofNullable(entries.get(queryId));
    }

    public void remove(String queryId) {
        entries.remove(queryId);
    }
    private static Throwable rootCause(Throwable t) {
        if (t == null) return null;
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static String stackTraceToString(Throwable t) {
        if (t == null) return null;

        StringWriter sw = new StringWriter(4096);
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        pw.flush();

        String s = sw.toString();

        // Safety: truncate very large traces (dashboard + API friendly)
        final int MAX = 20_000;
        if (s.length() > MAX) {
            return s.substring(0, MAX) + "\n... (truncated)";
        }
        return s;
    }

}
