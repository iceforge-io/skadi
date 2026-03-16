package org.iceforge.skadi.sqlgateway.pgwire;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks active pgwire sessions so that a CancelRequest on a separate connection
 * can locate and interrupt the correct session.
 *
 * <p>Thread-safe: register/deregister from session threads; cancel from acceptor thread.
 */
final class SessionRegistry {

    private static final AtomicInteger PID_SEQ = new AtomicInteger(1000);

    private final ConcurrentHashMap<Long, PgWireSession> sessions = new ConcurrentHashMap<>();

    /** Allocates a unique process-ID for a new session. */
    static int allocatePid() {
        return PID_SEQ.incrementAndGet();
    }

    void register(int pid, int secretKey, PgWireSession session) {
        sessions.put(sessionKey(pid, secretKey), session);
    }

    void deregister(int pid, int secretKey) {
        sessions.remove(sessionKey(pid, secretKey));
    }

    /**
     * Cancels the active query in the session identified by (pid, secretKey).
     * No-op if the session is not found.
     */
    void cancel(int pid, int secretKey) {
        PgWireSession s = sessions.get(sessionKey(pid, secretKey));
        if (s != null) {
            s.cancelActiveQuery();
        }
    }

    private static long sessionKey(int pid, int secretKey) {
        return ((long) pid << 32) | (secretKey & 0xFFFFFFFFL);
    }
}
