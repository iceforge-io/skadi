package org.iceforge.skadi.semantic.service;

import java.util.Objects;

/**
 * Identity and correlation metadata for a single service call.
 *
 * <p>An {@code ExecutionContext} is attached to every service request and
 * propagated through execution, cache lookup, contract resolution, and lineage
 * calls. It allows all service boundaries to share the same principal and
 * correlation identifier without each interface duplicating these fields.
 *
 * <p>{@link #correlationId()} is used for distributed tracing (e.g. MDC
 * {@code query_id}). It may be null when not available (e.g. in tests).
 *
 * <p>{@link #sourceSystem()} identifies the entry point that originated the
 * request — for example {@code "pgwire"}, {@code "semantic-api"},
 * {@code "ai-chat"}, or {@code "test"}. It is used for audit attribution and
 * is nullable when no source system is relevant.
 *
 * <pre>{@code
 * var ctx = new ExecutionContext("alice", "qid-0042", "pgwire");
 * var anon = ExecutionContext.anonymous();
 * }</pre>
 */
public record ExecutionContext(
        String principalName,
        String correlationId,
        String sourceSystem) {

    /**
     * @param principalName the identity of the requesting principal; must not be null;
     *                      empty string denotes an anonymous or unauthenticated caller
     * @param correlationId optional tracing identifier; may be null
     * @param sourceSystem  optional source-system label; may be null
     */
    public ExecutionContext {
        Objects.requireNonNull(principalName, "principalName must not be null");
    }

    /** Returns a context for an anonymous caller with no correlation or source. */
    public static ExecutionContext anonymous() {
        return new ExecutionContext("", null, null);
    }

    /** Returns a context for a named principal with no correlation or source. */
    public static ExecutionContext of(String principalName) {
        return new ExecutionContext(principalName, null, null);
    }
}
