package org.iceforge.skadi.semantic.loader;

import java.util.Objects;

/**
 * Thrown when a semantic contract file cannot be opened, parsed, or mapped to a
 * {@link org.iceforge.skadi.semantic.contract.SemanticContract}.
 *
 * <p>The {@link #source()} string identifies which file or stream caused the failure
 * so operators can locate and fix the offending contract without scanning logs.
 *
 * <p>This is an unchecked exception. Callers that want to recover from individual
 * load failures should catch it explicitly in a loop rather than in a blanket handler.
 */
public final class ContractLoadException extends RuntimeException {

    private final String source;

    /**
     * @param source  path or label identifying the contract file or stream; must not be null
     * @param message short description of the failure
     * @param cause   underlying exception; may be null
     */
    public ContractLoadException(String source, String message, Throwable cause) {
        super(Objects.requireNonNull(source, "source") + ": " + message, cause);
        this.source = source;
    }

    /**
     * @param source  path or label identifying the contract file or stream; must not be null
     * @param message short description of the failure
     */
    public ContractLoadException(String source, String message) {
        super(Objects.requireNonNull(source, "source") + ": " + message);
        this.source = source;
    }

    /** The file path or stream label that caused the failure. Never null. */
    public String source() {
        return source;
    }
}
