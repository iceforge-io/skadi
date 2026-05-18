package org.iceforge.skadi.semantic.demo;

/**
 * Thrown by {@link SemanticDemoSqlRenderer} when a {@link SemanticDemoRequest}
 * fails an allowlist check — unknown contract, measure, dimension, or filter key.
 */
public class SemanticDemoSqlRenderException extends RuntimeException {

    public SemanticDemoSqlRenderException(String message) {
        super(message);
    }
}
