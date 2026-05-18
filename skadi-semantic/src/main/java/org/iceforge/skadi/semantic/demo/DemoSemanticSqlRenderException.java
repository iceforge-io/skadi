package org.iceforge.skadi.semantic.demo;

/**
 * Thrown by {@link DemoSemanticSqlRenderer} when a {@link DemoSemanticQueryInterpretation}
 * cannot be rendered into safe SQL.
 *
 * <p>Common causes:
 * <ul>
 *   <li>The interpretation is not executable (confidence UNSUPPORTED or AMBIGUOUS)</li>
 *   <li>The contract ID is not on the renderer's allowlist</li>
 *   <li>The measure, a groupBy dimension, or a filter name is not on the allowlist</li>
 *   <li>The contract/output-shape combination is inconsistent</li>
 * </ul>
 *
 * <p>The message is operator-safe: it names which field failed the allowlist check
 * but does not contain raw SQL, credentials, or stack traces.
 */
public final class DemoSemanticSqlRenderException extends RuntimeException {

    public DemoSemanticSqlRenderException(String message) {
        super(message);
    }
}
