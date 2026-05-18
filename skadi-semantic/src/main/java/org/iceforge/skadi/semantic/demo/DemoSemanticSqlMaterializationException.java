package org.iceforge.skadi.semantic.demo;

/**
 * Thrown by {@link DemoSemanticSqlMaterializer} when a {@link DemoSemanticRenderedSql}
 * cannot be safely materialized into final SQL text.
 *
 * <p>Common causes:
 * <ul>
 *   <li>A {@code :param_name} placeholder has no corresponding entry in the params map</li>
 *   <li>A parameter value type is not supported (only String, Integer, Long, LocalDate,
 *       and other Number subtypes are accepted)</li>
 *   <li>A parameter value is {@code null}</li>
 * </ul>
 *
 * <p>The message names the parameter involved but never includes raw SQL fragments
 * or execution credentials.
 */
public final class DemoSemanticSqlMaterializationException extends RuntimeException {

    public DemoSemanticSqlMaterializationException(String message) {
        super(message);
    }
}
