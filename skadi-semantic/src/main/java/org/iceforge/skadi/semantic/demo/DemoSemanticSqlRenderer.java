package org.iceforge.skadi.semantic.demo;

/**
 * Renders a structured {@link DemoSemanticQueryInterpretation} into a safe,
 * whitelisted {@link DemoSemanticRenderedSql}.
 *
 * <p>Implementations must:
 * <ul>
 *   <li>Validate the interpretation against explicit allowlists before rendering.</li>
 *   <li>Use {@code :param_name} bind placeholders in the SQL text — never concatenate
 *       user-supplied filter values directly into the SQL string.</li>
 *   <li>Throw {@link DemoSemanticSqlRenderException} for any non-renderable input.</li>
 *   <li>Never execute queries, call Databricks, call S3, or generate arbitrary SQL.</li>
 * </ul>
 *
 * <p>The canonical implementation is {@link WhitelistedDemoSemanticSqlRenderer}.
 */
public interface DemoSemanticSqlRenderer {

    /**
     * Renders {@code interpretation} into a whitelisted SQL object.
     *
     * @param interpretation the structured interpretation to render; must not be null
     * @return the rendered SQL with named bind parameters; never null
     * @throws DemoSemanticSqlRenderException if the interpretation fails any allowlist check
     *         or is not executable
     */
    DemoSemanticRenderedSql render(DemoSemanticQueryInterpretation interpretation);
}
