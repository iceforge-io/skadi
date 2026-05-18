package org.iceforge.skadi.semantic.demo;

/**
 * Translates a plain-English {@link DemoSemanticQueryRequest} into a structured
 * {@link DemoSemanticQueryInterpretation}.
 *
 * <p>Implementations must:
 * <ul>
 *   <li>Never return {@code null} from {@link #interpret}.</li>
 *   <li>Never generate SQL, call Databricks, call S3, or execute queries.</li>
 *   <li>Return {@link DemoSemanticInterpretationConfidence#UNSUPPORTED} for
 *       requests that cannot be mapped to a known demo contract.</li>
 *   <li>Return {@link DemoSemanticInterpretationConfidence#AMBIGUOUS} for requests
 *       that appear financial but cannot be resolved to a single interpretation.</li>
 * </ul>
 *
 * <p>The canonical demo implementation is
 * {@link RuleBasedDemoSemanticIntentAdapter}, which uses deterministic rule
 * matching without any LLM or network calls.
 */
@FunctionalInterface
public interface DemoSemanticIntentAdapter {

    /**
     * Interprets {@code request} and returns a structured representation of the intent.
     *
     * @param request the plain-English query request; must not be null
     * @return the interpretation; never null
     */
    DemoSemanticQueryInterpretation interpret(DemoSemanticQueryRequest request);
}
