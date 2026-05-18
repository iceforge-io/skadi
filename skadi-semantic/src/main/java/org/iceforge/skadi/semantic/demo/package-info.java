/**
 * Demo semantic query DTOs for the market-risk sensitivity plain-English demo.
 *
 * <p>This package contains the request/response model types and the plain-English
 * intent adapter for the market-risk sensitivity demo spike. It does not implement
 * SQL rendering, query execution, or LLM integration.
 *
 * <p>Entry points:
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticQueryRequest} — plain-text input</li>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticIntentAdapter} — interpreter interface</li>
 *   <li>{@link org.iceforge.skadi.semantic.demo.RuleBasedDemoSemanticIntentAdapter} — deterministic rule-based implementation</li>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticQueryInterpretation} — interpreter output</li>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticQueryExecutionResponse} — full execution response</li>
 * </ul>
 */
package org.iceforge.skadi.semantic.demo;
