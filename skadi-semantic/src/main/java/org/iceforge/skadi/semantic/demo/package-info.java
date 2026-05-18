/**
 * Demo semantic query DTOs for the market-risk sensitivity plain-English demo.
 *
 * <p>This package contains the request/response model types that carry an
 * interpreted plain-English query through validation, future SQL-template
 * rendering, execution, and result return. It does not implement the interpreter,
 * SQL renderer, or any execution logic.
 *
 * <p>Entry points:
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticQueryRequest} — plain-text input</li>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticQueryInterpretation} — interpreter output</li>
 *   <li>{@link org.iceforge.skadi.semantic.demo.DemoSemanticQueryExecutionResponse} — full execution response</li>
 * </ul>
 */
package org.iceforge.skadi.semantic.demo;
