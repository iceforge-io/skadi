/**
 * Query contract and output-shape metadata — Lane C C3 boundary.
 *
 * <p>Types in this package describe <em>what a query returns</em>, not how it is
 * executed. They are plain descriptors consumed by future semantic endpoints,
 * UI bricks, Tableau/API consumers, and AI context builders.
 *
 * <h2>Types</h2>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.query.SemanticQueryContract} —
 *       top-level query contract: name, source contract, version, output shape</li>
 *   <li>{@link org.iceforge.skadi.semantic.query.SemanticOutputShape} —
 *       ordered list of output columns with optional row-count hint</li>
 *   <li>{@link org.iceforge.skadi.semantic.query.SemanticOutputColumn} —
 *       single output column: name, type, role, format hint, references</li>
 *   <li>{@link org.iceforge.skadi.semantic.query.SemanticOutputRole} —
 *       enum: MEASURE / DIMENSION / TIMESTAMP / IDENTIFIER / LABEL / DERIVED</li>
 *   <li>{@link org.iceforge.skadi.semantic.query.SemanticFormatHint} —
 *       display-only hints: pattern, unit, currency, precision, scale (all nullable)</li>
 *   <li>{@link org.iceforge.skadi.semantic.query.SemanticReference} —
 *       lightweight pointer: source system, type, id, name</li>
 * </ul>
 *
 * <h2>Design invariants</h2>
 * <p>Every type is either an immutable Java record or an enum. No type here:
 * <ul>
 *   <li>generates or executes SQL</li>
 *   <li>binds to a JDBC {@code ResultSet}</li>
 *   <li>opens connections to Databricks, S3, or any external system</li>
 *   <li>contains Spring beans or runtime wiring</li>
 *   <li>implements UI rendering or Tableau-specific formatting</li>
 *   <li>evaluates lineage or calls a lineage database</li>
 * </ul>
 *
 * <p>See {@code ai/lane-c/c2-semantic-contract-skeletons.md} for Lane C context
 * and {@code ai/adr/ADR-009-contracts-before-planning.md} for why output-shape
 * metadata is defined before the semantic compiler.
 */
package org.iceforge.skadi.semantic.query;
