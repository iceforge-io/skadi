/**
 * Service boundary interfaces and context records — Lane C C5.
 *
 * <p>This package defines the seams across which semantic-aware execution
 * will flow in post-Lane C lanes. Every type here is either a plain Java
 * interface, an immutable record, or a no-op/skeleton implementation.
 *
 * <h2>Interfaces</h2>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.service.QueryExecutionService} —
 *       executes a query (SQL-first today; semantic-first in a future lane)</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.QueryMetadataService} —
 *       inspects output-shape metadata without executing a query</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.CacheLookupService} —
 *       performs a logical cache lookup or write via C4 boundary types</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.SemanticContractResolver} —
 *       resolves a {@code SemanticContract} by name with principal context</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.LineageContextProvider} —
 *       returns lightweight lineage pointers; does not connect to BCBS239</li>
 * </ul>
 *
 * <h2>Request / result / context records</h2>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.service.ExecutionContext} —
 *       principal, correlation ID, source system</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.ExecutionStatus} —
 *       enum: COMPLETED / FAILED / CACHE_HIT / CANCELLED</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.QueryExecutionRequest} /
 *       {@link org.iceforge.skadi.semantic.service.QueryExecutionResult}</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.QueryMetadataRequest} /
 *       {@link org.iceforge.skadi.semantic.service.QueryMetadataResult}</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.SemanticResolutionRequest} /
 *       {@link org.iceforge.skadi.semantic.service.SemanticResolutionResult}</li>
 * </ul>
 *
 * <h2>No-op and skeleton implementations</h2>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.service.NoOpCacheLookupService} —
 *       returns ABSENT on every lookup; silently discards writes; for tests</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.NoOpLineageContextProvider} —
 *       returns an empty reference list; for tests and inactive deployments</li>
 *   <li>{@link org.iceforge.skadi.semantic.service.SkadiserverQueryExecutionService} —
 *       skeleton that throws {@code UnsupportedOperationException}; the HTTP call
 *       to {@code POST /api/v1/queries} is not yet activated (see DQR-002)</li>
 * </ul>
 *
 * <h2>Design invariants</h2>
 * <p>No type here:
 * <ul>
 *   <li>calls Databricks, S3, or any external system</li>
 *   <li>generates or executes SQL</li>
 *   <li>integrates with a lineage database or Market Risk Brain</li>
 *   <li>implements AI agent behaviour</li>
 *   <li>changes existing cache or SQL Gateway behaviour</li>
 *   <li>contains Spring beans or runtime wiring</li>
 * </ul>
 */
package org.iceforge.skadi.semantic.service;
