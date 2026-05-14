/**
 * Semantic contract records — the governed dataset vocabulary for Skadi.
 *
 * <h2>Types in this package</h2>
 *
 * <p><strong>Core vocabulary (C2.2)</strong>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticContract} —
 *       top-level contract: name, version, entity, measures, dimensions,
 *       access policy, cache policy</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticEntity} —
 *       logical dataset name bound to a {@link org.iceforge.skadi.semantic.contract.SemanticEndpoint}</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticEndpoint} —
 *       physical {@code catalog.schema.table} binding; no connection opened here</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticMeasure} —
 *       named aggregation expression (e.g. {@code SUM(pnl)}); expression is a
 *       string descriptor, never executed by this record</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticDimension} —
 *       column binding with {@code filterable} / {@code groupable} flags</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticRuleRef} —
 *       pointer to an external governance rule; no rule logic here</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticContractVersion} —
 *       semver wrapper for contract schema versioning</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticFieldType} —
 *       enum of logical data types (STRING, DECIMAL, DATE, etc.)</li>
 * </ul>
 *
 * <p><strong>Access and cache policy metadata (C2.3)</strong>
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticAccessPolicy} —
 *       descriptive metadata: allowed roles, principals, rule refs; no enforcement</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticRoleRef} —
 *       named role reference; no role resolution</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticPrincipalRef} —
 *       typed principal reference (user, group, service account); no auth</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticPrincipalType} —
 *       enum: USER / GROUP / SERVICE_ACCOUNT</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticCachePolicy} —
 *       descriptive cache hint: strategy, TTL; no cache I/O</li>
 *   <li>{@link org.iceforge.skadi.semantic.contract.SemanticCacheStrategy} —
 *       enum: NONE / TTL / DATASET_VERSION (DATASET_VERSION is a future seam)</li>
 * </ul>
 *
 * <h2>Design invariants</h2>
 *
 * <p>Every type in this package is either an immutable Java record or an enum.
 * All records with {@code List} fields use {@code List.copyOf()} in their
 * compact constructors — returned lists are always unmodifiable. No type in
 * this package:
 * <ul>
 *   <li>generates or executes SQL</li>
 *   <li>loads files from disk or validates YAML/JSON Schema</li>
 *   <li>opens connections to Databricks, S3, or any external system</li>
 *   <li>contains Spring beans or runtime wiring</li>
 *   <li>enforces access policy or evaluates rule logic</li>
 * </ul>
 *
 * <h2>Extension points</h2>
 *
 * <p>C3 adds {@code SemanticQuery} and {@code SemanticCompiler} interface —
 * these consume the vocabulary defined here without changing it.
 *
 * <p>C4 adds {@code CacheContract} and {@code CacheIdentity} — these use
 * {@link org.iceforge.skadi.semantic.contract.SemanticCachePolicy} as a hint
 * but live in a separate package.
 *
 * <p>See {@code ai/lane-c/c2-semantic-contract-skeletons.md} for full C2
 * implementation notes and C3/C4/C5 guidance.
 */
package org.iceforge.skadi.semantic.contract;
