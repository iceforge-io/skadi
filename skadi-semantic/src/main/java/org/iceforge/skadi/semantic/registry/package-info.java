/**
 * Contract registry — lookup surface for {@link org.iceforge.skadi.semantic.contract.SemanticContract} instances.
 *
 * <h2>Types in this package</h2>
 *
 * <ul>
 *   <li>{@link org.iceforge.skadi.semantic.registry.ContractRegistry} —
 *       interface defining the lookup surface; all production code depends
 *       only on this interface, never on any specific implementation</li>
 *   <li>{@link org.iceforge.skadi.semantic.registry.DuplicateContractException} —
 *       unchecked exception thrown by
 *       {@link org.iceforge.skadi.semantic.registry.ContractRegistry#register} on
 *       name collision</li>
 * </ul>
 *
 * <p>{@code InMemoryContractRegistry} — a mutable, LinkedHashMap-backed
 * implementation used in unit tests — lives in {@code src/test/java}, not
 * in this package, and must not be used in production code.
 *
 * <h2>Interface contract</h2>
 *
 * <p>{@link org.iceforge.skadi.semantic.registry.ContractRegistry} provides:
 * <ul>
 *   <li>{@code register(SemanticContract)} — adds a contract keyed by name</li>
 *   <li>{@code findByName(String)} → {@code Optional<SemanticContract>}</li>
 *   <li>{@code list()} → unmodifiable snapshot</li>
 *   <li>{@code forPrincipal(String)} → contracts accessible to a principal
 *       (stub in Lane C: returns {@code list()}; access policy enforcement
 *       is deferred to post-Lane C)</li>
 *   <li>{@code contains(String)} — default method via {@code findByName}</li>
 *   <li>{@code remove(String)} — default throws {@code UnsupportedOperationException};
 *       mutable implementations override it</li>
 * </ul>
 *
 * <h2>What does not exist here</h2>
 *
 * <p>This package deliberately contains no:
 * <ul>
 *   <li>File loader (YAML, JSON, or any format — format tracked by DQR-001)</li>
 *   <li>Startup scanning or hot-reload mechanism</li>
 *   <li>Spring beans or {@code @Component} annotations</li>
 *   <li>Access policy enforcement or entitlement engine</li>
 *   <li>REST endpoints</li>
 * </ul>
 *
 * <h2>Extension points</h2>
 *
 * <p>C5 introduces {@code SemanticExecutor} which depends on
 * {@code ContractRegistry} to resolve contracts before execution.
 * Lane E (AI Chat Buddy) calls {@code forPrincipal()} to build the LLM
 * context window. Both depend only on this interface — no implementation
 * change is required when the real loader is added post-Lane C.
 *
 * <p>See {@code ai/lane-c/c2-semantic-contract-skeletons.md} for full C2
 * implementation notes.
 */
package org.iceforge.skadi.semantic.registry;
