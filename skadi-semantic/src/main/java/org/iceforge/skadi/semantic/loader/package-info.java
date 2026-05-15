/**
 * File-backed contract loading for skadi-semantic (Lane D, D3).
 *
 * <p>{@link org.iceforge.skadi.semantic.loader.ContractLoader} is the loading interface.
 * {@link org.iceforge.skadi.semantic.loader.JsonContractLoader} is the canonical
 * implementation — reads JSON files per ADR-011 and returns immutable
 * {@link org.iceforge.skadi.semantic.contract.SemanticContract} records.
 *
 * <p>This package does not populate {@code ContractRegistry} (D4), enforce access
 * policy, execute SQL, or call any external system.
 */
package org.iceforge.skadi.semantic.loader;
