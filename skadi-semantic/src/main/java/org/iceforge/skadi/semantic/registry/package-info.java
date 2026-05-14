/**
 * Contract registry interface — lookup surface for semantic contracts.
 *
 * <p>Contains the {@code ContractRegistry} interface that future loaders,
 * semantic services, UI brick runtimes, and AI intent resolvers depend on.
 * The interface itself carries no implementation; loading and population are
 * deferred until the contract storage format is decided (see DQR-001).
 *
 * <p>The {@code ContractRegistry} interface is introduced in C2.4 (issue #58).
 */
package org.iceforge.skadi.semantic.registry;
