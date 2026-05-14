/**
 * Skadi semantic contract module — Lane C boundary.
 *
 * <p>This module contains plain Java records and interfaces that define the
 * semantic contract vocabulary for the Skadi platform. It is a contract-only
 * boundary; it does not contain:
 * <ul>
 *   <li>A semantic planner or rule engine</li>
 *   <li>SQL generation or execution</li>
 *   <li>YAML or JSON Schema loading</li>
 *   <li>Spring beans or runtime wiring</li>
 *   <li>REST endpoints</li>
 *   <li>Databricks, S3, or Tableau integration</li>
 *   <li>UI bricks or AI chatbot integration</li>
 * </ul>
 *
 * <p>Records and interfaces here are the shared vocabulary consumed by future
 * Lane D (UI Brick Runtime) and Lane E (AI Chat Buddy) after Lane C completes.
 * See {@code ai/adr/ADR-008-lane-c-scope.md} and
 * {@code ai/architecture/platform-boundary-model.md}.
 */
package org.iceforge.skadi.semantic;
