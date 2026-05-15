/**
 * Contract validation and diagnostics for skadi-semantic (Lane D, D6).
 *
 * <p>{@link org.iceforge.skadi.semantic.validation.ContractValidator} defines
 * the validation interface. {@link org.iceforge.skadi.semantic.validation.SemanticContractValidator}
 * is the deterministic, offline implementation — it validates structural rules
 * within and across loaded contracts and returns typed
 * {@link org.iceforge.skadi.semantic.validation.ContractValidationResult} objects
 * containing {@link org.iceforge.skadi.semantic.validation.ContractValidationIssue} items.
 *
 * <p>Validation does not execute SQL, call Databricks, call S3, populate
 * {@code ContractRegistry}, or enforce entitlements.
 *
 * <p>Issue codes are defined as constants on
 * {@link org.iceforge.skadi.semantic.validation.ContractValidationIssue}.
 */
package org.iceforge.skadi.semantic.validation;
