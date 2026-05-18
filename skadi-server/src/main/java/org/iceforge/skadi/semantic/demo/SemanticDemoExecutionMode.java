package org.iceforge.skadi.semantic.demo;

/**
 * Execution modes for the semantic JDBC demo endpoint.
 *
 * <p>The safe default is {@link #DISABLED}; opt in to {@link #JDBC_DIRECT} for
 * demo runs that hit Databricks through the server-side JDBC path.
 */
public enum SemanticDemoExecutionMode {

    /** Endpoint is active but does not execute queries. */
    DISABLED,

    /** Delegates execution to skadi-server's existing query execution service (not yet implemented). */
    SKADI_SERVER_DELEGATED,

    /** Executes directly via server-side JDBC using the configured datasource. */
    JDBC_DIRECT;

    /**
     * Resolves a config string to an execution mode.
     * Accepts kebab-case (e.g. {@code "jdbc-direct"}) or snake/upper case.
     */
    public static SemanticDemoExecutionMode fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return DISABLED;
        }
        return switch (value.trim().toLowerCase().replace("-", "_")) {
            case "disabled"                -> DISABLED;
            case "skadi_server_delegated"  -> SKADI_SERVER_DELEGATED;
            case "jdbc_direct"             -> JDBC_DIRECT;
            default -> throw new IllegalArgumentException(
                    "Unknown semantic demo execution-mode: '" + value + "'. "
                    + "Allowed: disabled, skadi_server_delegated, jdbc_direct");
        };
    }
}
