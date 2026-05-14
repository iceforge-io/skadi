package org.iceforge.skadi.semantic.service;

import java.util.Objects;

/**
 * A request to inspect output-shape metadata via {@link QueryMetadataService}.
 *
 * <p>Either {@link #sql()} or {@link #semanticContractName()} must be provided.
 * SQL-mode inspection derives the output shape from a live or cached result set
 * description (implementation-dependent). Semantic-mode inspection reads the
 * declared shape from the resolved {@code SemanticQueryContract}.
 */
public record QueryMetadataRequest(
        ExecutionContext context,
        String sql,
        String semanticContractName) {

    /**
     * @param context              execution context; must not be null
     * @param sql                  SQL for SQL-mode inspection; may be null
     * @param semanticContractName contract name for semantic-mode; may be null
     * @throws IllegalArgumentException if both are blank
     */
    public QueryMetadataRequest {
        Objects.requireNonNull(context, "context must not be null");
        boolean hasSql      = sql != null && !sql.isBlank();
        boolean hasContract = semanticContractName != null && !semanticContractName.isBlank();
        if (!hasSql && !hasContract) {
            throw new IllegalArgumentException(
                    "at least one of sql or semanticContractName must be non-blank");
        }
    }

    /** Convenience factory for SQL-mode inspection. */
    public static QueryMetadataRequest forSql(ExecutionContext context, String sql) {
        return new QueryMetadataRequest(context, sql, null);
    }

    /** Convenience factory for semantic-mode inspection. */
    public static QueryMetadataRequest forContract(ExecutionContext context, String contractName) {
        return new QueryMetadataRequest(context, null, contractName);
    }
}
