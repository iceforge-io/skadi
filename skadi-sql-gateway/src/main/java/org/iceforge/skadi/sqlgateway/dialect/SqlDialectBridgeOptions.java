package org.iceforge.skadi.sqlgateway.dialect;

import java.util.Objects;

public record SqlDialectBridgeOptions(
        SourceDialect sourceDialect,
        ClientCompatibility compatibility,
        boolean translateEnabled,
        boolean normalizeForCacheKeys,
        boolean rewritePgCasts,
        boolean normalizeIdentifierQuotes,
        boolean normalizeLimitOffset,
        boolean mapCommonTypes
) {
    public SqlDialectBridgeOptions {
        Objects.requireNonNull(sourceDialect, "sourceDialect");
        if (compatibility == null) compatibility = ClientCompatibility.GENERIC_JDBC;
    }

    public static SqlDialectBridgeOptions defaultForPgWire() {
        return new SqlDialectBridgeOptions(
                SourceDialect.POSTGRES,
                ClientCompatibility.GENERIC_JDBC,
                true,
                true,
                true,
                true,
                true,
                true
        );
    }
}
