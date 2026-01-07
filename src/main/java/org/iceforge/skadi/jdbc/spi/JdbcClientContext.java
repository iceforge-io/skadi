package org.iceforge.skadi.jdbc.spi;

import java.util.Map;
import java.util.Optional;

/**
 * Non-secret context information passed to JDBC providers.
 * <p>
 * Keep this free of credentials so it is safe to log in a redacted form.
 */
public record JdbcClientContext(
        Optional<String> datasourceId,
        Map<String, String> tags,
        Map<String, String> properties
) {
}
