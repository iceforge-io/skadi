package org.iceforge.skadi.jdbc.spi;

import org.iceforge.skadi.jdbc.SkadiJdbcProperties;
import org.iceforge.skadi.query.QueryModels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Resolves datasources to a {@link JdbcConnectionProvider}.
 * <p>
 * Discovery model matches the S3 SPI:
 * - Providers can be supplied as Spring beans
 * - Providers can also be discovered via {@link ServiceLoader}
 *
 * Spring wins if provider IDs collide.
 */
public final class JdbcClientFactory {
    private static final Logger log = LoggerFactory.getLogger(JdbcClientFactory.class);

    private final SkadiJdbcProperties jdbcProps;
    private final List<JdbcConnectionProvider> providers;

    public JdbcClientFactory(SkadiJdbcProperties jdbcProps, Collection<JdbcConnectionProvider> springProviders) {
        this.jdbcProps = Objects.requireNonNull(jdbcProps, "jdbcProps");

        List<JdbcConnectionProvider> fromSpring = springProviders == null ? List.of() : List.copyOf(springProviders);
        List<JdbcConnectionProvider> fromServiceLoader = ServiceLoader.load(JdbcConnectionProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();

        Map<String, JdbcConnectionProvider> merged = new LinkedHashMap<>();
        for (JdbcConnectionProvider p : fromServiceLoader) merged.put(p.id(), p);
        for (JdbcConnectionProvider p : fromSpring) merged.put(p.id(), p);

        this.providers = List.copyOf(merged.values());

        log.info("Discovered JdbcConnectionProviders: {}", this.providers.stream()
                .map(JdbcConnectionProvider::id).collect(Collectors.toList()));
    }

    /**
     * Open a connection based on either:
     * - datasourceId (preferred) resolved from {@code skadi.jdbc.datasources.*}
     * - legacy request-provided jdbcUrl/username/password (backward compatible)
     */
    public Connection openConnection(QueryModels.QueryRequest.Jdbc reqJdbc) throws Exception {
        Objects.requireNonNull(reqJdbc, "jdbc");

        String datasourceId = reqJdbc.datasourceId();
        if (datasourceId != null && !datasourceId.isBlank()) {
            SkadiJdbcProperties.JdbcDatasourceConfig cfg = jdbcProps.getDatasources().get(datasourceId);
            if (cfg == null) {
                throw new IllegalArgumentException("Unknown datasourceId='" + datasourceId + "'. Configured: " + jdbcProps.getDatasources().keySet());
            }

            // Merge request properties into cfg properties (cfg wins)
            Map<String, String> mergedProps = new LinkedHashMap<>();
            if (reqJdbc.properties() != null) mergedProps.putAll(reqJdbc.properties());
            if (cfg.getProperties() != null) mergedProps.putAll(cfg.getProperties());

            JdbcClientContext ctx = new JdbcClientContext(
                    Optional.of(datasourceId),
                    cfg.getTags() == null ? Map.of() : Map.copyOf(cfg.getTags()),
                    Map.copyOf(mergedProps)
            );

            JdbcConnectionProvider provider = resolveProvider(cfg.getProvider(), ctx);

            // Use a defensive copy of cfg with merged properties so provider sees the final view.
            SkadiJdbcProperties.JdbcDatasourceConfig effective = new SkadiJdbcProperties.JdbcDatasourceConfig();
            effective.setProvider(cfg.getProvider());
            effective.setJdbcUrl(cfg.getJdbcUrl());
            effective.setUsername(cfg.getUsername());
            effective.setPassword(cfg.getPassword());
            effective.setTags(cfg.getTags() == null ? Map.of() : new LinkedHashMap<>(cfg.getTags()));
            effective.setProperties(new LinkedHashMap<>(mergedProps));

            log.info("Using JDBC provider id='{}' for datasourceId='{}'", provider.id(), datasourceId);
            return provider.openConnection(ctx, effective);
        }

        // Legacy: treat the request itself as the datasource config.
        SkadiJdbcProperties.JdbcDatasourceConfig cfg = new SkadiJdbcProperties.JdbcDatasourceConfig();
        cfg.setJdbcUrl(reqJdbc.jdbcUrl());
        cfg.setUsername(reqJdbc.username());
        cfg.setPassword(reqJdbc.password());
        cfg.setProperties(reqJdbc.properties() == null ? Map.of() : new LinkedHashMap<>(reqJdbc.properties()));
        cfg.setTags(Map.of());

        JdbcClientContext ctx = new JdbcClientContext(Optional.empty(), Map.of(), cfg.getProperties());
        JdbcConnectionProvider provider = resolveProvider("default", ctx);
        return provider.openConnection(ctx, cfg);
    }

    private JdbcConnectionProvider resolveProvider(String forcedProviderId, JdbcClientContext ctx) {
        if (forcedProviderId != null && !forcedProviderId.isBlank()) {
            return providers.stream()
                    .filter(p -> forcedProviderId.equals(p.id()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Forced JDBC provider '" + forcedProviderId + "' not found. Available: " + ids()));
        }

        List<JdbcConnectionProvider> matching = providers.stream()
                .filter(p -> p.supports(ctx))
                .toList();

        if (matching.isEmpty()) {
            throw new IllegalStateException("No JdbcConnectionProvider supports ctx=" + safeCtx(ctx) + " providers=" + ids());
        }

        // Deterministic tie-break: lexicographically by id.
        return matching.stream()
                .sorted(Comparator.comparing(JdbcConnectionProvider::id))
                .findFirst()
                .orElseThrow();
    }

    private List<String> ids() {
        return providers.stream().map(JdbcConnectionProvider::id).sorted().toList();
    }

    private static String safeCtx(JdbcClientContext ctx) {
        return "datasourceId=" + ctx.datasourceId().orElse("<legacy>")
                + ", tags=" + ctx.tags()
                + ", propertiesKeys=" + ctx.properties().keySet();
    }
}
