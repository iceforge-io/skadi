package org.iceforge.skadi.sqlgateway.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Recognizes a subset of Tableau/JDBC metadata discovery queries and returns synthetic rowsets.
 *
 * <p>MVP: we answer from config stubs (single catalog/schema) and keep everything as TEXT.
 */
public final class MetadataQueryRouter {
    private static final Logger log = LoggerFactory.getLogger(MetadataQueryRouter.class);

    private final MetadataCache cache;
    private final Duration ttl;

    private final String pgDatabase;
    private final String dbxCatalog;
    private final String dbxSchema;

    public MetadataQueryRouter(MetadataCache cache,
                               Duration ttl,
                               String pgDatabase,
                               String dbxCatalog,
                               String dbxSchema) {
        this.cache = Objects.requireNonNull(cache, "cache");
        this.ttl = ttl == null ? Duration.ofMinutes(2) : ttl;
        this.pgDatabase = (pgDatabase == null || pgDatabase.isBlank()) ? "postgres" : pgDatabase;
        this.dbxCatalog = (dbxCatalog == null || dbxCatalog.isBlank()) ? "main" : dbxCatalog;
        this.dbxSchema = (dbxSchema == null || dbxSchema.isBlank()) ? "public" : dbxSchema;
    }

    public Optional<MetadataRowSet> tryAnswer(String sql) {
        String s = sql == null ? "" : sql.trim();
        if (s.isEmpty()) return Optional.empty();

        String normalized = normalizeSqlKey(s);
        String cacheKey = "meta:" + normalized;
        Optional<MetadataRowSet> cached = cache.get(cacheKey);
        if (cached.isPresent()) return cached;

        Optional<MetadataRowSet> rs = routeUncached(s, normalized);
        rs.ifPresent(r -> cache.put(cacheKey, r, ttl));
        return rs;
    }

    private Optional<MetadataRowSet> routeUncached(String sql, String lowerNorm) {
        // Common probes
        if (lowerNorm.equals("select current_database()") || lowerNorm.equals("select current_database() ;")) {
            return Optional.of(MetadataRowSet.of(List.of("current_database"), List.of(List.of(pgDatabase))));
        }
        if (lowerNorm.equals("select current_schema()") || lowerNorm.equals("select current_schema() ;")) {
            return Optional.of(MetadataRowSet.of(List.of("current_schema"), List.of(List.of(dbxSchema))));
        }

        // information_schema.schemata
        if (lowerNorm.contains("from information_schema.schemata")) {
            List<String> cols = List.of("catalog_name", "schema_name", "schema_owner", "default_character_set_catalog", "default_character_set_schema", "default_character_set_name", "sql_path");
            List<List<String>> rows = List.of(List.of(dbxCatalog, dbxSchema, "skadi", null, null, null, null));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }

        // information_schema.tables
        if (lowerNorm.contains("from information_schema.tables")) {
            List<String> cols = List.of(
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "table_type"
            );

            // MVP: no real table listing yet.
            // Provide a tiny anchor table so Tableau has something to hang on.
            List<List<String>> rows = List.of(List.of(dbxCatalog, dbxSchema, "skadi_dummy", "BASE TABLE"));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }

        // information_schema.columns
        if (lowerNorm.contains("from information_schema.columns")) {
            List<String> cols = List.of(
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "column_name",
                    "ordinal_position",
                    "data_type",
                    "is_nullable"
            );

            // MVP: single dummy column for dummy table.
            List<List<String>> rows = List.of(List.of(dbxCatalog, dbxSchema, "skadi_dummy", "one", "1", "integer", "YES"));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }

        // Some JDBC drivers query pg_catalog.pg_type / pg_namespace / etc.
        if (lowerNorm.contains("from pg_catalog.pg_namespace") || lowerNorm.contains("from pg_namespace")) {
            // Keep it minimal: one schema.
            return Optional.of(MetadataRowSet.of(List.of("nspname"), List.of(List.of(dbxSchema))));
        }

        if (lowerNorm.contains("from pg_catalog.pg_database") || lowerNorm.contains("from pg_database")) {
            return Optional.of(MetadataRowSet.of(List.of("datname"), List.of(List.of(pgDatabase))));
        }

        if (lowerNorm.startsWith("select") && lowerNorm.contains("information_schema") && lowerNorm.contains("tables")) {
            log.debug("metadata query matched loosely: {}", sql);
        }

        return Optional.empty();
    }

    static String normalizeSqlKey(String sql) {
        String s = sql == null ? "" : sql.trim();
        s = s.replaceAll("\\s+", " ");
        s = s.toLowerCase(Locale.ROOT);
        return s;
    }
}

