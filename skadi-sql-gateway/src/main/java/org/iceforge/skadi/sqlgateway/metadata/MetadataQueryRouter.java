package org.iceforge.skadi.sqlgateway.metadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.Arrays;

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
        // --- Single-value function probes ---

        // Matches both `SELECT current_database()` and `SELECT pg_catalog.current_database()`
        if (lowerNorm.contains("current_database()")) {
            return Optional.of(MetadataRowSet.of(List.of("current_database"), List.of(List.of(pgDatabase))));
        }
        if (lowerNorm.contains("current_schema()")) {
            return Optional.of(MetadataRowSet.of(List.of("current_schema"), List.of(List.of(dbxSchema))));
        }
        if (lowerNorm.equals("select current_user") || lowerNorm.equals("select current_user ;")
                || lowerNorm.equals("select session_user") || lowerNorm.equals("select session_user ;")) {
            return Optional.of(MetadataRowSet.of(List.of("current_user"), List.of(List.of("skadi"))));
        }

        // --- information_schema ---

        if (lowerNorm.contains("from information_schema.schemata")) {
            List<String> cols = List.of("catalog_name", "schema_name", "schema_owner",
                    "default_character_set_catalog", "default_character_set_schema",
                    "default_character_set_name", "sql_path");
            // Arrays.asList() is used here because List.of() rejects null elements.
            List<List<String>> rows = List.of(
                    Arrays.asList(dbxCatalog, dbxSchema, "skadi", null, null, null, null));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }
        if (lowerNorm.contains("from information_schema.tables")) {
            List<String> cols = List.of("table_catalog", "table_schema", "table_name", "table_type");
            List<List<String>> rows = List.of(List.of(dbxCatalog, dbxSchema, "skadi_dummy", "BASE TABLE"));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }
        if (lowerNorm.contains("from information_schema.columns")) {
            List<String> cols = List.of("table_catalog", "table_schema", "table_name",
                    "column_name", "ordinal_position", "data_type", "is_nullable");
            List<List<String>> rows = List.of(
                    List.of(dbxCatalog, dbxSchema, "skadi_dummy", "one", "1", "integer", "YES"));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }

        // --- pg_catalog system tables ---

        if (lowerNorm.contains("from pg_catalog.pg_namespace") || lowerNorm.contains("from pg_namespace")) {
            // Include oid so JDBC foreign-key / type lookups work.
            List<String> cols = List.of("oid", "nspname", "nspowner");
            List<List<String>> rows = List.of(List.of("2200", dbxSchema, "10"));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }
        if (lowerNorm.contains("from pg_catalog.pg_database") || lowerNorm.contains("from pg_database")) {
            List<String> cols = List.of("oid", "datname", "datdba");
            List<List<String>> rows = List.of(List.of("16384", pgDatabase, "10"));
            return Optional.of(new MetadataRowSet(cols, rows, "SELECT 1"));
        }

        // pg_type — return empty; JDBC uses this to discover custom types.
        if (lowerNorm.contains("from pg_catalog.pg_type") || lowerNorm.contains("from pg_type")) {
            return Optional.of(new MetadataRowSet(
                    List.of("oid", "typname", "typtype", "typelem", "typarray", "typinput"),
                    List.of(), "SELECT 0"));
        }
        // pg_proc — return empty; JDBC queries for aggregate/window function metadata.
        if (lowerNorm.contains("from pg_catalog.pg_proc") || lowerNorm.contains("from pg_proc")) {
            return Optional.of(new MetadataRowSet(
                    List.of("oid", "proname", "pronamespace", "prorettype"),
                    List.of(), "SELECT 0"));
        }
        // pg_class — return empty; used by JDBC for table-OID lookups.
        if (lowerNorm.contains("from pg_catalog.pg_class") || lowerNorm.contains("from pg_class")) {
            return Optional.of(new MetadataRowSet(
                    List.of("oid", "relname", "relnamespace", "relkind", "relpersistence"),
                    List.of(), "SELECT 0"));
        }
        // pg_attribute — return empty; column-level metadata for prepared statements.
        if (lowerNorm.contains("from pg_catalog.pg_attribute") || lowerNorm.contains("from pg_attribute")) {
            return Optional.of(new MetadataRowSet(
                    List.of("attrelid", "attname", "atttypid", "attnum", "attnotnull"),
                    List.of(), "SELECT 0"));
        }
        // pg_roles / pg_user — return empty.
        if (lowerNorm.contains("from pg_catalog.pg_roles") || lowerNorm.contains("from pg_roles")
                || lowerNorm.contains("from pg_catalog.pg_user") || lowerNorm.contains("from pg_user")) {
            return Optional.of(new MetadataRowSet(
                    List.of("oid", "rolname", "rolsuper", "rolcreaterole"),
                    List.of(), "SELECT 0"));
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

