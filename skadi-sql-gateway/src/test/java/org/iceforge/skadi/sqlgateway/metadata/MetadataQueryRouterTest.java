package org.iceforge.skadi.sqlgateway.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataQueryRouterTest {

    private MetadataQueryRouter router;

    @BeforeEach
    void setUp() {
        router = new MetadataQueryRouter(
                new MetadataCache(Clock.systemUTC()),
                Duration.ofMinutes(1),
                "testdb", "main", "sales");
    }

    @Test
    void answersCurrentDatabase() {
        assertSingleValue("SELECT current_database()", "testdb");
    }

    @Test
    void answersCurrentDatabaseWithCatalogQualifier() {
        assertSingleValue("SELECT pg_catalog.current_database()", "testdb");
    }

    @Test
    void answersCurrentSchema() {
        assertSingleValue("SELECT current_schema()", "sales");
    }

    @Test
    void answersCurrentUser() {
        Optional<MetadataRowSet> rs = router.tryAnswer("SELECT current_user");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).hasSize(1);
    }

    @Test
    void answersInformationSchemaTables() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "select table_schema, table_name from information_schema.tables");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isNotEmpty();
    }

    @Test
    void answersInformationSchemaSchemata() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "select schema_name from information_schema.schemata");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).hasSize(1);
        assertThat(rs.get().rows().get(0)).contains("sales");
    }

    @Test
    void answersInformationSchemaColumns() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "select column_name from information_schema.columns");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isNotEmpty();
    }

    @Test
    void answersPgNamespaceWithOid() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT oid, nspname FROM pg_catalog.pg_namespace");
        assertThat(rs).isPresent();
        assertThat(rs.get().columns()).contains("oid", "nspname");
        assertThat(rs.get().rows()).hasSize(1);
        assertThat(rs.get().rows().get(0)).contains("sales");
    }

    @Test
    void answersPgDatabaseWithOid() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT oid, datname FROM pg_catalog.pg_database");
        assertThat(rs).isPresent();
        assertThat(rs.get().columns()).contains("oid", "datname");
        assertThat(rs.get().rows().get(0)).contains("testdb");
    }

    @Test
    void answersPgTypeWithEmptyRows() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT oid, typname FROM pg_catalog.pg_type WHERE typname = 'int4'");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isEmpty();
    }

    @Test
    void answersPgProcWithEmptyRows() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT oid, proname FROM pg_catalog.pg_proc");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isEmpty();
    }

    @Test
    void answersPgClassWithEmptyRows() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT oid, relname FROM pg_catalog.pg_class");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isEmpty();
    }

    @Test
    void answersPgAttributeWithEmptyRows() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT attrelid, attname FROM pg_catalog.pg_attribute");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isEmpty();
    }

    @Test
    void answersPgRolesWithEmptyRows() {
        Optional<MetadataRowSet> rs = router.tryAnswer(
                "SELECT rolname FROM pg_catalog.pg_roles");
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).isEmpty();
    }

    @Test
    void returnsEmptyForArbitraryUserQuery() {
        Optional<MetadataRowSet> rs = router.tryAnswer("SELECT * FROM sales.orders");
        assertThat(rs).isEmpty();
    }

    @Test
    void resultIsCachedOnSecondCall() {
        router.tryAnswer("SELECT current_database()");
        Optional<MetadataRowSet> second = router.tryAnswer("SELECT current_database()");
        assertThat(second).isPresent();
    }

    // --- helper ---

    private void assertSingleValue(String sql, String expected) {
        Optional<MetadataRowSet> rs = router.tryAnswer(sql);
        assertThat(rs).isPresent();
        assertThat(rs.get().rows()).hasSize(1);
        assertThat(rs.get().rows().get(0)).containsExactly(expected);
    }
}
