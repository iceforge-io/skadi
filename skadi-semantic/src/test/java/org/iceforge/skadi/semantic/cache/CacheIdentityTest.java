package org.iceforge.skadi.semantic.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CacheIdentity} — field validation, null normalisation,
 * fingerprint determinism, and fingerprint uniqueness.
 * No Spring context, no external services.
 */
class CacheIdentityTest {

    static final String SQL = "SELECT SUM(pnl) FROM main.risk.gold_risk";

    // ── Construction ──────────────────────────────────────────────────────────

    @Test
    void holdsFields() {
        var id = new CacheIdentity(SQL, "alice", null);
        assertEquals(SQL,   id.normalizedSql());
        assertEquals("alice", id.principalName());
        assertNull(id.datasetVersionToken());
    }

    @Test
    void nullPrincipal_normalisedToEmptyString() {
        var id = new CacheIdentity(SQL, null, null);
        assertEquals("", id.principalName());
    }

    @Test
    void rejectsNullSql() {
        assertThrows(NullPointerException.class,
                () -> new CacheIdentity(null, "alice", null));
    }

    @Test
    void rejectsBlankSql() {
        assertThrows(IllegalArgumentException.class,
                () -> new CacheIdentity("   ", "alice", null));
    }

    @Test
    void allowsEmptyPrincipal() {
        assertDoesNotThrow(() -> new CacheIdentity(SQL, "", null));
    }

    @Test
    void allowsVersionToken() {
        var id = new CacheIdentity(SQL, "alice", "snap-00042");
        assertEquals("snap-00042", id.datasetVersionToken());
    }

    // ── Fingerprint — determinism ─────────────────────────────────────────────

    @Test
    void fingerprint_isDeterministic() {
        var id = new CacheIdentity(SQL, "alice", null);
        assertEquals(id.fingerprint(), id.fingerprint());
    }

    @Test
    void fingerprint_sameInputs_sameOutput() {
        var id1 = new CacheIdentity(SQL, "alice", null);
        var id2 = new CacheIdentity(SQL, "alice", null);
        assertEquals(id1.fingerprint(), id2.fingerprint());
    }

    @Test
    void fingerprint_is64HexChars() {
        var fp = new CacheIdentity(SQL, "alice", null).fingerprint();
        assertEquals(64, fp.length());
        assertTrue(fp.matches("[0-9a-f]+"), "fingerprint should be lowercase hex");
    }

    // ── Fingerprint — uniqueness ──────────────────────────────────────────────

    @Test
    void fingerprint_differentSql_differentOutput() {
        var f1 = new CacheIdentity(SQL, "alice", null).fingerprint();
        var f2 = new CacheIdentity("SELECT 1", "alice", null).fingerprint();
        assertNotEquals(f1, f2);
    }

    @Test
    void fingerprint_differentPrincipal_differentOutput() {
        var f1 = new CacheIdentity(SQL, "alice", null).fingerprint();
        var f2 = new CacheIdentity(SQL, "bob",   null).fingerprint();
        assertNotEquals(f1, f2);
    }

    @Test
    void fingerprint_withVersionToken_differentFromWithout() {
        var f1 = new CacheIdentity(SQL, "alice", null).fingerprint();
        var f2 = new CacheIdentity(SQL, "alice", "v1").fingerprint();
        assertNotEquals(f1, f2);
    }

    @Test
    void fingerprint_differentVersionTokens_differentOutputs() {
        var f1 = new CacheIdentity(SQL, "alice", "v1").fingerprint();
        var f2 = new CacheIdentity(SQL, "alice", "v2").fingerprint();
        assertNotEquals(f1, f2);
    }

    @Test
    void fingerprint_nullPrincipal_sameAsEmptyPrincipal() {
        var f1 = new CacheIdentity(SQL, null,  null).fingerprint();
        var f2 = new CacheIdentity(SQL, "",    null).fingerprint();
        assertEquals(f1, f2, "null and empty principal should produce same fingerprint");
    }
}
