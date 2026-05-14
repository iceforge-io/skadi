package org.iceforge.skadi.semantic.contract;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for access policy and cache policy metadata records.
 * No Spring context, no external services, no JSON serialization.
 */
class SemanticPolicyTest {

    // ── SemanticRoleRef ───────────────────────────────────────────────────────

    @Test
    void roleRef_holdsName() {
        var r = new SemanticRoleRef("risk_analyst");
        assertEquals("risk_analyst", r.name());
    }

    @Test
    void roleRef_rejectsBlankName() {
        assertThrows(IllegalArgumentException.class, () -> new SemanticRoleRef("  "));
    }

    @Test
    void roleRef_rejectsNullName() {
        assertThrows(NullPointerException.class, () -> new SemanticRoleRef(null));
    }

    // ── SemanticPrincipalRef ──────────────────────────────────────────────────

    @Test
    void principalRef_holdsFields() {
        var p = new SemanticPrincipalRef(SemanticPrincipalType.USER, "alice");
        assertEquals(SemanticPrincipalType.USER, p.type());
        assertEquals("alice", p.name());
    }

    @Test
    void principalRef_group() {
        var p = new SemanticPrincipalRef(SemanticPrincipalType.GROUP, "risk-analysts");
        assertEquals(SemanticPrincipalType.GROUP, p.type());
    }

    @Test
    void principalRef_serviceAccount() {
        var p = new SemanticPrincipalRef(SemanticPrincipalType.SERVICE_ACCOUNT, "svc-dashboard");
        assertEquals(SemanticPrincipalType.SERVICE_ACCOUNT, p.type());
    }

    @Test
    void principalRef_rejectsNullType() {
        assertThrows(NullPointerException.class,
                () -> new SemanticPrincipalRef(null, "alice"));
    }

    @Test
    void principalRef_rejectsBlankName() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticPrincipalRef(SemanticPrincipalType.USER, ""));
    }

    // ── SemanticAccessPolicy ──────────────────────────────────────────────────

    @Test
    void accessPolicy_unrestricted_isEmpty() {
        var p = SemanticAccessPolicy.unrestricted();
        assertTrue(p.allowedRoles().isEmpty());
        assertTrue(p.allowedPrincipals().isEmpty());
        assertTrue(p.ruleRefs().isEmpty());
        assertTrue(p.isEmpty());
    }

    @Test
    void accessPolicy_withRoleAndPrincipal() {
        var p = new SemanticAccessPolicy(
                List.of(new SemanticRoleRef("risk_analyst")),
                List.of(new SemanticPrincipalRef(SemanticPrincipalType.USER, "alice")),
                List.of());
        assertEquals(1, p.allowedRoles().size());
        assertEquals(1, p.allowedPrincipals().size());
        assertFalse(p.isEmpty());
    }

    @Test
    void accessPolicy_withRuleRef() {
        var p = new SemanticAccessPolicy(
                List.of(),
                List.of(),
                List.of(new SemanticRuleRef("GOV-001", "BCBS239 lineage requirement")));
        assertEquals(1, p.ruleRefs().size());
        assertFalse(p.isEmpty());
    }

    @Test
    void accessPolicy_allowedRoles_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticRoleRef>();
        mutable.add(new SemanticRoleRef("role_a"));
        var p = new SemanticAccessPolicy(mutable, List.of(), List.of());
        mutable.add(new SemanticRoleRef("role_b"));
        assertEquals(1, p.allowedRoles().size());
    }

    @Test
    void accessPolicy_allowedPrincipals_listIsUnmodifiable() {
        var p = SemanticAccessPolicy.unrestricted();
        assertThrows(UnsupportedOperationException.class,
                () -> p.allowedPrincipals().add(
                        new SemanticPrincipalRef(SemanticPrincipalType.USER, "x")));
    }

    @Test
    void accessPolicy_ruleRefs_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticRuleRef>();
        mutable.add(new SemanticRuleRef("R1", ""));
        var p = new SemanticAccessPolicy(List.of(), List.of(), mutable);
        mutable.add(new SemanticRuleRef("R2", ""));
        assertEquals(1, p.ruleRefs().size());
    }

    @Test
    void accessPolicy_rejectsNullRoles() {
        assertThrows(NullPointerException.class,
                () -> new SemanticAccessPolicy(null, List.of(), List.of()));
    }

    // ── SemanticCachePolicy ───────────────────────────────────────────────────

    @Test
    void cachePolicy_none_hasNoneStrategy() {
        var p = SemanticCachePolicy.none();
        assertEquals(SemanticCacheStrategy.NONE, p.strategy());
        assertNull(p.ttlSeconds());
        assertTrue(p.ruleRefs().isEmpty());
    }

    @Test
    void cachePolicy_ttl_holdsTtlSeconds() {
        var p = SemanticCachePolicy.ttl(600L);
        assertEquals(SemanticCacheStrategy.TTL, p.strategy());
        assertEquals(600L, p.ttlSeconds());
    }

    @Test
    void cachePolicy_ttl_rejectsZero() {
        assertThrows(IllegalArgumentException.class,
                () -> SemanticCachePolicy.ttl(0L));
    }

    @Test
    void cachePolicy_ttl_rejectsNegative() {
        assertThrows(IllegalArgumentException.class,
                () -> SemanticCachePolicy.ttl(-1L));
    }

    @Test
    void cachePolicy_ttl_rejectsNullTtlSeconds() {
        assertThrows(IllegalArgumentException.class,
                () -> new SemanticCachePolicy(SemanticCacheStrategy.TTL, null, List.of()));
    }

    @Test
    void cachePolicy_datasetVersion_allowsNullTtl() {
        // DATASET_VERSION does not require ttlSeconds
        assertDoesNotThrow(() ->
                new SemanticCachePolicy(SemanticCacheStrategy.DATASET_VERSION, null, List.of()));
    }

    @Test
    void cachePolicy_ruleRefs_areDefensivelyCopied() {
        var mutable = new ArrayList<SemanticRuleRef>();
        mutable.add(new SemanticRuleRef("R1", ""));
        var p = new SemanticCachePolicy(SemanticCacheStrategy.NONE, null, mutable);
        mutable.add(new SemanticRuleRef("R2", ""));
        assertEquals(1, p.ruleRefs().size());
    }

    @Test
    void cachePolicy_ruleRefs_listIsUnmodifiable() {
        var p = SemanticCachePolicy.none();
        assertThrows(UnsupportedOperationException.class,
                () -> p.ruleRefs().add(new SemanticRuleRef("X", "")));
    }

    @Test
    void cachePolicy_rejectsNullStrategy() {
        assertThrows(NullPointerException.class,
                () -> new SemanticCachePolicy(null, null, List.of()));
    }

    @Test
    void cachePolicy_rejectsNullRuleRefs() {
        assertThrows(NullPointerException.class,
                () -> new SemanticCachePolicy(SemanticCacheStrategy.NONE, null, null));
    }
}
