package org.iceforge.skadi.semantic.registry;

import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link ContractRegistry} and {@link InMemoryContractRegistry}.
 * No Spring context, no external services, no JSON serialization.
 */
class ContractRegistryTest {

    private InMemoryContractRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new InMemoryContractRegistry();
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    static SemanticContract contract(String name) {
        var entity = new SemanticEntity(
                name, "test entity",
                new SemanticEndpoint("main", "test", name),
                List.of());
        return new SemanticContract(
                name,
                new SemanticContractVersion("1.0.0"),
                "test contract " + name,
                entity,
                List.of(),
                List.of(),
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.none(),
                null);
    }

    // ── register / findByName ─────────────────────────────────────────────────

    @Test
    void register_thenFindByName_returnsContract() {
        registry.register(contract("alpha"));
        var found = registry.findByName("alpha");
        assertTrue(found.isPresent());
        assertEquals("alpha", found.get().name());
    }

    @Test
    void findByName_unknownName_returnsEmpty() {
        assertTrue(registry.findByName("unknown").isEmpty());
    }

    @Test
    void register_duplicate_throwsDuplicateContractException() {
        registry.register(contract("alpha"));
        var ex = assertThrows(DuplicateContractException.class,
                () -> registry.register(contract("alpha")));
        assertEquals("alpha", ex.contractName());
        assertTrue(ex.getMessage().contains("alpha"));
    }

    @Test
    void register_rejectsNull() {
        assertThrows(NullPointerException.class, () -> registry.register(null));
    }

    @Test
    void findByName_rejectsNull() {
        assertThrows(NullPointerException.class, () -> registry.findByName(null));
    }

    // ── list ──────────────────────────────────────────────────────────────────

    @Test
    void list_empty_returnsEmptyList() {
        assertTrue(registry.list().isEmpty());
    }

    @Test
    void list_returnsAllRegistered() {
        registry.register(contract("a"));
        registry.register(contract("b"));
        var names = registry.list().stream().map(SemanticContract::name).toList();
        assertEquals(List.of("a", "b"), names);
    }

    @Test
    void list_isUnmodifiable() {
        registry.register(contract("a"));
        assertThrows(UnsupportedOperationException.class,
                () -> registry.list().add(contract("x")));
    }

    @Test
    void list_isSnapshot_notLive() {
        registry.register(contract("a"));
        var snapshot = registry.list();
        registry.register(contract("b"));
        assertEquals(1, snapshot.size());   // snapshot taken before second register
        assertEquals(2, registry.list().size());
    }

    // ── contains (default method) ─────────────────────────────────────────────

    @Test
    void contains_registeredName_returnsTrue() {
        registry.register(contract("alpha"));
        assertTrue(registry.contains("alpha"));
    }

    @Test
    void contains_unknownName_returnsFalse() {
        assertFalse(registry.contains("nope"));
    }

    // ── remove ────────────────────────────────────────────────────────────────

    @Test
    void remove_thenFindByName_returnsEmpty() {
        registry.register(contract("alpha"));
        registry.remove("alpha");
        assertTrue(registry.findByName("alpha").isEmpty());
    }

    @Test
    void remove_nonExistentName_isNoOp() {
        assertDoesNotThrow(() -> registry.remove("ghost"));
    }

    @Test
    void remove_thenRegisterSameName_succeeds() {
        registry.register(contract("alpha"));
        registry.remove("alpha");
        assertDoesNotThrow(() -> registry.register(contract("alpha")));
        assertEquals(1, registry.size());
    }

    @Test
    void remove_rejectsNull() {
        assertThrows(NullPointerException.class, () -> registry.remove(null));
    }

    // ── forPrincipal (stub — no access policy filtering in Lane C) ────────────

    @Test
    void forPrincipal_returnsAllContracts() {
        registry.register(contract("a"));
        registry.register(contract("b"));
        var result = registry.forPrincipal("alice");
        assertEquals(2, result.size());
    }

    @Test
    void forPrincipal_emptyRegistry_returnsEmptyList() {
        assertTrue(registry.forPrincipal("alice").isEmpty());
    }

    @Test
    void forPrincipal_rejectsNull() {
        assertThrows(NullPointerException.class, () -> registry.forPrincipal(null));
    }

    @Test
    void forPrincipal_isUnmodifiable() {
        registry.register(contract("a"));
        assertThrows(UnsupportedOperationException.class,
                () -> registry.forPrincipal("alice").add(contract("x")));
    }

    // ── default remove on read-only registry ──────────────────────────────────

    @Test
    void defaultRemove_throwsUnsupportedOperationException() {
        // Anonymous read-only implementation — uses the default remove()
        ContractRegistry readOnly = new ContractRegistry() {
            @Override public void register(SemanticContract c) { }
            @Override public Optional<SemanticContract> findByName(String n) { return Optional.empty(); }
            @Override public List<SemanticContract> list() { return List.of(); }
            @Override public List<SemanticContract> forPrincipal(String p) { return List.of(); }
        };
        assertThrows(UnsupportedOperationException.class, () -> readOnly.remove("any"));
    }

    // ── insertion order preservation ──────────────────────────────────────────

    @Test
    void list_preservesInsertionOrder() {
        registry.register(contract("z"));
        registry.register(contract("a"));
        registry.register(contract("m"));
        var names = registry.list().stream().map(SemanticContract::name).toList();
        assertEquals(List.of("z", "a", "m"), names);
    }
}
