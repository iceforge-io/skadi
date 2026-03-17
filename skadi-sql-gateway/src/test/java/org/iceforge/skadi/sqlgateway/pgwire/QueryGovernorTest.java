package org.iceforge.skadi.sqlgateway.pgwire;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class QueryGovernorTest {

    @Test
    void unlimitedGovernorAlwaysGrantsSlot() {
        QueryGovernor g = new QueryGovernor(0);
        for (int i = 0; i < 100; i++) {
            assertThat(g.tryAcquire("alice")).isTrue();
        }
    }

    @Test
    void limitsAreEnforcedPerUser() {
        QueryGovernor g = new QueryGovernor(2);

        assertThat(g.tryAcquire("alice")).isTrue();
        assertThat(g.tryAcquire("alice")).isTrue();
        assertThat(g.tryAcquire("alice")).isFalse(); // 3rd slot denied

        // Bob is independent; his limit is not affected by Alice's.
        assertThat(g.tryAcquire("bob")).isTrue();
        assertThat(g.tryAcquire("bob")).isTrue();
        assertThat(g.tryAcquire("bob")).isFalse();
    }

    @Test
    void releaseOpensSlotAgain() {
        QueryGovernor g = new QueryGovernor(1);

        assertThat(g.tryAcquire("alice")).isTrue();
        assertThat(g.tryAcquire("alice")).isFalse();

        g.release("alice");

        assertThat(g.tryAcquire("alice")).isTrue();
    }

    @Test
    void activeCountTracksInFlightQueries() {
        QueryGovernor g = new QueryGovernor(5);

        assertThat(g.activeCount("alice")).isEqualTo(0);
        g.tryAcquire("alice");
        g.tryAcquire("alice");
        assertThat(g.activeCount("alice")).isEqualTo(2);

        g.release("alice");
        assertThat(g.activeCount("alice")).isEqualTo(1);
    }

    @Test
    void nullUserTreatedAsEmptyString() {
        QueryGovernor g = new QueryGovernor(1);
        assertThat(g.tryAcquire(null)).isTrue();
        assertThat(g.tryAcquire(null)).isFalse();
        g.release(null);
        assertThat(g.tryAcquire(null)).isTrue();
    }

    @Test
    void releaseOnUnlimitedGovernorIsNoOp() {
        QueryGovernor g = new QueryGovernor(0);
        // Should not throw even though no acquire was called.
        g.release("alice");
        assertThat(g.activeCount("alice")).isEqualTo(0);
    }
}
