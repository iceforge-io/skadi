package org.iceforge.skadi.semantic.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link SemanticExecutionReadiness#from(SemanticExecutionHealthStatus)}
 * maps every health status to the correct operational readiness category.
 */
class SemanticExecutionReadinessTest {

    @Test
    void healthy_mapsTo_ready() {
        assertEquals(SemanticExecutionReadiness.READY,
                SemanticExecutionReadiness.from(SemanticExecutionHealthStatus.HEALTHY));
    }

    @Test
    void disabled_mapsTo_disabled() {
        assertEquals(SemanticExecutionReadiness.DISABLED,
                SemanticExecutionReadiness.from(SemanticExecutionHealthStatus.DISABLED));
    }

    @Test
    void circuitOpen_mapsTo_degraded() {
        assertEquals(SemanticExecutionReadiness.DEGRADED,
                SemanticExecutionReadiness.from(SemanticExecutionHealthStatus.CIRCUIT_OPEN));
    }

    @Test
    void unavailable_mapsTo_unavailable() {
        assertEquals(SemanticExecutionReadiness.UNAVAILABLE,
                SemanticExecutionReadiness.from(SemanticExecutionHealthStatus.UNAVAILABLE));
    }

    @Test
    void timeout_mapsTo_unavailable() {
        assertEquals(SemanticExecutionReadiness.UNAVAILABLE,
                SemanticExecutionReadiness.from(SemanticExecutionHealthStatus.TIMEOUT));
    }

    @Test
    void failed_mapsTo_unavailable() {
        assertEquals(SemanticExecutionReadiness.UNAVAILABLE,
                SemanticExecutionReadiness.from(SemanticExecutionHealthStatus.FAILED));
    }

    @Test
    void allStatusesCovered() {
        // Ensures every enum constant has an explicit mapping (no switch fall-through)
        for (SemanticExecutionHealthStatus status : SemanticExecutionHealthStatus.values()) {
            SemanticExecutionReadiness result = SemanticExecutionReadiness.from(status);
            org.junit.jupiter.api.Assertions.assertNotNull(result,
                    "status " + status + " must map to a non-null readiness");
        }
    }
}
