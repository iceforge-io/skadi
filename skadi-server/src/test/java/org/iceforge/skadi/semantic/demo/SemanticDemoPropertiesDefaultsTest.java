package org.iceforge.skadi.semantic.demo;

import org.iceforge.skadi.semantic.DemoSemanticProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies that {@link DemoSemanticProperties} safe defaults match the spec.
 *
 * <p>No Spring context — creates the properties object directly to check Java-level defaults.
 */
class SemanticDemoPropertiesDefaultsTest {

    @Test
    void enabled_defaultsFalse() {
        assertFalse(new DemoSemanticProperties().isEnabled(),
                "demo endpoint must be disabled by default");
    }

    @Test
    void executionMode_defaultsDisabled() {
        assertEquals("disabled", new DemoSemanticProperties().getExecutionMode(),
                "execution-mode must default to 'disabled'");
    }

    @Test
    void executionMode_defaultResolvesToDisabledEnum() {
        var mode = SemanticDemoExecutionMode.fromConfig(
                new DemoSemanticProperties().getExecutionMode());
        assertEquals(SemanticDemoExecutionMode.DISABLED, mode);
    }

    @Test
    void datasourceId_defaultsToDefault() {
        assertEquals("default", new DemoSemanticProperties().getDatasourceId(),
                "datasource-id must default to 'default'");
    }

    @Test
    void maxRows_defaultsTo100() {
        assertEquals(100, new DemoSemanticProperties().getMaxRows(),
                "max-rows must default to 100");
    }

    @Test
    void queryTimeoutMs_defaultsTo30000() {
        assertEquals(30_000, new DemoSemanticProperties().getQueryTimeoutMs(),
                "query-timeout-ms must default to 30000");
    }

    @Test
    void marketRiskView_defaultsToNonBlank() {
        String view = new DemoSemanticProperties().getMarketRiskView();
        assertNotNull(view);
        assertFalse(view.isBlank(), "market-risk-view must have a non-blank default");
    }

    @Test
    void executionMode_fromConfig_nullDefaultsToDisabled() {
        assertEquals(SemanticDemoExecutionMode.DISABLED,
                SemanticDemoExecutionMode.fromConfig(null));
    }

    @Test
    void executionMode_fromConfig_blankDefaultsToDisabled() {
        assertEquals(SemanticDemoExecutionMode.DISABLED,
                SemanticDemoExecutionMode.fromConfig("  "));
    }

    @Test
    void executionMode_fromConfig_jdbcDirectParsed() {
        assertEquals(SemanticDemoExecutionMode.JDBC_DIRECT,
                SemanticDemoExecutionMode.fromConfig("jdbc-direct"));
        assertEquals(SemanticDemoExecutionMode.JDBC_DIRECT,
                SemanticDemoExecutionMode.fromConfig("jdbc_direct"));
        assertEquals(SemanticDemoExecutionMode.JDBC_DIRECT,
                SemanticDemoExecutionMode.fromConfig("JDBC_DIRECT"));
    }

    @Test
    void executionMode_fromConfig_unknownThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> SemanticDemoExecutionMode.fromConfig("invalid-mode"));
    }
}
