// Java
package org.iceforge.skadi.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataSizeParserTest {

    @Test
    void parsesMultiplicationExpression() {
        assertEquals(4096, DataSizeParser.parseBytes("4*1024"));
        assertEquals(4096, DataSizeParser.parseBytes("  4*1024  "));
    }

    @Test
    void mapsIecUnitsToEvaluatorUnits() {
        assertEquals(1024, DataSizeParser.parseBytes("1KiB"));
        assertEquals(1_048_576, DataSizeParser.parseBytes("1MiB"));
        assertEquals(1_073_741_824 / 2, DataSizeParser.parseBytes("0.5GiB")); // if evaluator supports decimals
    }

    @Test
    void rejectsBlankOrNull() {
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () -> DataSizeParser.parseBytes(null));
        assertTrue(ex1.getMessage().contains("blank"));
        IllegalArgumentException ex2 = assertThrows(IllegalArgumentException.class, () -> DataSizeParser.parseBytes("   "));
        assertTrue(ex2.getMessage().contains("blank"));
    }

    @Test
    void rejectsNonPositive() {
        IllegalArgumentException ex1 = assertThrows(IllegalArgumentException.class, () -> DataSizeParser.parseBytes("0"));
        assertTrue(ex1.getMessage().contains("out of range"));
    }

    @Test
    void rejectsValuesOverIntegerMax() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> DataSizeParser.parseBytes("3*1024*1024*1024"));
        assertTrue(ex.getMessage().contains("out of range"));
    }

    @Test
    void rejectsUnknownUnitsOrInvalidExpr() {
        assertThrows(RuntimeException.class, () -> DataSizeParser.parseBytes("10XB")); // bubbled from evaluator
        assertThrows(RuntimeException.class, () -> DataSizeParser.parseBytes("foo"));
    }
}