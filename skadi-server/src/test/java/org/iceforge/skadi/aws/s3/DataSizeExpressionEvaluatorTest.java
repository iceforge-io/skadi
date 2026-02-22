package org.iceforge.skadi.aws.s3;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataSizeExpressionEvaluatorTest {

    @Test
    void testEvaluate() {
        assertEquals(1024L, DataSizeExpressionEvaluator.evaluate("1KB"));
        assertEquals(1048576L, DataSizeExpressionEvaluator.evaluate("1MB"));
        assertEquals(1073741824L, DataSizeExpressionEvaluator.evaluate("1GB"));
        assertEquals(1099511627776L, DataSizeExpressionEvaluator.evaluate("1TB"));
        assertEquals(10737418240L, DataSizeExpressionEvaluator.evaluate("1024*1024*1024*10"));
        assertEquals(5120L, DataSizeExpressionEvaluator.evaluate("5KB"));
        assertEquals(5242880L, DataSizeExpressionEvaluator.evaluate("5MB"));
        assertEquals(5368709120L, DataSizeExpressionEvaluator.evaluate("5GB"));
        assertEquals(5497558138880L, DataSizeExpressionEvaluator.evaluate("5TB"));
    }

    @Test
    void testEvaluateWithSpaces() {
        assertEquals(1024L, DataSizeExpressionEvaluator.evaluate(" 1 KB "));
        assertEquals(1048576L, DataSizeExpressionEvaluator.evaluate(" 1 MB "));
    }

    @Test
    void testEvaluateInvalidInput() {
        assertThrows(NumberFormatException.class, () -> DataSizeExpressionEvaluator.evaluate("ABC"));
        assertThrows(NumberFormatException.class, () -> DataSizeExpressionEvaluator.evaluate("1PB"));
    }
}