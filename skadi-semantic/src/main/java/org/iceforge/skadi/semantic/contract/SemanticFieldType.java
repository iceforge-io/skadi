package org.iceforge.skadi.semantic.contract;

/**
 * Data types that a semantic measure or dimension may carry.
 *
 * <p>This enum describes the logical type of a field as declared in a
 * {@link SemanticMeasure} or {@link SemanticDimension}. It is a descriptor
 * only — no SQL type mapping or execution occurs here.
 */
public enum SemanticFieldType {

    /** Arbitrary Unicode text. */
    STRING,

    /** 32-bit signed integer. */
    INTEGER,

    /** 64-bit signed integer. */
    LONG,

    /** Arbitrary-precision decimal number (e.g. financial amounts). */
    DECIMAL,

    /** Calendar date without time component (ISO-8601 date). */
    DATE,

    /** Date and time with UTC offset (ISO-8601 datetime). */
    TIMESTAMP,

    /** Boolean true/false flag. */
    BOOLEAN
}
