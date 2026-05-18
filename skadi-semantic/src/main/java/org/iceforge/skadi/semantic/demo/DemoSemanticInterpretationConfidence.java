package org.iceforge.skadi.semantic.demo;

/**
 * Confidence level assigned to a {@link DemoSemanticQueryInterpretation}.
 *
 * <p>Set by the (future) plain-English interpreter to indicate how confidently
 * the free-text request could be mapped to a governed semantic contract.
 * Consumers should surface LOW/AMBIGUOUS confidence to users and reject
 * UNSUPPORTED requests before attempting SQL rendering or execution.
 */
public enum DemoSemanticInterpretationConfidence {

    /** The request was unambiguously matched to a single contract and measure. */
    HIGH,

    /** The request was matched but with some uncertainty (e.g. a plausible guess at measure). */
    MEDIUM,

    /** The request was matched but multiple interpretations were plausible. */
    LOW,

    /** The request cannot be fulfilled by any registered demo contract. */
    UNSUPPORTED,

    /** The request matched multiple contracts or measures and cannot be resolved without clarification. */
    AMBIGUOUS
}
