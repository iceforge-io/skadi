package org.iceforge.skadi.sqlgateway.pgwire;

/**
 * Minimal PostgreSQL type/OID constants used by the pgwire server.
 *
 * <p>We currently always use text format (formatCode=0) for values, but the column
 * type OIDs still matter to clients like Tableau to infer measures/dimensions.
 */
final class PgType {
    private PgType() {}

    static final int BOOL = 16;

    static final int INT2 = 21;
    static final int INT4 = 23;
    static final int INT8 = 20;

    static final int FLOAT4 = 700;
    static final int FLOAT8 = 701;

    static final int NUMERIC = 1700;

    static final int TEXT = 25;
    static final int VARCHAR = 1043;

    static final int DATE = 1082;
    static final int TIMESTAMP = 1114; // timestamp without time zone
    static final int TIMESTAMPTZ = 1184; // timestamp with time zone

    static final int UNKNOWN = 705;
}

