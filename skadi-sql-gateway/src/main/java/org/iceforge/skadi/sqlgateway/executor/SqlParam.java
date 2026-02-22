package org.iceforge.skadi.sqlgateway.executor;

public record SqlParam(int index, Integer jdbcType, Object value) {
}

