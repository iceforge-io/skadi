package org.iceforge.skadi.demo.h2;

final class DemoH2Util {
    private DemoH2Util() {}

    static String jdbcUrl(DemoH2Properties props) {
        if (props.getJdbcUrl() != null && !props.getJdbcUrl().isBlank()) {
            return props.getJdbcUrl();
        }
        // DB_CLOSE_DELAY=-1 keeps the in-memory DB alive for the JVM lifetime.
        // DATABASE_TO_UPPER=FALSE preserves identifier case.
        return "jdbc:h2:mem:" + props.getDbName() + ";DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE";
    }
}
