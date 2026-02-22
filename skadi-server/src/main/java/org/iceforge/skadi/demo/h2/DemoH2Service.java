package org.iceforge.skadi.demo.h2;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DemoH2Service {

    private final DataSource dataSource;
    private final DemoH2Properties props;

    public DemoH2Service(DataSource dataSource, DemoH2Properties props) {
        this.dataSource = dataSource;
        this.props = props;
    }

    public DemoH2Info info() {
        return new DemoH2Info(
                "org.h2.Driver",
                DemoH2Util.jdbcUrl(props),
                props.getUsername(),
                props.getPassword().isBlank() ? "" : "***",
                "Use these values in QueryV1 jdbcUrl/username/password, or configure a datasourceId in skadi.jdbc.datasources."
        );
    }

    /** Execute statements (DDL/DML) in a single transaction. */
    public DemoH2ExecuteResult execute(List<String> statements, boolean stopOnError) {
        Objects.requireNonNull(statements, "statements");

        List<DemoH2StatementResult> results = new ArrayList<>();
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            for (String sql : statements) {
                if (sql == null || sql.isBlank()) continue;

                try (Statement st = conn.createStatement()) {
                    boolean hasResultSet = st.execute(sql);
                    int updateCount = st.getUpdateCount();
                    results.add(new DemoH2StatementResult(sql, true, null, hasResultSet, updateCount));
                } catch (SQLException e) {
                    results.add(new DemoH2StatementResult(sql, false, e.getMessage(), false, -1));
                    if (stopOnError) {
                        conn.rollback();
                        return new DemoH2ExecuteResult(false, "Rolled back due to error", results);
                    }
                }
            }

            conn.commit();
            return new DemoH2ExecuteResult(true, "OK", results);
        } catch (SQLException e) {
            return new DemoH2ExecuteResult(false, e.getMessage(), results);
        }
    }

    public record DemoH2Info(
            String driverClass,
            String jdbcUrl,
            String username,
            String password,
            String note
    ) {}

    public record DemoH2ExecuteResult(
            boolean success,
            String message,
            List<DemoH2StatementResult> results
    ) {}

    public record DemoH2StatementResult(
            String sql,
            boolean success,
            String error,
            boolean hasResultSet,
            int updateCount
    ) {}
}
