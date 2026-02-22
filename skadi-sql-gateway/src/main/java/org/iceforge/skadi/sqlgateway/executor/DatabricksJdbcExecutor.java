package org.iceforge.skadi.sqlgateway.executor;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.iceforge.skadi.arrow.JdbcArrowStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes queries against a Databricks SQL Warehouse over JDBC.
 *
 * <p>MVP: uses JDBC + Arrow IPC streaming via {@link JdbcArrowStreamer}.
 */
public class DatabricksJdbcExecutor implements SqlExecutor {
    private static final Logger log = LoggerFactory.getLogger(DatabricksJdbcExecutor.class);

    private final DataSource dataSource;
    private final Executor executor;

    public DatabricksJdbcExecutor(DataSource dataSource) {
        this(dataSource, Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "dbx-jdbc-exec");
            t.setDaemon(true);
            return t;
        }));
    }

    DatabricksJdbcExecutor(DataSource dataSource, Executor executor) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.executor = Objects.requireNonNull(executor, "executor");
    }

    @Override
    public SqlExecutionHandle executeToArrow(SqlExecutionRequest request, OutputStream out) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(out, "out");

        AtomicBoolean cancel = new AtomicBoolean(false);
        CompletableFuture<SqlExecutionResult> fut = new CompletableFuture<>();

        final Duration timeout = request.timeout() == null ? Duration.ofMinutes(5) : request.timeout();
        final int fetchSize = request.fetchSize() <= 0 ? 1000 : request.fetchSize();
        final int batchRows = request.batchRows() <= 0 ? 1024 : request.batchRows();

        executor.execute(() -> {
            Instant start = Instant.now();
            String remoteQueryId = null;
            Map<String, String> diag = new HashMap<>();

            try (Connection conn = dataSource.getConnection()) {
                applyClientInfo(conn, request.tags());

                try (PreparedStatement ps = conn.prepareStatement(request.sql())) {
                    bind(ps, request.params());
                    try {
                        ps.setQueryTimeout((int) Math.max(1, timeout.toSeconds()));
                    } catch (SQLException ignored) {
                        // driver may not support it
                    }

                    try (BufferAllocator allocator = new RootAllocator()) {
                        long rows = JdbcArrowStreamer.stream(ps, batchRows, allocator, out, cancel::get);

                        Optional<String> qid = DatabricksQueryIdExtractor.fromWarnings(ps.getWarnings());
                        remoteQueryId = qid.orElse(null);
                        fut.complete(new SqlExecutionResult(rows, remoteQueryId, Duration.between(start, Instant.now()), diag));
                    }
                }
            } catch (Exception e) {
                fut.completeExceptionally(e);
            }
        });

        return new SqlExecutionHandle() {
            @Override
            public void cancel() {
                cancel.set(true);
                // Best-effort: we only have token-based cancel in MVP.
                // Future: store active Statement here and call ps.cancel().
            }

            @Override
            public CompletableFuture<SqlExecutionResult> completion() {
                return fut;
            }
        };
    }

    private static void bind(PreparedStatement ps, List<SqlParam> params) throws SQLException {
        if (params == null) return;
        for (SqlParam p : params) {
            if (p.value() == null) {
                if (p.jdbcType() != null) {
                    ps.setNull(p.index(), p.jdbcType());
                } else {
                    ps.setObject(p.index(), null);
                }
            } else {
                ps.setObject(p.index(), p.value());
            }
        }
    }

    private static void applyClientInfo(Connection conn, Map<String, String> tags) {
        if (tags == null || tags.isEmpty()) return;
        tags.forEach((k, v) -> {
            try {
                conn.setClientInfo(k, v);
            } catch (Exception ignored) {
            }
        });
    }
}

