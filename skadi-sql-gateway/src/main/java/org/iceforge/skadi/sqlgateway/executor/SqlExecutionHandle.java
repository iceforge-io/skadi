package org.iceforge.skadi.sqlgateway.executor;

import java.util.concurrent.CompletableFuture;

public interface SqlExecutionHandle {
    void cancel();

    CompletableFuture<SqlExecutionResult> completion();
}

