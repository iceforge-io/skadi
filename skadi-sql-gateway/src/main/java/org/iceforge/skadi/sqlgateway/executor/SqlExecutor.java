package org.iceforge.skadi.sqlgateway.executor;

import java.io.OutputStream;

public interface SqlExecutor {
    SqlExecutionHandle executeToArrow(SqlExecutionRequest request, OutputStream out);
}

