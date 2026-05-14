package org.iceforge.skadi.semantic.service;

/**
 * Skeleton {@link QueryExecutionService} that will delegate to {@code skadi-server}
 * via {@code POST /api/v1/queries}.
 *
 * <p><strong>Lane C skeleton — not yet activated.</strong>
 * The HTTP call to {@code skadi-server} is deferred to the post-Lane C activation
 * story. See DQR-002 ({@code ai/dqr/DQR-002-semantic-execution-delegation.md})
 * for the open design question on whether partial or full convergence is preferred.
 *
 * <p>This class throws {@link UnsupportedOperationException} on every call.
 * It exists only as a named skeleton so that the integration point is visible
 * and searchable. When activation is started, this class body is replaced with
 * the real HTTP client implementation.
 *
 * <p>For tests that need a functioning {@link QueryExecutionService} without a
 * real backend, use a hand-rolled lambda or anonymous class instead:
 * <pre>{@code
 * QueryExecutionService stub = req -> QueryExecutionResult.completed(
 *     req.context(),
 *     new CacheIdentity(req.sql(), req.context().principalName(), null),
 *     CacheEntryState.ABSENT);
 * }</pre>
 */
public final class SkadiServerQueryExecutionService implements QueryExecutionService {

    @Override
    public QueryExecutionResult execute(QueryExecutionRequest request) {
        // TODO: activate HTTP call to POST /api/v1/queries on skadi-server
        //       See DQR-002 and issue skadi#43 for activation scope.
        throw new UnsupportedOperationException(
                "SkadiserverQueryExecutionService: HTTP call to POST /api/v1/queries "
                + "not yet activated — see DQR-002");
    }
}
