package org.iceforge.skadi.api.v1;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

public final class QueryV1Models {

    private QueryV1Models() {}

    public enum State {
        QUEUED,
        RUNNING,
        SUCCEEDED,
        FAILED,
        CANCELED
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SubmitQueryRequest(
            Jdbc jdbc,
            String sql,
            Map<String, Object> parameters,
            String resultFormat,
            Long preferredChunkBytes,
            Long timeoutMs,
            String cacheMode
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Jdbc(
            String datasourceId,
            String jdbcUrl,
            String username,
            String password,
            Map<String, String> properties
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SubmitQueryResponse(
            String queryId,
            State state,
            String resultUrl,
            Instant expiresAt
    ) {
        @JsonIgnore
        public String resultsUrl() {
            if (resultUrl != null && !resultUrl.isBlank()) {
                return resultUrl;
            }
            return "/api/v1/queries/" + queryId + "/results";
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record QueryStatusResponse(
            String queryId,
            State state,
            Long rowsProduced,
            Long bytesProduced,
            Instant startedAt,
            Instant updatedAt,
            String errorCode,
            String message,
            Map<String, Object> details
    ) {}

}
