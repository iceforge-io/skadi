package org.iceforge.skadi.api;

import org.iceforge.skadi.api.v1.QueryV1Registry;
import org.iceforge.skadi.query.QueryRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Endpoints consumed by the simple static dashboard (src/main/resources/static/index.html).
 *
 * <p>Keep this payload intentionally small and UI-friendly.
 */
@RestController
@RequestMapping("/api")
public class UiQueryController {

    private final QueryV1Registry v1Registry;
    private final QueryRegistry v0Registry;

    public UiQueryController(QueryV1Registry v1Registry, QueryRegistry v0Registry) {
        this.v1Registry = Objects.requireNonNull(v1Registry);
        this.v0Registry = Objects.requireNonNull(v0Registry);
    }

    @GetMapping("/queries/recent")
    public List<Map<String, Object>> recent() {
        // Prefer v1 (Arrow streaming API). Fall back to v0 registry if needed.
        List<Map<String, Object>> out = new ArrayList<>();

        for (QueryV1Registry.Entry e : v1Registry.recent(25)) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("hash", e.queryId());

            String src = e.lastSource();
            row.put("source", (src == null || src.isBlank()) ? e.state().name() : src);

            Long latencyMs = null;
            Instant started = e.startedAt();
            Instant updated = e.updatedAt();
            if (started != null && updated != null) {
                latencyMs = Math.max(0L, Duration.between(started, updated).toMillis());
            }
            row.put("latencyMs", latencyMs);

            out.add(row);
        }

        // If no v1 entries yet, surface whatever v0 has seen so the UI isn't blank.
        if (out.isEmpty()) {
            // v0 registry doesn't know SQL or latency; just show status + last-updated.
            // (UI will render "-" for missing latency.)
            v0Registry.recent(25).forEach(entry -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("hash", entry.queryId());
                row.put("source", entry.status().name());
                row.put("latencyMs", null);
                out.add(row);
            });
        }

        return out;
    }
}
