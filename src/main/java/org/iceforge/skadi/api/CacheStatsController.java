package org.iceforge.skadi.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class CacheStatsController {

    @GetMapping("/cache/stats")
    public Map<String, Object> stats() {
        return Map.of(
                "hits", 0,
                "misses", 0,
                "bytesUsed", 0
        );
    }
}
