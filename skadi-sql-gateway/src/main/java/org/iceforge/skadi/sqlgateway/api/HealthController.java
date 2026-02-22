package org.iceforge.skadi.sqlgateway.api;

import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Objects;

/**
 * Simple health endpoint for external callers.
 *
 * <p>Delegates to Spring Boot Actuator health so DOWN/OUT_OF_SERVICE propagates properly.
 */
@RestController
@RequestMapping("/api")
public class HealthController {

    private final HealthEndpoint healthEndpoint;

    public HealthController(HealthEndpoint healthEndpoint) {
        this.healthEndpoint = Objects.requireNonNull(healthEndpoint);
    }

    @GetMapping("/health")
    public Map<String, Object> health() {
        HealthComponent hc = healthEndpoint.health();
        return Map.of(
                "status", hc.getStatus().getCode()
        );
    }
}

