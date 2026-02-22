package org.iceforge.skadi.sqlgateway.api;

import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InfoController {
  @GetMapping("/sql-gateway")
  public Map<String, Object> info() {
    return Map.of(
        "service", "skadi-sql-gateway",
        "status", "ok");
  }
}
