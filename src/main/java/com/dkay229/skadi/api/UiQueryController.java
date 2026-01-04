package com.dkay229.skadi.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class UiQueryController {

    @GetMapping("/queries/recent")
    public List<Map<String, Object>> recent() {
        return List.of();
    }
}
