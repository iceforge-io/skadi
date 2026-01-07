package org.iceforge.skadi.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class PeerController {

    @GetMapping("/peers")
    public List<Map<String, String>> peers() {
        return List.of(
                Map.of(
                        "id", "local",
                        "host", getHost(),
                        "status", "UP"
                )
        );
    }

    private String getHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
