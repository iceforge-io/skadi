package org.iceforge.skadi.demo.h2;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

@RestController
@RequestMapping(path = "/api/demo/h2", produces = MediaType.APPLICATION_JSON_VALUE)
@ConditionalOnProperty(prefix = "skadi.demo.h2", name = "enabled", havingValue = "true")
public class DemoH2Controller {

    private final DemoH2Service svc;
    private final DemoH2Properties props;

    public DemoH2Controller(DemoH2Service svc, DemoH2Properties props) {
        this.svc = svc;
        this.props = props;
    }

    @GetMapping("/info")
    public DemoH2Service.DemoH2Info info(
            HttpServletRequest req,
            @RequestHeader(value = "X-Skadi-Demo-Token", required = false) String token) {
        authorize(req, token);
        return svc.info();
    }

    @PostMapping(path = "/execute", consumes = MediaType.APPLICATION_JSON_VALUE)
    public DemoH2Service.DemoH2ExecuteResult execute(
            @RequestBody ExecuteRequest request,
            HttpServletRequest req,
            @RequestHeader(value = "X-Skadi-Demo-Token", required = false) String token) {
        authorize(req, token);
        boolean stopOnError = request != null && Boolean.TRUE.equals(request.stopOnError);
        List<String> statements = request == null ? List.of() : request.statements;
        return svc.execute(statements, stopOnError);
    }

    private void authorize(HttpServletRequest req, String token) {
        if (!props.isAllowRemote()) {
            String addr = req.getRemoteAddr();
            boolean local = "127.0.0.1".equals(addr) || "0:0:0:0:0:0:0:1".equals(addr) || "::1".equals(addr);
            if (!local) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN, "demo H2 endpoints are localhost-only");
            }
        }

        if (props.getAdminToken() != null && !props.getAdminToken().isBlank()) {
            if (token == null || !props.getAdminToken().equals(token)) {
                throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "missing/invalid demo token");
            }
        }
    }

    public static class ExecuteRequest {
        public List<String> statements;
        public Boolean stopOnError;
    }
}
