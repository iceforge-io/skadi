# Semantic Execution Readiness — Operator Runbook

> Lane F F2 | Issue #112 | Covers `skadi-server` semantic execution delegation path only.
> `skadi-sql-gateway` is unchanged and not referenced here.

---

## Purpose

This runbook explains how to determine whether the semantic query execution delegation
path is ready, how to interpret the diagnostics endpoint, and how to diagnose and
recover from common failure modes.

The semantic execution path delegates queries from `skadi-server` to its own
`POST /api/v1/queries` endpoint via HTTP. It is guarded by a circuit breaker that
limits the blast radius of server-side failures.

---

## Diagnostics Endpoint

```
GET /api/semantic/v1/execution/status
```

Returns a JSON snapshot of activation state, lifetime execution counters, and
circuit-breaker health. This endpoint is read-only — it does not execute queries,
contact Databricks, or modify any state.

### Example response (healthy)

```json
{
  "active": true,
  "serverUrl": "http://skadi-server:8080",
  "datasourceId": "prod-databricks",
  "metrics": {
    "attempts": 142,
    "cacheHits": 98,
    "asyncAccepted": 44,
    "successes": 43,
    "failures": 1,
    "timeouts": 0,
    "errors": 0
  },
  "health": {
    "status": "HEALTHY",
    "readiness": "READY",
    "failureCount": 0,
    "failureThreshold": 3,
    "lastSuccessAt": "2026-05-17T22:48:00Z",
    "lastFailureAt": "2026-05-17T20:13:00Z",
    "lastFailureReason": "unexpected submit response: HTTP 503 state="
  }
}
```

### Example response (circuit open)

```json
{
  "active": true,
  "serverUrl": "http://skadi-server:8080",
  "datasourceId": "prod-databricks",
  "metrics": {
    "attempts": 47,
    "cacheHits": 0,
    "asyncAccepted": 0,
    "successes": 0,
    "failures": 47,
    "timeouts": 0,
    "errors": 0
  },
  "health": {
    "status": "CIRCUIT_OPEN",
    "readiness": "DEGRADED",
    "failureCount": 3,
    "failureThreshold": 3,
    "lastFailureAt": "2026-05-17T23:05:12Z",
    "lastFailureReason": "Connection refused",
    "circuitOpenUntil": "2026-05-17T23:05:42Z"
  }
}
```

---

## Readiness Classification

The `health.readiness` field provides a four-value operational classification derived
from the raw `health.status`. Use `readiness` for dashboards, alerting, and deployment
gates; use `status` for detailed diagnostics.

| `health.status` | `health.readiness` | Meaning |
|---|---|---|
| `HEALTHY` | `READY` | Path is healthy; semantic queries will be delegated. |
| `DISABLED` | `DISABLED` | Disabled by configuration; no calls are attempted. Not a fault. |
| `CIRCUIT_OPEN` | `DEGRADED` | Circuit tripped after repeated failures; probe call pending after `circuitOpenUntil`. |
| `UNAVAILABLE` | `UNAVAILABLE` | Server unreachable or returned an unexpected response. |
| `TIMEOUT` | `UNAVAILABLE` | Server did not complete the query within the configured wait window. |
| `FAILED` | `UNAVAILABLE` | Server returned a `FAILED` or `CANCELED` terminal state. |

### Quick readiness decision tree

```
health.readiness == "READY"       → ✅ OK — delegation is active and healthy
health.readiness == "DISABLED"    → ℹ️  By design — check skadi.semantic.execution.enabled
health.readiness == "DEGRADED"    → ⚠️  Circuit open — see circuitOpenUntil; wait or investigate
health.readiness == "UNAVAILABLE" → ❌  Needs attention — see failure mode section below
```

---

## Configuration Reference

```yaml
skadi:
  semantic:
    execution:
      enabled: true                          # false → DISABLED; no HTTP calls attempted
      server-url: http://localhost:8080      # skadi-server base URL for POST /api/v1/queries
      datasource-id: default                 # which skadi.jdbc.datasources entry to forward
      circuit-breaker:
        enabled: true                        # false → failures recorded but circuit never opens
        failure-threshold: 3                 # consecutive failures before opening circuit
        open-duration-ms: 30000              # ms the circuit stays open before probe (default: 30 s)
```

**Secrets note:** JDBC credentials (`jdbcUrl`, `username`, `password`) from the resolved
datasource are forwarded to `skadi-server` in each request body but are **never** exposed
in the diagnostics endpoint response. The `datasourceId` field is a logical identifier only.

---

## Common Failure Modes

### 1. skadi-server is down

**Symptoms:**
- `health.status`: `UNAVAILABLE` → `CIRCUIT_OPEN` after threshold failures
- `health.readiness`: `UNAVAILABLE` → `DEGRADED`
- `health.lastFailureReason`: `Connection refused` or similar
- `metrics.errors` incrementing

**Resolution:**
1. Check skadi-server process/pod is running and reachable at `serverUrl`.
2. Once server is up, the circuit will self-heal after `open-duration-ms` expires —
   a probe call will transition status back to `HEALTHY` on success.
3. To confirm recovery: poll `GET /api/semantic/v1/execution/status` until
   `health.readiness == "READY"`.

---

### 2. Bad base URL

**Symptoms:**
- `health.readiness`: `UNAVAILABLE`
- `health.lastFailureReason`: `Connection refused` or DNS resolution failure
- `serverUrl` in response does not match the running skadi-server address

**Resolution:**
1. Check `skadi.semantic.execution.server-url` in `application.yml`.
2. Verify the URL is reachable from the server process: `curl <serverUrl>/actuator/health`.
3. Fix the URL and restart `skadi-server`.

---

### 3. Timeout

**Symptoms:**
- `health.status`: `TIMEOUT`
- `health.readiness`: `UNAVAILABLE`
- `health.lastFailureReason`: `query timed out after Nms`
- `metrics.timeouts` incrementing

**Resolution:**
1. Determine whether queries are taking longer than `open-duration-ms` to complete on
   skadi-server (check skadi-server logs for query IDs from `metrics.asyncAccepted`).
2. Increase `skadi.semantic.execution.circuit-breaker.open-duration-ms` if queries
   legitimately require more time.
3. Investigate slow Databricks SQL Warehouse utilization if queries are stalling.

---

### 4. Repeated failures causing circuit open

**Symptoms:**
- `health.status`: `CIRCUIT_OPEN`
- `health.readiness`: `DEGRADED`
- `health.failureCount` equals `health.failureThreshold`
- `health.circuitOpenUntil` is a future timestamp
- `active: true` but calls are short-circuited (no new HTTP requests until probe)

**Behavior:**
The circuit breaker automatically allows one probe call after `circuitOpenUntil` passes.
- If the probe succeeds → `status: HEALTHY`, `readiness: READY`, `failureCount: 0`
- If the probe fails → circuit re-opens for another `open-duration-ms` window

**Resolution:**
1. Identify the root cause from `health.lastFailureReason`.
2. Fix the underlying issue (server down, bad config, slow queries).
3. Wait for `circuitOpenUntil` to pass — the circuit self-heals without a restart.
4. To verify recovery: `GET /api/semantic/v1/execution/status` → `readiness == "READY"`.
5. If you need immediate recovery after fixing the root cause and don't want to wait:
   restart `skadi-server` to reset circuit state (failure count resets to 0 on startup).

---

### 5. Recovery after server returns

**Automatic recovery (recommended):**
The circuit breaker self-heals. After `open-duration-ms` expires, a probe call is
attempted. A successful probe resets failure count and restores `HEALTHY` status.
No operator action required.

**Verify recovery:**
```bash
# Poll until READY
while true; do
  STATUS=$(curl -s http://localhost:8080/api/semantic/v1/execution/status \
    | jq -r '.health.readiness')
  echo "$(date -u +%H:%M:%S) readiness=$STATUS"
  [ "$STATUS" = "READY" ] && break
  sleep 5
done
```

**Manual reset (restart):**
Restarting `skadi-server` resets all circuit-breaker state. Use only when the root
cause is confirmed fixed and waiting for the probe window is not acceptable.

---

### 6. Semantic execution disabled by configuration

**Symptoms:**
- `active: false`
- `health.status`: `DISABLED`
- `health.readiness`: `DISABLED`
- No metrics incrementing

**This is not a fault.** `DISABLED` means `skadi.semantic.execution.enabled=false` was
intentionally set. Semantic queries will return a `FAILED` result without any HTTP call.

**To enable:** Set `skadi.semantic.execution.enabled=true` and restart `skadi-server`.

---

## Deployment Gate Pattern

Use `readiness` to gate deployments or health checks:

```bash
READINESS=$(curl -s http://localhost:8080/api/semantic/v1/execution/status \
  | jq -r '.health.readiness')

case "$READINESS" in
  READY)       echo "Semantic execution is ready"; exit 0 ;;
  DISABLED)    echo "Semantic execution disabled (by config)"; exit 0 ;;
  DEGRADED)    echo "Circuit open — check circuitOpenUntil"; exit 1 ;;
  UNAVAILABLE) echo "Semantic execution unavailable — check lastFailureReason"; exit 1 ;;
  *)           echo "Unknown readiness: $READINESS"; exit 1 ;;
esac
```

---

## Guardrails

The following are **explicitly out of scope** for this runbook and for all Lane F work:

- No SQL gateway convergence (DQR-004 remains open)
- No `skadi-sql-gateway` changes
- No buddy-chat runtime
- No LLM integration
- No SQL generation from semantic contracts
- No UI runtime components
- No secrets exposed in diagnostics (JDBC credentials are forwarded in request bodies
  only and never appear in diagnostic responses)

---

## Related

- F1 implementation: `SemanticExecutionCircuitBreaker`, `SemanticExecutionHealthStatus`, `SemanticExecutionHealthSnapshot`
- F2 implementation: `SemanticExecutionReadiness`
- Diagnostics endpoint: `SemanticExecutionDiagnosticsController`
- Configuration: `SemanticExecutionProperties`
- DQR-002: semantic execution delegation resolved (Option 4 — partial convergence)
- DQR-004: full SQL gateway convergence (open; future scope)
