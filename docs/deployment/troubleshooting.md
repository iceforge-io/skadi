# Troubleshooting — Skadi SQL Gateway

Common issues, their likely causes, and fixes.

---

## Connection issues

### `Connection refused` on port 15432

**Symptoms:** `psql: error: connection to server at "<host>" (x.x.x.x), port 15432 failed: Connection refused`

**Causes / fixes:**

1. Gateway is not running — check `docker ps` or `kubectl get pods`.
2. pgwire is not enabled — verify `SKADI_SQL_GATEWAY_PGWIRE_ENABLED=true`.
3. Port mapping wrong — check `docker ps` shows `0.0.0.0:15432->15432/tcp`.
4. Firewall is blocking — confirm port 15432 is open between Tableau and gateway host.

---

### `SSL connection has been closed unexpectedly` (Tableau Desktop)

**Cause:** Tableau tried to upgrade to SSL but the gateway does not handle TLS natively.

**Fix:** Either:
- Uncheck **Require SSL** in the Tableau connection dialog and connect to port 15432 directly.
- Deploy a TLS-terminating proxy (see [docker.md](docker.md#tls-in-front-of-the-gateway-recommended-for-production)) and point Tableau at the proxy port with SSL enabled.

---

### `FATAL: password authentication failed`

**Cause:** Auth mode is `password` and the supplied credentials do not match any configured user.

**Fixes:**
1. Verify `SKADI_SQL_GATEWAY_PGWIRE_AUTH_USERS_<USERNAME>` is set with the correct password.
2. If using bcrypt, ensure the stored hash was generated correctly
   (`htpasswd -bnBC 12 "" <password> | tr -d ':\n'`).
3. In dev/staging, switch to `trust` mode temporarily to confirm connectivity.

---

### Tableau "An error occurred while communicating with the PostgreSQL data source"

**Cause:** Gateway returned a protocol error or an unexpected SQL error.

**Fix:**
1. Check gateway logs: `docker logs skadi-sql-gateway | tail -100`
2. Look for `ERROR` or `WARN` lines with a `session_id` matching the Tableau connection time.
3. Try the same query from `psql` to narrow down whether it's a Tableau-specific issue.

---

### `psql: error: SSL connection required`

**Cause:** The gateway has `require-ssl=true` but the client connected without SSL.

**Fix:** Either disable `require-ssl`, or connect with `psql "sslmode=require ..."`.

---

## Query errors

### `42601 — syntax error` when Tableau sends a query

**Cause:** A Tableau-generated query uses PostgreSQL syntax that the SQL dialect bridge
does not yet translate.

**Fix:**
1. Capture the raw SQL from the gateway logs (look for `Forwarding query` log lines).
2. Check `SqlDialectBridge` and `SqlNormalizer` for missing transformations.
3. As a workaround, use a Tableau **Custom SQL** data source with Databricks-compatible SQL.

---

### `Query exceeded timeout`

**Cause:** The query ran longer than `SKADI_SQL_GATEWAY_PGWIRE_QUERY_TIMEOUT`.

**Fixes:**
1. Increase the timeout: `SKADI_SQL_GATEWAY_PGWIRE_QUERY_TIMEOUT=10m`.
2. Check whether the Databricks warehouse is cold-starting — first query after idle period
   can take 30–60 seconds on serverless warehouses.
3. Add a `LIMIT` clause or narrow the date range of the query.

---

### All queries fail with `08001 — connection failed`

**Cause:** The gateway cannot connect to Databricks. This usually means the JDBC driver
is missing or credentials are wrong.

**Fixes:**
1. Confirm `DATABRICKS_ENABLED=true` and host/path/token are set.
2. Check the Databricks JDBC driver is present — see
   [docker.md](docker.md#adding-the-databricks-jdbc-driver).
3. Verify the PAT token is valid and not expired.
4. Check outbound TCP 443 from the gateway to the Databricks workspace is not blocked.

---

### `42501 — insufficient privilege`

**Cause:** The Databricks service account (PAT token) does not have `SELECT` on the queried table.

**Fix:** Grant the service account `SELECT` on the target schema:
```sql
GRANT SELECT ON SCHEMA <catalog>.<schema> TO `<service-account-email>`;
```

---

### Queries return stale data

**Cause:** The query result cache is serving old results.

**Fix:**
1. Reduce `SKADI_SQL_GATEWAY_CACHE_TTL` (default 5 min) for time-sensitive workloads.
2. Disable cache temporarily: `SKADI_SQL_GATEWAY_CACHE_ENABLED=false`.
3. Cache is keyed on normalized SQL + username — if the SQL changed slightly (different
   whitespace, case), it may be treated as a new query.

---

## Databricks connectivity

### First query is slow, subsequent queries are fast

**Expected behaviour** on serverless SQL Warehouses — the warehouse cold-starts on the first
query (30–90 seconds) and stays warm for subsequent queries within the auto-stop window.

**Fix:** Set the Databricks warehouse auto-stop to a longer idle timeout during business hours,
or use the Databricks API to pre-warm the warehouse before Tableau sessions begin.

---

### `java.lang.ClassNotFoundException: com.databricks.client.jdbc.Driver`

**Cause:** The Databricks JDBC driver jar is not in the classpath.

**Fix:** The driver is not bundled in the gateway image (it is `provided`/`optional` scope).
Extend the image or mount the jar — see [docker.md](docker.md#adding-the-databricks-jdbc-driver).

---

## Performance

### High p99 latency on cache misses

**Causes:**
1. Databricks warehouse is cold-starting.
2. Query is doing a full table scan on a large table.
3. JDBC connection pool is exhausted — increase `SKADI_SQL_GATEWAY_DATABRICKS_MAX_POOL_SIZE`.

**Checks:**
- Monitor `skadi_queries_seconds{quantile="0.99",cache_tier="miss"}` in Prometheus.
- Check `skadi_sessions_active` to see concurrent session count.

---

### `Too many open connections`

**Cause:** `MAX_CONCURRENT_QUERIES_PER_USER` or JDBC pool is the bottleneck.

**Fix:**
1. Increase `SKADI_SQL_GATEWAY_PGWIRE_MAX_CONCURRENT_QUERIES_PER_USER`.
2. Increase `SKADI_SQL_GATEWAY_DATABRICKS_MAX_POOL_SIZE` (each additional unit requires
   one Databricks warehouse cluster slot).
3. For Tableau Server, reduce the number of concurrent background refresh jobs.

---

## Docker / startup

### Gateway starts but `pgWire` health is `DOWN`

**Cause:** pgwire failed to bind the port, or `pgwire.enabled=false`.

**Checks:**
```bash
docker logs skadi-sql-gateway | grep -i "pgwire\|port\|bind\|error"
curl http://localhost:8090/actuator/health/pgWire
```

**Fix:** Verify `SKADI_SQL_GATEWAY_PGWIRE_ENABLED=true` and that port 15432 is not already
in use on the host (`ss -tlnp | grep 15432`).

---

### `OOMKilled` in k8s

**Cause:** Container ran out of memory.

**Fix:** Increase the container memory limit. The gateway needs at minimum 512 MB; 2 GB
if the cache has many large results. Add JVM heap tuning via `JAVA_TOOL_OPTIONS`:

```bash
JAVA_TOOL_OPTIONS="-Xms256m -Xmx1g"
```

---

## Tableau-specific

### Tableau shows no tables after connecting

**Cause:** `DBX_SCHEMA` is set to a schema that has no tables, or Unity Catalog permissions
exclude the service account from seeing the tables.

**Checks:**
```bash
psql -h localhost -p 15432 -U demo postgres \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema = '<your-schema>'"
```

If empty, the metadata facade has no tables loaded. Check the gateway startup logs for
metadata refresh errors.

---

### Tableau reports "The name was not found" for a column

**Cause:** Tableau caches column metadata and a schema change occurred after the data source
was published.

**Fix:** In Tableau Desktop, right-click the data source → **Refresh**. Re-publish to Tableau Server.

---

### Repeated `SELECT 1` queries from Tableau

**Expected behaviour** — Tableau sends keepalive queries to maintain the connection. These
hit the cache (SQLSTATE `00000`, cache tier `hit`) and are essentially free.

---

## Logging and diagnostics

Enable debug logging for the pgwire package temporarily:

```yaml
# application-local.yml (mount into container)
logging:
  level:
    org.iceforge.skadi.sqlgateway.pgwire: DEBUG
    org.iceforge.skadi.sqlgateway.dialect: DEBUG
```

Or via environment variable:
```bash
LOGGING_LEVEL_ORG_ICEFORGE_SKADI_SQLGATEWAY_PGWIRE=DEBUG
```

The audit log is at logger name `skadi.audit` — enable it at `INFO` to see all connect and
query events:

```bash
LOGGING_LEVEL_SKADI_AUDIT=INFO
```

Each log line includes `session_id`, `user`, `query_id`, `cache_tier`, `rows`, and `latency_ms`
for structured log queries (e.g., in Datadog, CloudWatch Logs Insights, or Loki).
