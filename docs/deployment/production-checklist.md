# Production Checklist — Skadi SQL Gateway

Work through this checklist before promoting the gateway to production traffic.
Each item links to the relevant guide section.

---

## Infrastructure

- [ ] Gateway deployed with `restart: unless-stopped` (Docker) or `restartPolicy: Always` (k8s)
- [ ] At least **512 MB RAM** allocated; **2 GB recommended** for cache + JDBC pool
- [ ] CPU: at minimum 1 vCPU; 2–4 vCPU for production under concurrent load
- [ ] Gateway host can reach `<workspace>.azuredatabricks.net:443` outbound

---

## Configuration

- [ ] `SKADI_SQL_GATEWAY_PGWIRE_ENABLED=true`
- [ ] `SKADI_SQL_GATEWAY_DATABRICKS_ENABLED=true`
- [ ] `SKADI_SQL_GATEWAY_DATABRICKS_HOST` set to correct workspace hostname
- [ ] `SKADI_SQL_GATEWAY_DATABRICKS_HTTP_PATH` set to correct warehouse HTTP path
- [ ] `SKADI_SQL_GATEWAY_DATABRICKS_TOKEN` injected via secret manager (not plain env var)
- [ ] `SKADI_SQL_GATEWAY_PGWIRE_AUTH_MODE=password` (not `trust`)
- [ ] At least one user configured in `SKADI_SQL_GATEWAY_PGWIRE_AUTH_USERS_<NAME>`
- [ ] Passwords use bcrypt: `SKADI_SQL_GATEWAY_PGWIRE_AUTH_CREDENTIAL_STORE=bcrypt`
- [ ] `SKADI_SQL_GATEWAY_PGWIRE_QUERY_TIMEOUT` set (e.g., `5m`)
- [ ] `SKADI_SQL_GATEWAY_PGWIRE_MAX_CONCURRENT_QUERIES_PER_USER` set (e.g., `10`)
- [ ] `SKADI_SQL_GATEWAY_DATABRICKS_MAX_POOL_SIZE` tuned for expected concurrency (default 5)
- [ ] `SKADI_SQL_GATEWAY_METADATA_DBX_CATALOG` set to correct Unity Catalog name
- [ ] `SKADI_SQL_GATEWAY_METADATA_DBX_SCHEMA` set to the schema to expose

---

## Security

- [ ] Databricks PAT token stored in secret manager (k8s Secret, Vault, AWS Secrets Manager, etc.)
- [ ] Databricks PAT token has minimum required permissions (SELECT on target schema)
- [ ] Gateway not directly accessible from the public internet (firewall / security group)
- [ ] Port 15432 open only to known Tableau Server / Bridge IP ranges
- [ ] Port 8090 (Actuator) accessible only from internal monitoring systems
- [ ] TLS-terminating proxy (stunnel or nginx) deployed in front of port 15432 — see [docker.md](docker.md#tls-in-front-of-the-gateway-recommended-for-production)
- [ ] Tableau connection configured with `SSL=Required` pointing at TLS proxy
- [ ] Auth mode `trust` is **not** used
- [ ] `.env` file (if used) not committed to git; added to `.gitignore`
- [ ] `PrincipalPolicy` ACLs configured to restrict users to their authorized schemas

---

## Health checks

- [ ] `GET /actuator/health` returns `{"status":"UP"}`
- [ ] `GET /actuator/health/pgWire` returns `{"status":"UP","details":{"port":15432,...}}`
- [ ] `GET /ping` returns `ok`
- [ ] k8s / load balancer readiness probe configured on `/actuator/health/pgWire`
- [ ] k8s / load balancer liveness probe configured on `/actuator/health`

---

## Connectivity smoke test

```bash
# From a machine with psql installed and network access to the gateway

# 1. Basic connectivity (trust mode / known user)
psql -h <gateway-host> -p 15432 -U <user> postgres -c "SELECT 1"

# 2. Information schema (metadata facade)
psql -h <gateway-host> -p 15432 -U <user> postgres \
  -c "SELECT table_name FROM information_schema.tables LIMIT 5"

# 3. Real Databricks query (replace with a table in your schema)
psql -h <gateway-host> -p 15432 -U <user> postgres \
  -c "SELECT COUNT(*) FROM <your-schema>.<your-table>"
```

Or run the full smoke-test script:

```bash
./scripts/smoke-test.sh --host <gateway-host> --user <user> --password <password>
```

---

## Observability

- [ ] Prometheus scraping `/actuator/prometheus` — see [docker.md](docker.md#prometheus--grafana-optional)
- [ ] Grafana dashboards imported or created for:
  - `skadi_sessions_active` — active connections
  - `skadi_queries_seconds_count` — QPS by cache tier
  - `skadi_queries_seconds{quantile="0.99"}` — p99 query latency
  - `skadi_query_errors_total` — error rate by SQLSTATE
- [ ] Alerting configured on:
  - `skadi_query_errors_total` rate spikes
  - `skadi_queries_seconds{quantile="0.99"}` exceeding SLA threshold
  - Gateway health endpoint returning non-`UP`

---

## Tableau connectivity

- [ ] Tableau Desktop can connect to the gateway (see [tableau-server.md](tableau-server.md))
- [ ] Published data sources on Tableau Server refresh successfully
- [ ] Tableau Bridge agent running and connected to Tableau Cloud site (if applicable — see [tableau-bridge.md](tableau-bridge.md))
- [ ] SSL mode configured correctly in Tableau data source connections

---

## Databricks SQL Warehouse

- [ ] Warehouse type is **Serverless** or **Pro** (Classic warehouses have longer cold-start times)
- [ ] Warehouse auto-stop configured appropriately (consider keeping alive during business hours)
- [ ] Warehouse cluster policy allows the service account to run queries
- [ ] Unity Catalog permissions: `SELECT` on the target schema's tables; `USE SCHEMA`; `USE CATALOG`

---

## Runbook reference

| Scenario | Action |
|---|---|
| Gateway won't start | Check `docker logs skadi-sql-gateway`; verify JDBC jar present and credentials correct |
| Tableau "Connection Error" | Check pgwire port reachability; verify auth mode and credentials |
| All queries timing out | Check Databricks warehouse status; increase `QUERY_TIMEOUT` or pool size |
| High error rate | Check `skadi_query_errors_total` labels; review audit log for `SQLSTATE` codes |
| Cache not working | Verify `CACHE_ENABLED=true`; check `skadi_queries_seconds` `cache_tier` labels |
| JDBC driver missing | Extend image or mount driver — see [docker.md](docker.md#adding-the-databricks-jdbc-driver) |
