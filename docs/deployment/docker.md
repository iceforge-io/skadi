# Docker Deployment

This guide covers building and running Skadi SQL Gateway as a Docker container.

---

## Prerequisites

- Docker 24+ with BuildKit enabled
- A Databricks workspace with a SQL Warehouse
- `docker compose` plugin (or Docker Compose v2)

---

## 1. Build the image

From the repository root:

```bash
# Standard build (pulls dependencies from Maven Central)
docker build -f skadi-sql-gateway/Dockerfile -t skadi-sql-gateway:latest .

# With BuildKit cache (faster rebuilds during development)
DOCKER_BUILDKIT=1 docker build \
  -f skadi-sql-gateway/Dockerfile \
  -t skadi-sql-gateway:latest .
```

> The Databricks JDBC driver is **not included** in the image (it is `provided`/`optional`
> in the build). See [Adding the Databricks JDBC driver](#adding-the-databricks-jdbc-driver).

---

## 2. Configure credentials

Create a `.env` file (never commit this):

```bash
# skadi-sql-gateway/.env — local overrides
DATABRICKS_ENABLED=true
DATABRICKS_HOST=<workspace>.azuredatabricks.net
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse-id>
DATABRICKS_TOKEN=dapi...

DBX_CATALOG=main
DBX_SCHEMA=sales

PGWIRE_AUTH_MODE=password
QUERY_TIMEOUT=5m
MAX_CONCURRENT=10
CACHE_TTL=5m
```

---

## 3. Run with Docker Compose

```bash
cd skadi-sql-gateway
docker compose up -d
```

Verify the gateway is healthy:

```bash
# HTTP health check
curl -s http://localhost:8090/actuator/health | python3 -m json.tool

# pgwire smoke test (requires psql)
psql -h localhost -p 15432 -U demo postgres -c "SELECT 1"
```

---

## 4. Run with Docker directly

```bash
docker run -d \
  --name skadi-sql-gateway \
  -p 15432:15432 \
  -p 8090:8090 \
  -e SKADI_SQL_GATEWAY_PGWIRE_ENABLED=true \
  -e SKADI_SQL_GATEWAY_PGWIRE_AUTH_MODE=password \
  -e SKADI_SQL_GATEWAY_DATABRICKS_ENABLED=true \
  -e SKADI_SQL_GATEWAY_DATABRICKS_HOST="<workspace>.azuredatabricks.net" \
  -e SKADI_SQL_GATEWAY_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/<id>" \
  -e SKADI_SQL_GATEWAY_DATABRICKS_TOKEN="dapi..." \
  -e SKADI_SQL_GATEWAY_METADATA_DBX_CATALOG="main" \
  -e SKADI_SQL_GATEWAY_METADATA_DBX_SCHEMA="sales" \
  --restart unless-stopped \
  skadi-sql-gateway:latest
```

---

## Adding the Databricks JDBC driver

The Databricks JDBC driver must be present at runtime for Databricks connectivity.
Download it from: https://www.databricks.com/spark/jdbc-drivers-download

**Option A — Extend the image (recommended for production):**

```dockerfile
FROM skadi-sql-gateway:latest
COPY databricks-jdbc-2.x.x.jar /app/BOOT-INF/lib/
```

Rebuild:
```bash
docker build -t skadi-sql-gateway:with-dbx-driver .
```

**Option B — Mount as a volume:**

Add `JAVA_TOOL_OPTIONS` to point to the mounted jar:

```bash
docker run ... \
  -e JAVA_TOOL_OPTIONS="-cp /drivers/databricks-jdbc.jar" \
  -v /host/path/to/drivers:/drivers:ro \
  skadi-sql-gateway:latest
```

---

## TLS in front of the gateway (recommended for production)

The gateway listens on plain TCP. Wrap it with a TLS-terminating proxy:

**stunnel example** (`/etc/stunnel/skadi.conf`):

```ini
[skadi-pgwire]
accept  = 5432
connect = 127.0.0.1:15432
cert    = /etc/ssl/certs/skadi.crt
key     = /etc/ssl/private/skadi.key
```

**nginx stream proxy example** (`nginx.conf`):

```nginx
stream {
    upstream skadi_pgwire {
        server skadi-sql-gateway:15432;
    }
    server {
        listen 5432 ssl;
        ssl_certificate     /etc/ssl/skadi.crt;
        ssl_certificate_key /etc/ssl/skadi.key;
        proxy_pass skadi_pgwire;
    }
}
```

Point Tableau at the proxy host on port 5432 with `SSL=Required`.

---

## Prometheus / Grafana (optional)

Uncomment the `prometheus` and `grafana` services in `docker-compose.yml`.

Create `monitoring/prometheus.yml`:

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: skadi-sql-gateway
    static_configs:
      - targets: ['skadi-sql-gateway:8090']
    metrics_path: /actuator/prometheus
```

Key metrics to dashboard:
- `skadi_sessions_active` — active client connections
- `skadi_queries_seconds_count` — QPS by cache tier
- `skadi_queries_seconds{quantile="0.99"}` — p99 query latency
- `skadi_query_errors_total` — errors by SQLSTATE

---

## Kubernetes (no Helm chart yet)

A minimal k8s `Deployment` + `Service`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: skadi-sql-gateway
spec:
  replicas: 1
  selector:
    matchLabels:
      app: skadi-sql-gateway
  template:
    metadata:
      labels:
        app: skadi-sql-gateway
    spec:
      containers:
        - name: skadi-sql-gateway
          image: skadi-sql-gateway:latest
          ports:
            - containerPort: 15432
              name: pgwire
            - containerPort: 8090
              name: http
          env:
            - name: SKADI_SQL_GATEWAY_PGWIRE_ENABLED
              value: "true"
            - name: SKADI_SQL_GATEWAY_DATABRICKS_TOKEN
              valueFrom:
                secretKeyRef:
                  name: skadi-secrets
                  key: databricks-token
            # ... other env vars
          livenessProbe:
            httpGet:
              path: /actuator/health
              port: 8090
            initialDelaySeconds: 30
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /actuator/health/pgWire
              port: 8090
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: skadi-sql-gateway
spec:
  selector:
    app: skadi-sql-gateway
  ports:
    - name: pgwire
      port: 15432
      targetPort: 15432
    - name: http
      port: 8090
      targetPort: 8090
```

> Note: Use `readinessProbe` on `/actuator/health/pgWire` so the pod only receives
> traffic once the pgwire listener is bound and ready.
