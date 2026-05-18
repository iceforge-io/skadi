# Plain-English Market Risk Sensitivity Demo Runbook

> **Status: spike/demo only.** This is not a production semantic layer.
> It demonstrates one governed, deterministic interpretation path for a
> fixed set of market-risk sensitivity prompts. No arbitrary NL-to-SQL,
> no LLM integration, no production entitlement engine.

---

## 1. Objective

This demo proves the following pipeline works end-to-end against a
Databricks-backed Skadi deployment:

```
plain English
  → deterministic semantic interpretation  (RuleBasedDemoSemanticIntentAdapter)
  → governed demo contract                 (risk_sensitivity_exposure_grid /
                                            risk_sensitivity_historical_timeseries)
  → whitelisted SQL template               (WhitelistedDemoSemanticSqlRenderer)
  → safe SQL materialization               (DemoSemanticSqlMaterializer)
  → Skadi semantic execution               (QueryExecutionService.sqlOnly)
  → Databricks-backed view
  → DemoSemanticQueryExecutionResponse     (JSON result)
```

The SQL rendered by this path comes from Skadi-owned templates and explicit
allowlists only. Filter values are substituted as SQL literals with strict
single-quote escaping. No user-supplied text reaches the SQL outside of
quoted string literals.

---

## 2. Architecture flow

```
POST /api/semantic/v1/query/demo/execute
  │
  ▼
DemoSemanticQueryController
  │ checks skadi.semantic.demo.enabled=true (→ 404 if false)
  ▼
DemoSemanticQueryExecutionFacade
  ├─ RuleBasedDemoSemanticIntentAdapter.interpret()
  │    Deterministic regex/keyword matching. No LLM.
  │    Returns DemoSemanticQueryInterpretation
  │    (confidence: HIGH / MEDIUM / AMBIGUOUS / UNSUPPORTED)
  │
  ├─ isExecutable() check
  │    UNSUPPORTED or AMBIGUOUS → returns denied response, pipeline stops
  │
  ├─ WhitelistedDemoSemanticSqlRenderer.render()
  │    Validates contract / measure / dimensions / filters against allowlists.
  │    Produces SQL text with :param_name placeholders + params map.
  │    DemoSemanticSqlRenderException → returns denied response, pipeline stops
  │
  ├─ DemoSemanticSqlMaterializer.materialize()
  │    Replaces :param_name placeholders with safe SQL literals.
  │    Strings → 'single-quoted with '' doubled'
  │    Integers / Longs → bare numerics
  │    LocalDate → DATE 'yyyy-MM-dd'
  │    Unknown type or missing param → render failure response, pipeline stops
  │
  └─ QueryExecutionService.execute(sqlOnly(materializedSql))
       Delegates to skadi-server POST /api/v1/queries
       → skadi-server executes against Databricks
       → returns QueryExecutionResult (COMPLETED / CACHE_HIT / FAILED)
         └─ mapped to DemoSemanticQueryExecutionResponse
```

> **Note on SQL materialization:** `QueryExecutionRequest.sqlOnly()` currently
> carries SQL text only — there is no named-parameter carrier in the execution
> seam. The `DemoSemanticSqlMaterializer` is a demo-scoped workaround.
> It is not a general SQL parameter framework.

---

## 3. Runtime endpoint

```
POST /api/semantic/v1/query/demo/execute
Content-Type: application/json
Accept: application/json
```

- Returns **HTTP 404** when `skadi.semantic.demo.enabled=false` (the default).
- Returns **HTTP 200** for all other outcomes — including unsupported prompts,
  ambiguous prompts, and execution failures. The response body carries the
  `validation.allowed` flag and `metadata.executionStatus` to distinguish these.
- No raw SQL, credentials, or stack traces appear in any response field.

---

## 4. Required configuration

### 4a. Demo endpoint properties

```yaml
skadi:
  semantic:
    demo:
      enabled: true                                              # must be true; default is false
      default-limit: 500                                         # rows returned per query
      max-limit: 5000                                            # hard cap; renderer enforces it
      exposure-view: demo.market_risk.v_sensitivity_exposure_demo  # GRID query source
      history-view:  demo.market_risk.v_sensitivity_history_demo   # TIMESERIES query source
```

Set `exposure-view` and `history-view` to the fully-qualified Databricks view
names in your catalog (e.g. `main.market_risk.sensitivity_exposure_demo`).

### 4b. Semantic execution properties

The demo endpoint executes through the existing Skadi semantic execution path.
These must also be configured:

```yaml
skadi:
  semantic:
    execution:
      enabled: true
      server-url: http://localhost:8080   # URL where skadi-server is running
      datasource-id: default             # must match an entry in skadi.jdbc.datasources

  jdbc:
    datasources:
      default:
        jdbc-url: jdbc:databricks://<host>:443/default;httpPath=<path>;transportMode=http;ssl=1
        username: token
        password: <databricks-personal-access-token>
```

> **Do not commit credentials.** Use environment variable injection or a
> secrets manager for `password`. The `password` field can be supplied via
> `SKADI_JDBC_DATASOURCES_DEFAULT_PASSWORD` environment variable with
> Spring Boot's relaxed binding.

### 4c. Complete `application-demo.properties` snippet

```properties
# Demo spike — plain-English sensitivity endpoint
skadi.semantic.demo.enabled=true
skadi.semantic.demo.default-limit=500
skadi.semantic.demo.max-limit=5000
skadi.semantic.demo.exposure-view=main.market_risk.v_sensitivity_exposure_demo
skadi.semantic.demo.history-view=main.market_risk.v_sensitivity_history_demo

# Semantic execution path
skadi.semantic.execution.enabled=true
skadi.semantic.execution.server-url=http://localhost:8080
skadi.semantic.execution.datasource-id=default

# Databricks JDBC (use env var for password in real deployments)
skadi.jdbc.datasources.default.jdbc-url=jdbc:databricks://adb-1234.azuredatabricks.net:443/default;httpPath=/sql/1.0/warehouses/abc123;transportMode=http;ssl=1
skadi.jdbc.datasources.default.username=token
skadi.jdbc.datasources.default.password=${DATABRICKS_TOKEN}
```

---

## 5. Required Databricks views

The renderer maps semantic IDs 1:1 to demo view column names for this spike.
Both views must expose the following logical columns. Column names are
case-sensitive as written below.

### Exposure view (used for GRID queries)

| Column | Type | Notes |
|--------|------|-------|
| `cob_date` | DATE | Close of business date; used as equality filter |
| `org` | STRING | Organisation code (e.g. CIB); filter only |
| `currency` | STRING | Currency code (e.g. USD); filter or GROUP BY |
| `legal_entity` | STRING | Legal entity; filter or GROUP BY |
| `risk_class` | STRING | Risk class (e.g. rates, fx); filter or GROUP BY |
| `risk_factor` | STRING | Risk factor; GROUP BY only in current demo |
| `desk` | STRING | Trading desk; filter or GROUP BY |
| `business_unit` | STRING | Business unit; filter or GROUP BY |
| `product` | STRING | Product; GROUP BY only (not groupable per contract) |
| `delta_exposure` | DECIMAL | Used with `SUM()` aggregate |
| `dv01` | DECIMAL | Used with `SUM()` aggregate |
| `vega` | DECIMAL | Used with `SUM()` aggregate |
| `gamma` | DECIMAL | Used with `SUM()` aggregate |

### History view (used for TIMESERIES queries)

| Column | Type | Notes |
|--------|------|-------|
| `cob_date` | DATE | Time axis; filter range (`between`) and GROUP BY |
| `org` | STRING | Organisation code; filter only |
| `currency` | STRING | Currency code; filter only (not groupable per contract) |
| `risk_class` | STRING | Risk class; filter or GROUP BY |
| `desk` | STRING | Trading desk; filter or GROUP BY |
| `risk_factor` | STRING | Risk factor; filter only (not groupable per contract) |
| `legal_entity` | STRING | Legal entity; filter or GROUP BY |
| `business_unit` | STRING | Business unit; filter only (not groupable per contract) |
| `product` | STRING | Product; not filterable or groupable per contract |
| `delta_exposure` | DECIMAL | Used with `SUM()` aggregate |
| `dv01` | DECIMAL | Used with `SUM()` aggregate |
| `vega` | DECIMAL | Used with `SUM()` aggregate |
| `gamma` | DECIMAL | Used with `SUM()` aggregate |

> Semantic IDs map 1:1 to physical demo view column names for this spike.
> There is no semantic-to-physical mapping layer.

---

## 6. Supported prompts

The adapter uses deterministic keyword/regex matching. Prompts are
case-insensitive and tolerate minor punctuation differences.

### Prompt 1 — Exposure grid: delta by legal entity and risk class

```
Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15
```

| Field | Value |
|-------|-------|
| `contractId` | `risk_sensitivity_exposure_grid` |
| `measure` | `delta_exposure` |
| `groupBy` | `["legal_entity", "risk_class"]` |
| `filters` | `org=CIB`, `currency=USD`, `cob_date=2026-05-15` |
| `outputShape` | `GRID` |
| `confidence` | `HIGH` |

### Prompt 2 — Time series: last 30 days rates DV01 by desk

```
Show the last 30 days of rates DV01 by desk for CIB
```

| Field | Value |
|-------|-------|
| `contractId` | `risk_sensitivity_historical_timeseries` |
| `measure` | `dv01` |
| `groupBy` | `["desk"]` |
| `filters` | `org=CIB`, `risk_class=rates`, `date_range=last_30_days` |
| `outputShape` | `TIMESERIES` |
| `confidence` | `HIGH` |

`date_range=last_30_days` expands at render time to
`cob_date between DATE '<today-30d>' and DATE '<today>'`
using the server's system clock.

### Prompt 3 — Exposure grid: vega by risk factor

```
Show vega exposure by risk factor for CIB on 2026-05-15
```

| Field | Value |
|-------|-------|
| `contractId` | `risk_sensitivity_exposure_grid` |
| `measure` | `vega` |
| `groupBy` | `["risk_factor"]` |
| `filters` | `org=CIB`, `cob_date=2026-05-15` |
| `outputShape` | `GRID` |
| `confidence` | `HIGH` |

### Prompt 4 — Exposure grid: gamma by desk and currency

```
Show gamma exposure by desk and currency for CIB on 2026-05-15
```

| Field | Value |
|-------|-------|
| `contractId` | `risk_sensitivity_exposure_grid` |
| `measure` | `gamma` |
| `groupBy` | `["desk", "currency"]` |
| `filters` | `org=CIB`, `cob_date=2026-05-15` |
| `outputShape` | `GRID` |
| `confidence` | `HIGH` |

### Prompt 5 — Exposure grid: delta by desk

```
Show delta exposure by desk for CIB on 2026-05-15
```

| Field | Value |
|-------|-------|
| `contractId` | `risk_sensitivity_exposure_grid` |
| `measure` | `delta_exposure` |
| `groupBy` | `["desk"]` |
| `filters` | `org=CIB`, `cob_date=2026-05-15` |
| `outputShape` | `GRID` |
| `confidence` | `HIGH` |

### Prompt 6 — Time series: rates DV01 history by desk

```
Show rates DV01 history by desk for CIB over the last 30 days
```

| Field | Value |
|-------|-------|
| `contractId` | `risk_sensitivity_historical_timeseries` |
| `measure` | `dv01` |
| `groupBy` | `["desk"]` |
| `filters` | `org=CIB`, `risk_class=rates`, `date_range=last_30_days` |
| `outputShape` | `TIMESERIES` |
| `confidence` | `HIGH` |

---

## 7. Curl examples

### Exposure grid

```bash
curl -s -X POST http://localhost:8080/api/semantic/v1/query/demo/execute \
  -H 'Content-Type: application/json' \
  -d '{"text":"Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15","limit":500}' \
  | jq
```

### Time series

```bash
curl -s -X POST http://localhost:8080/api/semantic/v1/query/demo/execute \
  -H 'Content-Type: application/json' \
  -d '{"text":"Show the last 30 days of rates DV01 by desk for CIB","limit":500}' \
  | jq
```

Use `"limit"` in the request body to override the default row limit (max 5000).
The field maps to `DemoSemanticQueryRequest.limit`.

---

## 8. Expected response shape

All fields below use the exact names from the DTO records.

```json
{
  "requestText": "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15",

  "interpretation": {
    "originalText": "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15",
    "contractId":   "risk_sensitivity_exposure_grid",
    "measure":      "delta_exposure",
    "groupBy":      ["legal_entity", "risk_class"],
    "filters": [
      { "name": "org",      "value": "CIB" },
      { "name": "currency", "value": "USD" },
      { "name": "cob_date", "value": "2026-05-15" }
    ],
    "outputShape":  "GRID",
    "confidence":   "HIGH",
    "warnings":     []
  },

  "validation": {
    "allowed":    true,
    "reasonCode": "ALLOWED",
    "message":    "Executed successfully.",
    "contractId": "risk_sensitivity_exposure_grid"
  },

  "metadata": {
    "contractId":            "risk_sensitivity_exposure_grid",
    "queryId":               "a3f9c1d2e4b5",
    "cacheStatus":           "ABSENT",
    "rowCount":              0,
    "executionStatus":       "COMPLETED",
    "failureClassification": null
  },

  "columns": [
    { "name": "legal_entity",  "displayName": "legal entity",  "dataType": "STRING",  "semanticRole": "DIMENSION" },
    { "name": "risk_class",    "displayName": "risk class",    "dataType": "STRING",  "semanticRole": "DIMENSION" },
    { "name": "delta_exposure","displayName": "delta exposure","dataType": "DECIMAL", "semanticRole": "MEASURE"   }
  ],

  "rows": []
}
```

> **`rowCount` and `rows`:** The current demo spike returns `rowCount: 0` and
> `rows: []`. The execution service confirms the SQL ran on Databricks, but row
> data is not fetched back through this path in the current architecture.
> Row retrieval requires a separate Arrow IPC / S3 cache fetch step that is not
> wired in this spike.

For a time-series response, `interpretation.outputShape` is `"TIMESERIES"`,
`metadata.contractId` is `"risk_sensitivity_historical_timeseries"`, and the
first column in `columns` is `cob_date` with `semanticRole: "TIME_AXIS"`.

For a **denied** response (unsupported or ambiguous prompt):
```json
{
  "validation": {
    "allowed":    false,
    "reasonCode": "UNSUPPORTED_REQUEST",
    "message":    "Demo supports only market-risk sensitivity queries. ..."
  },
  "metadata": {
    "executionStatus": "FAILED",
    "contractId":      "demo",
    "queryId":         "not-executed"
  },
  "columns": [],
  "rows":    []
}
```

---

## 9. Failure examples

| Scenario | HTTP status | `validation.allowed` | `metadata.executionStatus` | `metadata.failureClassification` |
|---|---|---|---|---|
| Endpoint disabled (`enabled=false`) | 404 | — (no body) | — | — |
| Unsupported prompt | 200 | `false` | `FAILED` | `null` |
| Ambiguous prompt (e.g. "Show exposure") | 200 | `false` | `FAILED` | `null` |
| Render/allowlist failure | 200 | `false` | `FAILED` | `null` |
| Execution unavailable (skadi-server down) | 200 | `true` | `FAILED` | `"UNAVAILABLE"` |
| Execution timed out | 200 | `true` | `FAILED` | `"TIMEOUT"` |
| Circuit breaker open | 200 | `true` | `FAILED` | `"CIRCUIT_OPEN"` |
| Semantic execution disabled | 200 | `true` | `FAILED` | `"DISABLED"` |
| Remote server reported failure | 200 | `true` | `FAILED` | `"REMOTE_ERROR"` |
| Missing Databricks view columns | 200 | `true` | `FAILED` | `"REMOTE_ERROR"` or `"UNEXPECTED"` |

For execution failures, `validation.allowed` is `true` — the prompt was valid
and the SQL was rendered correctly. The failure is in the execution layer.

---

## 10. Demo operator sequence

1. **Pull latest main.**
   ```bash
   git checkout main && git pull origin main
   ```

2. **Create Databricks views** (or confirm existing views match the expected
   column schema in section 5). Note the fully-qualified view names
   (`<catalog>.<schema>.<view>`).

3. **Configure properties.** Create `skadi-server/src/main/resources/application-demo.properties`
   (or `application-local.properties`) with the snippet from section 4c.
   Use environment variables for the Databricks PAT.

4. **Start skadi-server with the demo profile:**
   ```bash
   DATABRICKS_TOKEN=<your-pat> \
     ./mvnw spring-boot:run -pl skadi-server \
       -Dspring-boot.run.profiles=demo
   ```
   Or pass the property directly:
   ```bash
   DATABRICKS_TOKEN=<your-pat> \
     ./mvnw spring-boot:run -pl skadi-server \
       -Dspring-boot.run.arguments="--skadi.semantic.demo.enabled=true \
         --skadi.semantic.demo.exposure-view=main.market_risk.v_sensitivity_exposure_demo \
         --skadi.semantic.demo.history-view=main.market_risk.v_sensitivity_history_demo \
         --skadi.semantic.execution.enabled=true \
         --skadi.jdbc.datasources.default.jdbc-url=jdbc:databricks://... \
         --skadi.jdbc.datasources.default.username=token \
         --skadi.jdbc.datasources.default.password=\${DATABRICKS_TOKEN}"
   ```

5. **Confirm the application started:**
   ```bash
   curl -s http://localhost:8080/api/ping | jq
   # expected: {"status":"ok"} or similar
   ```

6. **Call the exposure-grid prompt:**
   ```bash
   curl -s -X POST http://localhost:8080/api/semantic/v1/query/demo/execute \
     -H 'Content-Type: application/json' \
     -d '{"text":"Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15","limit":500}' \
     | jq
   ```
   Verify: `validation.allowed = true`, `metadata.executionStatus = "COMPLETED"` (or `CACHE_HIT`).

7. **Call the time-series prompt:**
   ```bash
   curl -s -X POST http://localhost:8080/api/semantic/v1/query/demo/execute \
     -H 'Content-Type: application/json' \
     -d '{"text":"Show the last 30 days of rates DV01 by desk for CIB","limit":500}' \
     | jq
   ```
   Verify: `metadata.contractId = "risk_sensitivity_historical_timeseries"`,
   `validation.allowed = true`.

8. **Review the semantic interpretation** in the response's `interpretation`
   field to confirm the adapter correctly resolved the measure, groupBy,
   filters, and outputShape.

9. **Note the guardrails.** The response contains no SQL. The SQL that ran on
   Databricks was produced from a Skadi-owned template allowlisted to:
   - measures: `delta_exposure`, `dv01`, `vega`, `gamma`
   - dimensions: `cob_date`, `legal_entity`, `business_unit`, `desk`,
     `risk_class`, `risk_factor`, `currency`, `product`
   - filters: `org`, `currency`, `cob_date`, `risk_class`, `date_range`

   Filter values are substituted as SQL string literals with `'` doubled — not
   via dynamic JDBC bind params in this spike.

10. **Use the optional helper script** for a single-command demo run:
    ```bash
    bash ai/spikes/run-plain-english-sensitivity-demo-curl.sh
    ```

---

## 11. Known limitations

- **Two query shapes only.** `GRID` (exposure grid) and `TIMESERIES`
  (historical time series). No other shapes are supported.
- **Two contracts only.** `risk_sensitivity_exposure_grid` and
  `risk_sensitivity_historical_timeseries`. Any other contract name is rejected
  by the renderer's allowlist.
- **Configured views only.** The FROM clause is set by
  `skadi.semantic.demo.exposure-view` / `skadi.semantic.demo.history-view`.
  No arbitrary table discovery.
- **Semantic IDs map 1:1 to demo view columns.** There is no semantic-to-physical
  mapping layer in this spike.
- **SQL is whitelisted-template rendered, not LLM-generated.** Templates are
  Skadi-owned and hardcoded in `WhitelistedDemoSemanticSqlRenderer`.
- **SQL is safely materialized for this spike because
  `QueryExecutionRequest.sqlOnly()` has no bind-parameter carrier.** The
  `DemoSemanticSqlMaterializer` replaces `:param_name` placeholders with
  escaped SQL literals. This is demo-scoped and not a general SQL parameter
  framework.
- **No arbitrary NL-to-SQL.** The adapter (`RuleBasedDemoSemanticIntentAdapter`)
  uses deterministic keyword/regex rules. It will return `UNSUPPORTED` for any
  prompt outside the supported grammar.
- **No LLM integration.** Nothing in this path calls an LLM.
- **No production entitlement engine.** The access policy check from the
  contract is not enforced in this demo spike.
- **No full semantic planner.** This is not a general semantic query planner.
- **No buddy-chat UI.** The endpoint is REST-only.
- **No SQL gateway convergence.** `skadi-sql-gateway` is unchanged.
- **`rows` is always empty in current spike.** Row data from Databricks is not
  fetched back through this path. The execution result confirms the SQL ran;
  actual rows require a separate Arrow IPC / S3 cache retrieval step.
- **Not production-ready.** Treat this as a proof-of-concept spike.

---

## 12. Troubleshooting

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| HTTP 404 on `POST /api/semantic/v1/query/demo/execute` | `skadi.semantic.demo.enabled=false` (the default) | Set `skadi.semantic.demo.enabled=true` and restart |
| `validation.allowed=false`, `reasonCode=UNSUPPORTED_REQUEST` | Prompt not recognised by the deterministic grammar | Use one of the supported prompts in section 6; check for typos |
| `validation.allowed=false`, `reasonCode=AMBIGUOUS_REQUEST` | Prompt contains financial keywords but no resolvable measure (e.g. "Show exposure") | Specify the measure explicitly ("delta exposure", "dv01", "vega", or "gamma") |
| `metadata.failureClassification=UNAVAILABLE` | skadi-server is not running or `skadi.semantic.execution.server-url` is wrong | Verify skadi-server is up and reachable; check `server-url` config |
| `metadata.failureClassification=DISABLED` | `skadi.semantic.execution.enabled=false` | Set `skadi.semantic.execution.enabled=true` |
| `metadata.failureClassification=CIRCUIT_OPEN` | Circuit breaker tripped after repeated failures | Wait for the open window to expire, fix the underlying issue, and retry |
| `metadata.failureClassification=TIMEOUT` | Databricks query exceeded timeout | Check Databricks warehouse health; verify the view exists and has data |
| `metadata.failureClassification=REMOTE_ERROR` | Databricks rejected the SQL (e.g. missing view column or wrong view name) | Verify view schema matches section 5; check `exposure-view` and `history-view` config |
| `rows: []` with `executionStatus=COMPLETED` | Expected — rows are not fetched in this spike | The SQL ran; row retrieval is a follow-up story |
| SQL submitted to Databricks still contains `:org` or `:limit` | `DemoSemanticSqlMaterializer.materialize()` is not in the execution path | Check `DemoSemanticQueryExecutionFacade.execute()` — verify `materializedSql` (not `rendered.sql()`) is passed to `QueryExecutionRequest.sqlOnly()` |
| `validation.allowed=false` with "SQL render failed" message | Renderer allowlist rejection after interpretation | The interpretation contained a contract, measure, dimension, or filter not on the allowlist |

---

## 13. Validation commands

```bash
# Verify skadi-semantic module (includes materializer and renderer tests)
./mvnw verify -pl skadi-semantic

# Verify skadi-server module and its dependencies
./mvnw verify -pl skadi-server -am

# Full build
./mvnw verify

# Confirm materializer is wired in the execution path
grep -Rn "DemoSemanticSqlMaterializer" skadi-semantic skadi-server

# Confirm materializedSql (not rendered.sql()) is used for execution
grep -n "materialize" skadi-server/src/main/java/org/iceforge/skadi/semantic/DemoSemanticQueryExecutionFacade.java
grep -n "materializedSql" skadi-server/src/main/java/org/iceforge/skadi/semantic/DemoSemanticQueryExecutionFacade.java
```

---

## 14. Helper script

See `ai/spikes/run-plain-english-sensitivity-demo-curl.sh` for a ready-to-run
demo script that executes both curl examples and prints the responses.

```bash
# Syntax check
bash -n ai/spikes/run-plain-english-sensitivity-demo-curl.sh

# Run (requires skadi-server running with demo enabled)
bash ai/spikes/run-plain-english-sensitivity-demo-curl.sh

# Override base URL
SKADI_BASE_URL=http://myhost:8080 bash ai/spikes/run-plain-english-sensitivity-demo-curl.sh
```
