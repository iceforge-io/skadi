# Semantic JDBC Demo Mode

## Status

Demo/spike path. Not the production semantic execution architecture.

## What this is

A demo-only endpoint that accepts structured semantic queries (measure + groupBy + filters)
and executes them directly against Databricks via the server-side JDBC path.

```
POST /api/semantic/v1/demo/query
Content-Type: application/json

{
  "question": "Show me VaR by legal entity for CIB",
  "contractId": "market-risk-demo",
  "measure": "var",
  "groupBy": ["legal_entity"],
  "filters": {"lob": "CIB"}
}
```

## Where it lives

All implementation is in `skadi-server`:

```
skadi-server/src/main/java/org/iceforge/skadi/semantic/demo/
├── SemanticDemoController.java          # REST endpoint
├── SemanticDemoRequest.java             # inbound DTO
├── SemanticDemoResponse.java            # outbound DTO
├── SemanticDemoExecutionMode.java       # enum: DISABLED | SKADI_SERVER_DELEGATED | JDBC_DIRECT
├── SemanticDemoSqlRenderer.java         # allowlist-only SQL generator
├── SemanticDemoSqlRenderException.java  # thrown on allowlist violation
└── DirectJdbcSemanticDemoExecutor.java  # JDBC execution, ResultSet mapping
```

Configuration extends `DemoSemanticProperties` (`skadi.semantic.demo.*`).

## Module boundaries

This implementation **does not** change:
- `skadi-semantic` — no new classes, no changed interfaces
- `skadi-sql-gateway` — untouched

## Cache behaviour

This path bypasses the query cache intentionally. Every request hits the database.
The cache bypass is a deliberate demo design choice, not a bug.
Production convergence requires a future DQR/ADR for semantic cache identity.

## Execution modes

| Mode | Behaviour |
|---|---|
| `disabled` (default) | Endpoint active, returns 200 with empty rows. No DB calls. |
| `skadi_server_delegated` | Not implemented. Returns HTTP 501. |
| `jdbc_direct` | Executes via server-side JDBC using `skadi.jdbc.datasources.<id>`. |

## Activation

```properties
# Enable the demo endpoint family
skadi.semantic.demo.enabled=true

# Switch to JDBC_DIRECT mode
skadi.semantic.demo.execution-mode=jdbc_direct

# Point at a configured datasource
skadi.semantic.demo.datasource-id=default

# Row cap (hard LIMIT in generated SQL)
skadi.semantic.demo.max-rows=100

# JDBC query timeout in milliseconds; 0 = no timeout
skadi.semantic.demo.query-timeout-ms=30000

# Fully-qualified view name for the market-risk demo
skadi.semantic.demo.market-risk-view=demo.market_risk.v_market_risk_demo

# Datasource config (credentials via environment variable, never committed)
skadi.jdbc.datasources.default.jdbc-url=jdbc:databricks://<host>:443/default;...
skadi.jdbc.datasources.default.username=token
skadi.jdbc.datasources.default.password=${DATABRICKS_TOKEN}
```

## SQL renderer allowlists

Allowed measures (map to physical columns):
| Semantic | Physical |
|---|---|
| `var` | `var_amount` |
| `expected_shortfall` | `es_amount` |
| `sensitivity` | `sensitivity_amount` |

Allowed dimensions / filter keys:
`legal_entity`, `lob`, `desk`, `risk_class`, `cob_date`

Filter values are bound via `PreparedStatement` (`?` placeholders). No user text is
concatenated into SQL.

## Security guardrails

- Credentials are never returned in API responses or diagnostics.
- Exception messages are not propagated — only the exception class is logged.
- All SQL identifiers come from operator-controlled config or the renderer allowlists.

## Production convergence

Moving this from demo to production requires at minimum:
1. A DQR/ADR defining semantic cache identity (what constitutes a cache hit for a semantic query).
2. Moving execution out of the demo path and into a governed service layer.
3. Auth/entitlement enforcement (currently absent in the demo path).
