# Semantic JDBC Demo Mode Runbook

## Intent

Add a narrow semantic demo path that proves plain-English market-risk requests can be resolved into governed semantic intent, rendered into constrained SQL, executed against Databricks, and returned as a small JSON table.

## Call flow

```text
POST /api/semantic/v1/demo/query
  -> SemanticDemoController
  -> SemanticDemoIntent adapter / request mapper
  -> SemanticDemoSqlRenderer
  -> DirectJdbcSemanticDemoExecutor
  -> JdbcClientFactory
  -> Databricks JDBC driver
  -> ResultSet -> SemanticDemoResponse
```

## Module ownership

| Module | Responsibility |
| --- | --- |
| `skadi-semantic` | Contracts, DTOs, validation concepts, service seams |
| `skadi-server` | Demo controller, SQL renderer, direct JDBC demo executor |
| `skadi-sql-gateway` | Unchanged; SQL wire protocol compatibility surface |

## Why direct JDBC for the demo

The semantic demo is meant to prove semantic mapping and governed request execution. Cache identity, async materialization, S3/local cache behavior, SQL gateway convergence, and production execution ownership are separate architectural concerns.

## Why not put this in `skadi-semantic`

`skadi-semantic` should remain a clean contract and semantic model module. Databricks/JDBC runtime concerns belong in `skadi-server` for this spike because server-side JDBC infrastructure already exists there.

## Runtime config

```properties
skadi.semantic.demo.enabled=true
skadi.semantic.demo.execution-mode=jdbc-direct
skadi.semantic.demo.datasource-id=default
skadi.semantic.demo.max-rows=100
skadi.semantic.demo.query-timeout-ms=30000
```

## Example request

```bash
curl -s -X POST http://localhost:8080/api/semantic/v1/demo/query \
  -H 'Content-Type: application/json' \
  -d '{
    "question": "Show me VaR by legal entity for CIB",
    "contractId": "market-risk-demo",
    "measure": "var",
    "groupBy": ["legal_entity"],
    "filters": {
      "lob": "CIB"
    }
  }' | jq
```

## Validation expectations

The renderer must reject:

- unknown measure
- unknown dimension
- unknown filter key
- unsupported contract
- empty group-by if the demo endpoint requires grouped output
- any raw SQL submitted by the caller

## Result expectations

Return a small JSON response:

```json
{
  "executionMode": "JDBC_DIRECT",
  "contractId": "market-risk-demo",
  "generatedSql": "...",
  "columns": ["legal_entity", "var_amount"],
  "rows": [],
  "rowCount": 0,
  "explanation": "Grouped VaR by legal entity using the market-risk demo contract."
}
```

## Verification

```bash
./mvnw verify -pl skadi-server -am
```

Optional full repo verification:

```bash
./mvnw verify
```
