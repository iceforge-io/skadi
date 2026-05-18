# Claude Code Prompt — Semantic JDBC Direct Demo Mode

You are working in the `iceforge-io/skadi` repository.

## Goal

Implement a demo-only semantic execution mode that lets the semantic market-risk demo execute directly against Databricks using the existing server-side JDBC infrastructure.

This is a spike/demo path. It must not become the production semantic execution architecture.

## Architectural boundary

Implement in:

```text
skadi-server/src/main/java/org/iceforge/skadi/semantic/demo/
```

Do not put JDBC or Databricks dependencies in:

```text
skadi-semantic/
skadi-sql-gateway/
```

## Files/classes to add or adapt

Suggested classes:

```text
SemanticDemoExecutionMode.java
SemanticDemoExecutionProperties.java
SemanticDemoIntent.java
SemanticDemoRequest.java
SemanticDemoResponse.java
SemanticDemoSqlRenderer.java
DirectJdbcSemanticDemoExecutor.java
SemanticDemoController.java
```

Reuse existing server-side JDBC infrastructure where possible:

```text
SkadiJdbcProperties
JdbcClientFactory
existing datasource-id pattern
```

## Config

Add safe-default config under:

```properties
skadi.semantic.demo.enabled=false
skadi.semantic.demo.execution-mode=disabled
skadi.semantic.demo.datasource-id=default
skadi.semantic.demo.max-rows=100
skadi.semantic.demo.query-timeout-ms=30000
```

Allowed execution modes:

```text
DISABLED
SKADI_SERVER_DELEGATED
JDBC_DIRECT
```

## Endpoint

Add endpoint:

```text
POST /api/semantic/v1/demo/query
```

Request example:

```json
{
  "question": "Show me VaR by legal entity for CIB",
  "contractId": "market-risk-demo",
  "measure": "var",
  "groupBy": ["legal_entity"],
  "filters": {
    "lob": "CIB"
  }
}
```

Response example:

```json
{
  "executionMode": "JDBC_DIRECT",
  "contractId": "market-risk-demo",
  "generatedSql": "SELECT legal_entity, SUM(var_amount) AS var_amount ... LIMIT 100",
  "columns": ["legal_entity", "var_amount"],
  "rows": [
    { "legal_entity": "LE1", "var_amount": 12345.67 }
  ],
  "rowCount": 1,
  "explanation": "Grouped VaR by legal entity using the market-risk demo contract."
}
```

## SQL renderer rules

`SemanticDemoSqlRenderer` must be allowlist-only.

Allowed measures should initially be small, for example:

```text
var
expected_shortfall
sensitivity
```

Allowed dimensions should initially be small, for example:

```text
legal_entity
lob
desk
risk_class
cob_date
```

No raw user text may be interpolated into SQL.

All requested measures, dimensions, and filters must be mapped from known semantic identifiers to known physical columns.

Always append a configured maximum row limit.

## Tests

Add focused tests for:

- safe config defaults
- jdbc-direct mode uses configured datasource id
- SQL renderer accepts known measures/dimensions/filters
- SQL renderer rejects unknown measure
- SQL renderer rejects unknown dimension
- SQL renderer rejects unknown filter key
- generated SQL includes limit/max rows
- ResultSet maps into tabular DTO
- credentials/tokens/passwords are not exposed in response/diagnostics
- no changes under `skadi-sql-gateway`

Run:

```bash
./mvnw verify -pl skadi-server -am
```

If wider module interactions require it, also run:

```bash
./mvnw verify
```

## Documentation

Add a short doc under:

```text
ai/lane-e/semantic-jdbc-demo-mode.md
```

It must state:

- this is demo-only
- it bypasses cache intentionally
- it lives in `skadi-server`
- it does not change `skadi-semantic` module responsibilities
- it does not change `skadi-sql-gateway`
- production convergence requires a future DQR/ADR for semantic cache identity

## Guardrails

Do not modify `skadi-sql-gateway`.

Do not move Databricks/JDBC execution into `skadi-semantic`.

Do not add arbitrary LLM-to-SQL execution.

Do not bypass semantic validation.

Do not use cache/materialized SQL in this demo path.

Do not log credentials.

Do not expose credentials in API responses.
