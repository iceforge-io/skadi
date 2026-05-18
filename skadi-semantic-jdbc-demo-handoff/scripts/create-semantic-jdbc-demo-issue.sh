#!/usr/bin/env bash
set -euo pipefail

REPO="${REPO:-iceforge-io/skadi}"

LABELS="lane-e,semantic-execution,server,ai-eligible"

# Create missing labels opportunistically. Ignore failures if labels already exist or permissions are limited.
for label in lane-e semantic-execution server ai-eligible; do
  gh label create "$label" --repo "$REPO" --color "0E8A16" --description "Skadi semantic JDBC demo handoff" >/dev/null 2>&1 || true
done

gh issue create \
  --repo "$REPO" \
  --title "SPIKE: Direct Databricks JDBC execution mode for semantic market-risk demo" \
  --label "$LABELS" \
  --body-file - <<'ISSUE_BODY'
## Purpose

Add a demo-only semantic execution mode that executes generated semantic-demo SQL directly through the Databricks JDBC path instead of delegating through Skadi cache/materialization.

This is intended to prove the plain-English market-risk semantic demo end-to-end without forcing cache identity, async materialization, or SQL gateway convergence into the demo.

## Decision

For the demo path only:

```text
Plain-English request
  -> semantic demo intent adapter
  -> governed semantic contract / validation
  -> constrained SQL renderer
  -> direct Databricks JDBC execution
  -> small tabular response DTO
```

This does not replace the production Skadi execution/cache boundary.

## Scope

Implement in `skadi-server`, not `skadi-semantic`.

Add:

- demo execution mode config
- direct JDBC semantic demo execution adapter
- limited SQL renderer for the existing market-risk demo intents
- small tabular result DTO
- endpoint wiring for the demo path
- focused tests

## Suggested config

```properties
skadi.semantic.demo.execution-mode=jdbc-direct
skadi.semantic.demo.datasource-id=default
skadi.semantic.demo.max-rows=100
skadi.semantic.demo.query-timeout-ms=30000
skadi.semantic.demo.enabled=true
```

Allowed modes:

```text
disabled
skadi-server-delegated
jdbc-direct
```

## Guardrails

Do not modify `skadi-sql-gateway`.

Do not move Databricks dependencies into `skadi-semantic`.

Do not introduce general arbitrary natural-language SQL generation.

Do not bypass semantic validation.

Do not use cache for this demo path.

Do not claim this is production semantic execution convergence.

Do not expose JDBC credentials in responses, logs, diagnostics, or test snapshots.

## Implementation Notes

Prefer using existing server-side JDBC infrastructure:

- `SkadiJdbcProperties`
- `JdbcClientFactory`
- existing datasource-id pattern

The demo adapter should open a connection using datasource id, execute a constrained generated SQL statement, and return a small row/column DTO.

The endpoint should return JSON, not Arrow, for the demo.

## Tests

Add tests covering:

- config defaults are safe
- jdbc-direct mode uses datasource id
- SQL renderer only allows known demo measures/dimensions/filters
- unknown measure is rejected
- unknown dimension is rejected
- generated SQL includes limit/max rows
- JDBC adapter maps ResultSet into tabular DTO
- logs/diagnostics do not expose token/password
- `skadi-sql-gateway` remains unchanged

## Acceptance Criteria

- Plain-English semantic demo can execute directly against Databricks via JDBC.
- Demo response returns a small tabular result.
- Cache/materialization path is bypassed in jdbc-direct mode.
- Existing delegated execution remains available.
- `skadi-semantic` remains Databricks-free.
- `skadi-sql-gateway` remains untouched.
- Maven verify passes.

## Follow-up

Create a later DQR/ADR or issue to converge semantic demo execution back into the Skadi execution/cache boundary once canonical semantic cache identity is defined.
ISSUE_BODY
