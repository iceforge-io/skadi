# Semantic JDBC Demo Guardrails

## This is demo-only

The direct JDBC semantic execution path is for the plain-English market-risk demo only. It is not the long-term production semantic execution architecture.

## Do not change SQL Gateway

No source changes under:

```text
skadi-sql-gateway/
```

The SQL Gateway remains a protocol-facing compatibility surface for Tableau/JDBC clients.

## Do not move JDBC into skadi-semantic

No Databricks or JDBC runtime dependencies should be introduced into:

```text
skadi-semantic/
```

The semantic module must remain suitable for contracts, validation, metadata, and service seams.

## No arbitrary NL-to-SQL

The renderer must be allowlist-only:

- known semantic measure ids
- known semantic dimension ids
- known filter ids
- known physical column mappings
- fixed table/view target or governed contract target

Raw user text must not be interpolated into SQL.

## No cache in this path

`jdbc-direct` intentionally bypasses cache/materialization. That avoids confusing the demo with cache identity and warm/cold behavior.

## No credential leakage

Do not log or return:

- Databricks token
- JDBC password
- full JDBC URL if it includes credentials
- Authorization headers
- secret manager paths that reveal sensitive structure

## Required follow-up

Create a later DQR/ADR for:

```text
canonical semantic cache identity and convergence of semantic JDBC demo execution into Skadi execution/cache boundary
```
