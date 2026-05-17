# Skadi Platform — Claude Code Guide

## What is Skadi?

A PostgreSQL wire-protocol gateway that lets BI tools (Tableau, DBeaver, psql) connect to Databricks SQL as if it were Postgres. The gateway translates PG dialect SQL to Databricks SQL, handles auth, caches results, and streams rows back over pgwire.

## Project Layout

```
skadi-parent           # Maven parent POM (Java 21, Spring Boot 3.5.6)
├── skadi-core         # Shared domain model (placeholder)
├── skadi-server       # REST/management layer (placeholder)
└── skadi-sql-gateway  # The gateway — all active code lives here
```

### skadi-sql-gateway packages

| Package | Purpose |
|---|---|
| `pgwire` | PostgreSQL wire protocol server (`PgWireServer`, `PgWireSession`) |
| `auth` | Pluggable auth (`AuthProvider`, BCrypt/Plaintext/AllowAll) + `PrincipalPolicy` ACL |
| `dialect` | SQL translation: PG/MySQL → Databricks SQL (`SqlDialectBridge`, `SqlNormalizer`) |
| `executor` | Databricks JDBC connection pool (`DatabricksJdbcExecutor`) |
| `metadata` | `information_schema` facade served from in-memory cache |
| `cache` | TTL query-result cache keyed by `normalizedSql|user` |
| `trace` | Structured session/query trace logging + optional `.jsonl` corpus writer |
| `config` | `SqlGatewayProperties` — all config via `application.yml` |
| `api` | Health + ping REST endpoints |

## Build & Test

```bash
# Full build + all tests
./mvnw verify

# Gateway module only (fastest feedback loop)
./mvnw test -pl skadi-sql-gateway

# Build without tests
./mvnw package -DskipTests

# Run the gateway locally
./mvnw spring-boot:run -pl skadi-sql-gateway
```

Tests live alongside code in `src/test/java`. Use AssertJ assertions. No Mockito — unit tests use real objects or hand-rolled fakes.

## Key Config (`application.yml`)

```yaml
skadi:
  sql-gateway:
    pgwire:
      enabled: true
      port: 15432
      auth:
        mode: trust          # trust | password
        credential-store: plaintext  # plaintext | bcrypt
        users:
          alice: secret
        policies:
          alice:
            allowed-schemas: [sales]
    cache:
      enabled: true
      ttl: 5m
      max-entries: 500
    trace:
      enabled: false
      testdata-path: testdata/tableau-traces
```

## Coding Conventions

- **Java 21**: use records, sealed types, pattern matching where natural
- **No mocks in tests** — real instances or lightweight fakes only
- **Static factory methods** preferred over constructors for providers/registries
- **`Clock` injection** for any time-dependent logic (enables testable TTL)
- **SLF4J + MDC** for logging; always include `session_id` and `client` in session context
- Config is accessed only via `SqlGatewayProperties` records — no `@Value` annotations

## GitHub Workflow

- Repo: `iceforge-io/skadi`, project board: `iceforge-io/projects/1`
- One commit per story, tagged `skadi#<issue>` in commit message
- Close issues after pushing: `gh issue close <N> --repo iceforge-io/skadi`
- Main branch is `main`; do not push to main directly; create and work on feature/<gh-isssue-#>-<meaingful-name> branches. When complete create PRs to ai-main-candidate branch

## Roadmap Lanes

- **Lane A** (POC) — Stories A1–A9: wire protocol, dialect bridge, metadata, caching, tracing ✅
- **Lane B** (Production) — Stories B1–B9: auth, multi-tenant, TLS, observability, packaging
  - B1 Auth ✅ | B2–B9 pending
