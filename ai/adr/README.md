# Skadi — Architecture Decision Records

ADRs record significant architectural decisions: what was decided, why, and what was ruled out.
They are immutable once accepted. Superseded ADRs are marked but not deleted.

---

| ADR | Title | Status |
|---|---|---|
| [ADR-001](ADR-001-sql-dialect.md) | SQL Dialect: PostgreSQL/MySQL wire compatibility | Accepted |
| [ADR-002](ADR-002-pgwire-netty.md) | pgwire implementation using Netty | Accepted |
| [ADR-003](ADR-003-skadi-warehouse-lite-future-direction.md) | Skadi Warehouse Lite (cache-aware SQL engine) | Proposed (Future) |
| [ADR-004](ADR-004-semantic-query-layer.md) | Semantic Query Layer (`skadi-semantic`) | Proposed |
| [ADR-005](ADR-005-semantic-contracts.md) | Semantic Contracts (YAML format and governance) | Proposed |
| [ADR-006](ADR-006-dashboard-brick-model.md) | Dashboard Brick Model (composable, governed dashboards) | Proposed |
| [ADR-007](ADR-007-ai-chat-integration.md) | AI Chat Integration Points (semantic-layer-only access) | Proposed |

---

See [ai/architecture-evolution.md](../architecture-evolution.md) for the full evolution
proposal, reusable component analysis, and phased roadmap (Lanes C, D, E).
