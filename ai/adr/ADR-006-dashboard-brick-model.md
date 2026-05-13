# ADR-006: Dashboard Brick Model

**Status:** Proposed
**Date:** 2026-05-13
**Owners:** engineering, product
**Related ADRs:** ADR-004, ADR-005, ADR-007
**Supersedes:** —

---

## 1. Context

### Problem

Skadi's current UI surface (`skadi-server/src/ui/SkadiMonitoringPage.tsx`) is a single
operational monitoring page — it is not a governed, composable dashboard platform.

As Skadi evolves to serve analysts, risk managers, and AI-assisted workflows, it needs a
dashboard layer that:
- Is governed (reviewed, versioned, published — not edited ad-hoc)
- Is composable (dashboards assembled from reusable, independently governed building blocks)
- Integrates with the semantic layer (queries are metric/dimension selections, not raw SQL)
- Supports AI chat as a first-class brick type (not a separate application)
- Is operationally simple (no full BI tool build — no drag-and-drop editor in Phase 1)

The alternative — continuing with Tableau as the only dashboard surface — leaves AI chat,
operational dashboards, and semantic governance unreachable.

### Background

- The semantic query layer (ADR-004) produces Arrow results; a rendering layer is needed to turn those results into visualisations
- `skadi-server/src/ui/` already contains a React app skeleton — the brick runtime builds on this
- The existing `GatewayMetrics` Prometheus metrics are already consumed by an operational dashboard; the brick model generalises this
- dbt, Metabase, and Evidence.dev have each explored config-driven dashboard approaches; Evidence.dev (Markdown + SQL components) is the closest analogue to what is proposed here

---

## 2. Decision

Dashboards are composed of **bricks**. Bricks are the atomic governed unit of the dashboard
platform.

### 2.1 What is a brick

A brick is a YAML-defined artifact containing:

```yaml
# skadi-semantic/bricks/risk_pnl_by_book.yaml

brick: risk_pnl_by_book
version: "1.0"
label: "PnL by Book (Last 30 days)"
description: "Daily PnL aggregated by trading book for the trailing 30-day window"

query:
  dataset:    mxl_risk
  metrics:    [pnl]
  dimensions: [book, cob_date]
  filters:
    - dimension: cob_date
      operator:  gte
      value:     "$today-30d"     # relative date macro

visualisation:
  type:    bar_chart              # bar_chart | line_chart | table | metric_card | chat
  x_axis:  cob_date
  y_axis:  pnl
  series:  book
  sort:    desc

cache:
  ttl:  "1h"                     # overrides dataset contract default

access:
  roles: [risk_analyst, risk_viewer]
```

Key properties:
- **Declarative**: a brick is configuration, not code. No JavaScript or SQL inside a brick file.
- **Self-contained**: a brick carries its own query, visualisation config, access policy, and cache hint.
- **Semantically-bound**: `query.dataset` and `query.metrics` reference semantic contract names (ADR-005). No raw SQL.
- **Versioned**: `version` field; breaking changes require a major bump.
- **Access-controlled**: `access.roles` is enforced by the brick runtime before the query is issued.

### 2.2 What is a dashboard

A dashboard is an ordered list of brick references with layout metadata:

```yaml
# skadi-semantic/dashboards/risk_morning_pack.yaml

dashboard: risk_morning_pack
version:   "1.0"
label:     "Risk Morning Pack"
layout:    grid                  # grid | stacked

bricks:
  - brick: risk_pnl_summary      # metric_card brick
    grid:  { col: 0, row: 0, w: 4, h: 2 }
  - brick: risk_pnl_by_book      # bar_chart brick
    grid:  { col: 4, row: 0, w: 8, h: 4 }
  - brick: risk_greeks_table     # table brick
    grid:  { col: 0, row: 4, w: 12, h: 6 }

access:
  roles: [risk_analyst, risk_viewer]
```

A dashboard is a composition artifact — it holds no query logic of its own.

### 2.3 Brick types (Phase 1)

| Type | Description |
|---|---|
| `metric_card` | Single aggregated value with label and trend indicator |
| `table` | Tabular result with sortable columns |
| `bar_chart` | Bar chart; supports stacking and grouping by series |
| `line_chart` | Time-series or ordered line chart |
| `chat` | AI chat input brick — see ADR-007 |

### 2.4 Brick runtime

The **brick runtime** is a React component library (`skadi-ui-bricks` npm package). Given a
brick YAML definition and an auth token, it:

1. Fetches the brick definition from the **brick registry** (`GET /bricks/{id}`)
2. Issues the semantic query via the **semantic layer** (`POST /semantic/v1/query`) with the user's identity
3. Renders the result using the specified `visualisation.type`
4. Caches the rendered result in the browser for the `cache.ttl` duration (in addition to server-side caching)

The brick runtime does not generate SQL. It does not contain business logic. It is a rendering layer.

### 2.5 Governance workflow

Bricks and dashboards are code artifacts. The publish lifecycle:

```
Draft (local YAML)
      ↓  PR opened
Review (peer + data governance review)
      ↓  PR merged to main
Published (brick registry picks up on deploy)
```

Bricks cannot be created or edited through the UI. Edits require a PR.
The UI shows only published bricks.

### 2.6 What is NOT in scope

- Drag-and-drop dashboard builder (Phase 1 is config-first)
- Server-side chart rendering for PDF/email export (Phase 1 browser-only)
- Real-time / streaming data bricks (Phase 1 is request/response only)
- Embedded Tableau replacement (Tableau remains the raw SQL BI path)

---

## 3. Rationale

**Why config-driven rather than code-driven (React components per brick)?**
Config-driven bricks are reviewable by non-engineers (governance teams, data owners can read
YAML). Code-driven bricks require a frontend developer per brick and make governance review
harder. The analogy is Terraform: infrastructure is config, not imperative code.

**Why not build on an existing tool (Metabase, Superset, Grafana)?**
These tools have their own data source models, auth systems, and embedding constraints. None
of them integrate with the Skadi semantic layer natively. Adopting them would require Skadi
to wrap or duplicate its governance model in the tool's config. The brick model keeps governance
in one place.

**Why no drag-and-drop editor in Phase 1?**
A dashboard editor is a significant product investment. Phase 1 validates the brick model and
semantic layer integration. If the config-driven approach has adoption issues, an editor can be
added later without changing the underlying model — the YAML format is the interface, not the
editor.

---

## 4. Consequences

### Positive
- Governance review happens at PR time — no surprise dashboard changes in production
- Every brick is independently versioned, testable, and observable
- AI chat is a brick type — it integrates into the governed platform rather than running outside it
- Brick cache entries are shared with all other semantic query callers (dashboard, AI, direct API)

### Negative / Risks
- Engineers must write YAML for every new visualisation — this is intentional friction; if it becomes a bottleneck, an editor can be added
- The brick runtime requires a React rendering environment — not available in all embedding contexts
- Breaking changes to the brick schema require versioning coordination between registry and runtime

### Operational Impact
- New deployable: `skadi-ui-bricks` npm package (internal registry or bundled)
- Brick and dashboard YAML files must be mounted into the `skadi-semantic` container alongside contracts
- A brick registry endpoint (`GET /bricks/{id}`, `GET /dashboards/{id}`) is added to `skadi-semantic`

---

## 5. Alternatives Considered

| Option | Why Rejected |
|--------|--------------|
| Grafana dashboards over Prometheus | Grafana is metrics/time-series focused, not SQL result focused; would need custom data source plugin |
| Superset / Metabase embedding | External tool with its own auth and data model; semantic governance would be duplicated |
| React components per brick (code-driven) | Not reviewable by non-engineers; governance hard to enforce; every brick is custom code |
| Tableau embedding for all dashboards | Tableau does not consume semantic queries; raw SQL only; no AI chat integration |

---

## 6. Fitness Functions / Enforcement

- CI validates all `bricks/*.yaml` and `dashboards/*.yaml` against their JSON schemas
- A brick that references a non-existent dataset or metric fails validation in CI
- The brick runtime must not render a brick to a principal who does not have access (enforced by the semantic layer, not by the runtime)
- Brick definition format is versioned; a test asserts that a v1 brick renders correctly with the current runtime

> No automated enforcement of "bricks must not contain SQL" in Phase 1. Review-only.

---

## 7. Migration / Rollout Plan

1. **D1 (Lane D):** Define and validate `brick.schema.json`; write 3 example bricks for the risk morning pack
2. **D2 (Lane D):** Brick registry endpoint in `skadi-semantic`; bricks loaded at startup alongside contracts
3. **D3 (Lane D):** `skadi-ui-bricks` React library; renders `metric_card` and `table` brick types against live semantic layer
4. **D4 (Lane D):** `bar_chart` and `line_chart` brick types; `DashboardRuntime` React app
5. **D5 (Lane D):** Governance workflow documentation; publish lifecycle; PR template for brick contributions
6. `chat` brick type is delivered in Lane E (ADR-007)

---

## 8. Open Questions

- Q1: Should bricks support parameterised filters (e.g., a date-range picker that the viewer controls)?
- Q2: Is the brick registry a read-only endpoint (load from YAML at startup) or does it have a write path for brick registration via API?
- Q3: Should dashboards support conditional brick visibility (show this brick only if the user has role X)?
- Q4: What is the versioning contract for the `skadi-ui-bricks` npm package — how do breaking runtime changes roll out to deployed dashboards?
