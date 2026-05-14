# skadi-semantic

Lane C candidate module — semantic contract skeletons.

## What this module is

A plain Java library that defines the **semantic contract vocabulary** for the Skadi
platform. It contains immutable records and interfaces that describe datasets, measures,
dimensions, access policies, and cache policies.

## What this module is NOT

| Not present | Reason |
| --- | --- |
| Semantic planner / rule engine | Out of scope for Lane C — see ADR-008 |
| SQL generation or execution | Delegated to `skadi-server` via the executor interface |
| YAML / JSON Schema loading | Format decision deferred — see DQR-001 |
| Runtime contract registry population | Deferred to post-Lane C |
| Spring beans or `@Component` annotations | Plain library; no Spring context |
| REST endpoints or controllers | No runtime behaviour |
| Databricks, S3, or Tableau integration | No external system calls |
| UI bricks or AI chatbot | Future lanes D and E |

## Package layout

```
org.iceforge.skadi.semantic          # module root
org.iceforge.skadi.semantic.contract # dataset vocabulary records (C2.2, C2.3)
org.iceforge.skadi.semantic.registry # ContractRegistry interface (C2.4)
```

## Lane C story sequence

| Story | Issue | Deliverable |
| --- | --- | --- |
| C2.1 | #55 | This module/package boundary ← **you are here** |
| C2.2 | #56 | Core semantic contract records |
| C2.3 | #57 | Access policy and cache policy skeletons |
| C2.4 | #58 | `ContractRegistry` interface |
| C2.5 | #59 | JSON serialization fixtures and tests |
| C2.6 | #60 | C2 documentation and implementation notes |

## Architecture references

- `ai/adr/ADR-008-lane-c-scope.md` — Lane C scope boundary
- `ai/adr/ADR-009-contracts-before-planning.md` — why contracts come before the planner
- `ai/adr/ADR-010-cache-positioning.md` — cache contract boundary
- `ai/dqr/DQR-001-contract-definition-format.md` — open question: contract storage format
- `ai/architecture/platform-boundary-model.md` — full platform boundary diagram
