# DQR-004: SQL Gateway and Semantic Execution Convergence

**Status:** Open  
**Raised:** 2026-05-17  
**Related:** ADR-004, ADR-010, DQR-002, Issue #97, dev-status debt item L10  
**Follows:** Lane E E1 semantic execution activation

---

## Scope

Should `skadi-sql-gateway` eventually converge onto the same execution boundary used by semantic execution, or should the gateway continue to own a separate SQL-first execution path?

This DQR is intentionally separate from Issue #97.

Issue #97 activated semantic execution only. It did not modify `skadi-sql-gateway` and did not converge SQL gateway execution.

---

## Impact

The decision affects:

- JDBC pool ownership
- cache ownership and consistency
- query cancellation behavior
- SQL gateway latency and reliability
- production blast radius for Tableau and SQL clients
- whether Skadi has one execution boundary or two long-lived paths

---

## Tie-in

DQR-002 resolved the semantic execution topology with Option 4: partial convergence.

That means:

- semantic execution delegates to `skadi-server`
- SQL gateway remains direct for now
- full convergence remains a future decision

This DQR tracks that future decision explicitly so it does not leak into Lane E E1 implementation work.

---

## Options

| # | Option | Summary | Pros | Cons |
|---|---|---|---|---|
| 1 | Keep dual execution paths | Semantic execution delegates to `skadi-server`; SQL gateway remains direct | Lowest gateway risk; preserves known SQL client behavior | Two JDBC/cache owners persist; duplicated execution concerns remain |
| 2 | Converge SQL gateway through `skadi-server` | Gateway delegates execution to server query execution boundary | One JDBC/cache owner; cleaner platform architecture; resolves L10 more fully | Higher blast radius; requires careful Tableau/client regression testing |
| 3 | Introduce a shared execution library | Both server and gateway call a shared execution module in-process | Avoids network hop; reduces duplicated logic | Still complex; may blur module boundaries; deployment coupling increases |
| 4 | Phased convergence | Add optional gateway delegation behind config, soak, then migrate | Controlled migration; measurable risk reduction | More temporary complexity; requires dual-mode test coverage |

---

## Current Leaning

Option 4 is the safest likely path.

The SQL gateway is a compatibility surface for BI clients. Convergence should be tested behind a feature flag or configuration switch, with SQL client regression coverage and production-like soak before becoming default.

---

## Risk Summary

Premature convergence could break SQL client behavior, cancellation semantics, metadata compatibility, or performance assumptions.

Leaving convergence deferred indefinitely preserves duplicated execution concerns, duplicated cache ownership, and architectural debt L10.

---

## Decision Status

Open.

Do not resolve this inside Issue #97. Resolve only through a future dedicated issue or ADR-backed story.
