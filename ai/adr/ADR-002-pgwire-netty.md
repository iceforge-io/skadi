# ADR-0001: Short, Specific Decision Title

**Status:** Accepted  
**Date:** 2026-02-23          
**Owners:** architects  
**Related ADRs:** ADR-0001  

---

## 1. Context

### Problem
What architectural problem are we solving?

### Background
- Relevant existing architecture details
- Known constraints (link `ai/constraints/*`)
- Known invariants (link `ai/invariants/*`)
- Business or operational drivers

---

## 2. Decision

Clearly and explicitly state:

- We will use pwire (PostgreSQL wire protocol) for our SQL gateway service, implemented using Netty.

- This section must be implementation-directive, not vague.

---

## 3. Rationale

Add a dedicated PostgreSQL wire-protocol (“pgwire”) TCP listener to skadi-sql-gateway so tools like psql and Tableau (Postgres/JDBC) can connect, authenticate, send simple queries, and receive results. Use a Netty-based pgwire library so we only implement the small set of messages needed for MVP, while delegating protocol framing/encoding. Back the execution path with Skadi’s existing JDBC abstractions (datasourceId + JdbcConnectionProvider model) and keep session state small and explicit.

---

## 4. Consequences

### Positive
- …

### Negative / Risks
- …

### Operational Impact
- Build changes
- Deployment changes
- Monitoring impact
- Migration complexity

---

## 5. Alternatives Considered

| Option | Why Rejected |
|--------|--------------|
| A      |              |
| B      |              |

---

## 6. Fitness Functions / Enforcement

How do we prevent drift?

- ArchUnit rule?
- Maven Enforcer rule?
- CI validation?
- Contract tests?
- Static analysis?

If no enforcement exists, state explicitly:
> No automated enforcement (review-only)

---

## 7. Migration / Rollout Plan (if applicable)

- Phases
- Backwards compatibility
- Strangler pattern?
- Cutover strategy

---

## 8. Open Questions

- …
