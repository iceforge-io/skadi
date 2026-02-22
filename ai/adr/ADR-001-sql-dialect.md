# ADR-0001: Short, Specific Decision Title

**Status:** Accepted
**Date:** 2026-02-20
**Owners:** engineering team, architecture team
**Related ADRs:**
**Supersedes:**

---

## 1. Context

### Problem
Tableau connectivity and caching control - we want to ensure that Tableau can connect to our data sources and that we have control over caching behavior to optimize performance and data freshness.

### Background
- Current state of Tableau connectivity and caching

---

## 2. Decision

Clearly and explicitly state:

- SQL dialect for Tableau connectivity and caching control will be PostgreSQL / MySQL wire-compatibility.
- We are NOT doing: Trino/Presto compatibility, or any other SQL dialects.
- We are not doing Custom Tableau Connector (.taco)

---

## 3. Rationale

Why this approach?
- Simplest sql dialect to implement and maintain
- Most widely supported by Tableau and other BI tools
- Avoids complexity of supporting multiple dialects or custom connectors

---

## 4. Consequences

### Positive
- …

### Negative / Risks
- …

### Operational Impact

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
