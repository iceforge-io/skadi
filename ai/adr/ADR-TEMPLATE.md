# ADR-0001: Short, Specific Decision Title

**Status:** Proposed | Accepted | Superseded  
**Date:** YYYY-MM-DD  
**Owners:** <team or individual>  
**Related ADRs:** ADR-0003, ADR-0007  
**Supersedes:** ADR-0002 (if applicable)

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

- What we are doing
- What we are NOT doing
- Scope boundaries
- Ownership boundaries

This section must be implementation-directive, not vague.

---

## 3. Rationale

Why this approach?

- Trade-offs
- Why alternatives were rejected
- Long-term impact

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
