# DQR-003: Lineage and Market Risk Brain Integration Seams

**Status:** Open
**Raised:** 2026-05-14
**Blocking:** Lane E (AI Chat Buddy); external BCBS239 lineage integration
**Related:** ADR-007, ADR-008, ADR-010
**Resolves in:** Lane E planning or a dedicated external integration story

---

## Scope

How should future BCBS239 regulatory lineage and Market Risk Brain (unstructured knowledge)
integrations attach to Skadi without contaminating Lane C scope?

This question has two distinct sub-questions:

**A. BCBS239 Lineage:**
How should Skadi's audit trail be consumed by a downstream BCBS239 lineage database?
Should Skadi push lineage events, expose a lineage API, or rely on an external subscriber
consuming the existing audit log?

**B. Market Risk Brain:**
How should unstructured risk knowledge (stress scenarios, model documentation, expert
judgement) be injected into the AI Chat Buddy intent resolution flow?

Lane C should preserve seams for both integrations without implementing either.

---

## Impact

**BCBS239 Lineage impact:**

- If Skadi must emit explicit lineage events, the audit log schema and emission mechanism
  need to be designed before Lane E
- If a subscriber model is used, the audit log fields (`query_id`, `principal`, `source`,
  dataset name, timestamp) must be stable and not changed without a migration plan
- Regulatory obligation: BCBS239 requires data lineage from source to report; Skadi is
  on the critical path for risk data lineage if it sits between Databricks and risk reports

**Market Risk Brain impact:**

- If MRB context is injected at intent resolution time (Lane E), the `POST /ai/v1/intent`
  endpoint needs a `ContextProvider` interface that MRB can implement
- If MRB context is embedded in contracts (static), it must be updated whenever model
  parameters change — coupling contract governance to model risk management workflows
- If MRB queries Skadi directly, it needs its own authentication identity and access
  policy, creating a new principal type

---

## Tie-in

- **ADR-007 (AI Chat Integration, Lane E)** describes `ContextProvider` as the MRB seam:
  "Market Risk Brain context is injected into the LLM system prompt by the intent resolver
  via a `ContextProvider` interface." ADR-007 is Proposed — the interface shape is not
  yet final.
- **Lane C C1 platform boundary model** records the BCBS239 seam as: "BCBS239 lineage
  integration consumes these [audit log] events as an external subscriber — no change to
  the audit log schema is required in Lane C."
- **Current audit log** (`AuditLog` in `skadi-sql-gateway`) records: `query_id`,
  `principal`, SQL text (redacted), execution time, datasource. It does not yet record
  `source` (`direct` | `brick` | `ai_chat`) or `dataset_name` — these are future semantic
  fields that the audit log would need when the semantic path is activated.
- **ADR-010** (cache positioning) notes the `source` field is needed for lineage
  attribution to distinguish query origins.

---

## Sub-question A: BCBS239 Lineage Seam Options

| # | Option | Summary | Pros | Cons |
| --- | --- | --- | --- | --- |
| 1 | **External subscriber on audit log** (current C1 leaning) | BCBS239 system reads `AuditLog` events (file, log stream, or message broker) | No Skadi changes required in Lane C; audit log is already emitted; lineage system owns its own data model | Audit log must be enriched with `source` and `dataset_name` before the semantic path lands; coupling to audit log format |
| 2 | **Skadi emits dedicated lineage events** | A `LineagePublisher` interface in `skadi-semantic` emits structured events to a broker (Kafka, SQS) | Clean, typed lineage model; decoupled from audit log; can include semantic context (metric names, contract version) | Requires broker infrastructure; new interface to define and maintain; adds a dependency to the semantic module |
| 3 | **Lineage API on `skadi-semantic`** | `GET /lineage/query/{query_id}` returns lineage for a given query | Pull model; BCBS239 system queries on demand | Cache required to retain lineage beyond in-memory; queries with no semantic path have no lineage record here |
| 4 | **Lineage embedded in query response** | Each query response includes a `lineage` block | Simple; no separate integration point | Lineage consumers must parse query responses; not appropriate for async lineage reporting |

---

## Sub-question B: Market Risk Brain Seam Options

| # | Option | Summary | Pros | Cons |
| --- | --- | --- | --- | --- |
| 1 | **`ContextProvider` interface at intent resolution** (ADR-007 proposal) | Lane E `POST /ai/v1/intent` calls `ContextProvider.getContext(principal, query)` before building LLM prompt | Clean separation; MRB implements the interface; Skadi does not depend on MRB | Interface shape must be agreed before Lane E starts; MRB must be available at intent resolution time |
| 2 | **MRB context embedded in contracts** | Risk model descriptions and stress scenario summaries are fields in `SemanticContract` | No new interface; contracts are already the vocabulary source | Tightly couples model risk management to contract governance workflow; MRB updates require contract PRs |
| 3 | **MRB queries Skadi semantic layer directly** | MRB acts as a semantic API client | MRB retrieves governed data using standard API | Creates a circular dependency if MRB context is needed to answer MRB's own query; MRB must have a Skadi identity |
| 4 | **MRB context injected as a system prompt prefix** | MRB provides a text block that is prepended to every intent resolution prompt | Simplest implementation; no interface required | Not structured; hard to test; MRB cannot dynamically scope its context to the user's query |

---

## Current Leaning

**Sub-question A (Lineage):** Option 1 (external subscriber on audit log) is the current
leaning for Lane C, consistent with the C1 platform boundary model. The prerequisite is
enriching the audit log with `source` and `dataset_name` when the semantic path is
activated (post-Lane C). No lineage infrastructure is introduced in Lane C.

**Sub-question B (Market Risk Brain):** Option 1 (`ContextProvider` interface) is the
current leaning per ADR-007. The interface is defined in Lane E, not Lane C. Lane C must
not introduce a `ContextProvider` interface — the seam is preserved by not hardcoding the
LLM system prompt structure before Lane E.

---

## Risk Summary

**BCBS239 Lineage:**

- If audit log fields (`source`, `dataset_name`) are not added when the semantic path is
  activated, lineage records will be incomplete and BCBS239 attributability is broken.
- If the audit log format changes after the BCBS239 subscriber is built, the subscriber
  breaks. The audit log schema should be treated as a published interface once external
  consumers exist.

**Market Risk Brain:**

- If MRB context is baked into the intent resolution prompt without a `ContextProvider`
  interface, replacing or disabling MRB requires editing the prompt template — not just
  removing a provider implementation.
- If Option 2 (contracts carry MRB context) is chosen, every model parameter change
  triggers a contract PR. For volatile risk parameters, this creates PR volume that
  governance teams cannot sustain.

**If both integrations are attempted in Lane C:**

- Scope contamination: Lane C becomes a delivery lane for regulatory and AI infrastructure
  that depends on ADR-007 decisions not yet finalised.
- The C1 platform boundary model explicitly lists both as future — Lane C preserves seams
  only.

---

## Decision Status

**Not yet decided.** Both sub-questions are open. Current leanings are noted above.

The BCBS239 lineage question (A) should be resolved before the semantic path is activated
post-Lane C, as the `source` field must be present in audit events from day one of
semantic query traffic.

The Market Risk Brain question (B) resolves in Lane E planning. No Lane C action required
beyond noting that the LLM system prompt in `POST /ai/v1/intent` must have a
`ContextProvider` injection point — not a hardcoded text block.
