# ADR-007: AI Chat Integration Points

**Status:** Proposed
**Date:** 2026-05-13
**Owners:** engineering, architecture
**Related ADRs:** ADR-004, ADR-005, ADR-006
**Supersedes:** —

---

## 1. Context

### Problem

Analysts and risk managers want to ask questions about their data in natural language:
"What was the total PnL by book for yesterday?" or "Show me the desks with the highest
delta risk this week."

Integrating an LLM directly against raw SQL creates structural problems:

- **Governance bypass**: an LLM generating raw SQL can query any table the user has schema-level
  access to, bypassing semantic-level governance and metric definitions
- **Fragility**: LLM-generated SQL breaks silently when physical schemas change; it must know
  table names, column names, join conditions, and Databricks SQL dialect
- **Inconsistency**: the LLM will compute `SUM(pnl)` differently from the governed metric
  definition — answers diverge from governed dashboard numbers
- **Audit opacity**: a raw SQL query from an LLM is hard to attribute, review, or explain to
  a compliance function

### Background

- The semantic layer (ADR-004) exposes a governed query API where queries are specified as
  metric + dimension + filter selections — this is the right LLM output target
- Semantic contracts (ADR-005) include metric and dimension names, labels, and descriptions —
  these form the vocabulary the LLM needs to construct a semantic query
- The `chat` brick type (ADR-006) is the dashboard surface for AI chat — this ADR defines
  the backend integration
- Skadi operates in regulated financial environments where all query activity must be auditable

---

## 2. Decision

AI chat integrates with Skadi **exclusively through the semantic query layer** (ADR-004). The
LLM never generates raw SQL and never accesses Databricks directly.

### 2.1 Integration architecture

Three discrete integration points:

```
User: "What was total PnL by book yesterday?"
           ↓
[1] Intent Resolution  (skadi-semantic: POST /ai/v1/intent)
           ↓  structured SemanticQuery
[2] Query Execution    (skadi-semantic: POST /semantic/v1/query)
           ↓  Arrow result
[3] Response Generation (skadi-semantic: POST /ai/v1/explain)
           ↓  natural language answer
User: "Total PnL by book on 2026-05-12: Book A £2.1M, Book B £0.8M ..."
```

These three points are separate API calls, each observable and testable independently.

### 2.2 Intent resolution (`POST /ai/v1/intent`)

The intent resolver translates a natural language prompt into a structured `SemanticQuery`.

**Input:**
```json
{
  "principal": "alice",
  "prompt":    "What was total PnL by book yesterday?"
}
```

**Process:**
1. Build a context window for the LLM containing:
   - The datasets, metrics, and dimensions the principal has access to (filtered from `ContractRegistry`)
   - Their names, labels, descriptions, and types
   - The supported filter operators and relative date macros
   - A few-shot example of prompt → `SemanticQuery` mapping
2. Call the LLM (Claude API via Anthropic SDK with prompt caching on the context window)
3. Parse the structured output as a `SemanticQuery`
4. Validate the output against the contract registry: reject any metric or dimension not in the principal's accessible contracts

**Output:**
```json
{
  "dataset":    "mxl_risk",
  "metrics":    ["pnl"],
  "dimensions": ["book"],
  "filters":    [{ "dimension": "cob_date", "operator": "eq", "value": "$yesterday" }]
}
```

The LLM output is a `SemanticQuery` record, not SQL. If the LLM produces anything other than
a valid `SemanticQuery`, the intent resolution call returns a structured error.

### 2.3 Query execution

The resolved `SemanticQuery` is passed to `POST /semantic/v1/query` — the same endpoint used
by dashboard bricks. No separate execution path. The same governance checks, same cache,
same audit log.

The `source` field on the audit event is set to `ai_chat` to distinguish AI-driven queries
from brick-driven or direct API queries.

### 2.4 Response generation (`POST /ai/v1/explain`)

**Input:**
```json
{
  "principal": "alice",
  "prompt":    "What was total PnL by book yesterday?",
  "query":     { ...SemanticQuery... },
  "result":    { ...Arrow result as structured rows... }
}
```

**Process:**
1. Pass the result table (row-limited to a safe maximum for the context window) plus the original prompt to the LLM
2. LLM generates a natural language answer; does not generate follow-up SQL
3. Response is returned to the caller with the query and result attached for transparency

**Output:**
```json
{
  "answer":       "Yesterday (2026-05-12), total PnL by book was: Book A £2.1M, Book B £0.8M, ...",
  "query":        { ...SemanticQuery... },
  "result_rows":  12,
  "source":       "ai_chat"
}
```

### 2.5 LLM provider abstraction

The intent resolver and response generator use a `LlmProvider` interface with a single
Claude implementation in Phase 1. The interface accepts a prompt + context and returns a
string; the provider detail (model, API key, streaming) is internal to the implementation.

```java
interface LlmProvider {
    String complete(LlmRequest request);
}

record LlmRequest(String systemPrompt, String userPrompt, boolean cache) {}
```

Provider config (`application.yml`):
```yaml
skadi:
  semantic:
    ai:
      provider:  claude              # claude | stub (test)
      model:     claude-sonnet-4-6
      api-key:   ${ANTHROPIC_API_KEY}
      max-tokens: 1024
      prompt-cache: true
```

### 2.6 The `chat` brick type

The `chat` brick (ADR-006) is a dashboard brick that renders a chat input panel. It:
- Displays a text input and conversation history
- Calls `POST /ai/v1/intent` with the user's prompt and identity
- Calls `POST /semantic/v1/query` with the resolved intent
- Calls `POST /ai/v1/explain` with the result
- Renders the natural language answer and, optionally, a mini-table of the result rows

The chat brick can be placed on any dashboard alongside other bricks. There is no separate
chat application.

### 2.7 What AI chat will NOT do

- Generate raw SQL — not possible through the API
- Access datasets/metrics the user does not have access to — enforced structurally
- Execute arbitrary code — the LLM produces `SemanticQuery` records, not instructions
- Answer questions about data outside the semantic contract catalog — it has no visibility beyond what the contracts expose
- Modify data — the semantic layer is read-only

---

## 3. Rationale

**Why semantic-layer-only and not a SQL generation approach?**
SQL generation gives the LLM physical schema access, which bypasses governance and metric
consistency. It also makes the AI fragile to schema changes. Constraining the LLM output to
`SemanticQuery` records means governance is enforced by the API, not by prompt engineering —
a much stronger guarantee.

**Why three separate endpoints rather than a single "ask" endpoint?**
Separation makes each point independently testable, observable, and cacheable. The intent
resolution step can be benchmarked and improved independently of the response generation step.
A caller can also use intent resolution without response generation (e.g., to preview what
query would be issued before executing it).

**Why prompt cache the contract context?**
The contract context window (all accessible metrics and dimensions) is the same for all
queries from a given principal. Prompt caching (Anthropic API) eliminates repeated tokenisation
of this context, reducing both latency and cost for high-frequency chat.

**Why a `LlmProvider` abstraction?**
Vendor lock-in on the AI layer is an explicit risk to avoid (per architecture principles).
The abstraction costs nothing in Phase 1 and preserves the option to switch providers.

---

## 4. Consequences

### Positive
- AI answers are consistent with governed dashboard numbers — they use the same metric definitions
- All AI-driven queries are auditable (`source=ai_chat` in audit log)
- Governance is structural, not prompt-level — no jailbreak possible through the chat interface
- Prompt caching on the contract context makes chat responses fast and cost-efficient
- The chat brick integrates into the existing dashboard governance model — no separate product to govern

### Negative / Risks
- Intent resolution quality depends on LLM output quality — malformed `SemanticQuery` outputs must be handled gracefully with clear user error messages
- The context window for large contract registries (many datasets, metrics, dimensions) may hit LLM limits — contract context must be scoped to the principal's accessible contracts only
- Response generation passes result rows to the LLM — a large result set must be row-limited before building the context window (risk of truncated answers)
- LLM API latency is non-deterministic; chat responses will be slower than cached query responses

### Operational Impact
- `ANTHROPIC_API_KEY` (or equivalent) must be injected as a secret into `skadi-semantic`
- New metrics: `skadi_intent_resolution_seconds`, `skadi_ai_requests_total` (outcome: resolved | failed | access_denied)
- LLM API errors must not propagate as unhandled exceptions — the chat brick must display a clear error message
- Token consumption should be logged (not in audit log — operational metric only)

---

## 5. Alternatives Considered

| Option | Why Rejected |
|--------|--------------|
| LLM generates raw SQL directly | Bypasses semantic governance; fragile to schema changes; metric inconsistency |
| LLM generates dbt/LookML semantic queries | Couples Skadi to an external tool's query format; governance lives outside the codebase |
| Vector-search-over-data-rows approach | Doesn't work for aggregated analytical queries; not suited to risk/PnL calculation patterns |
| Rule-based NL→SQL (no LLM) | Cannot handle the breadth of natural language patterns; requires extensive maintenance |
| Separate AI application outside Skadi | Governance would be duplicated or bypassed; AI queries would not benefit from Skadi's cache |

---

## 6. Fitness Functions / Enforcement

- Integration test: `POST /ai/v1/intent` with a prompt that requests a metric the principal cannot access → HTTP 403 with `semantic_access_denied` error
- Integration test: intent resolver returns a `SemanticQuery` that references a non-existent metric → validation error, not a pass-through to the compiler
- `LlmProvider.complete()` must never be called with raw SQL or physical table names in the user prompt — lint rule or review-only in Phase 1
- Token consumption logged at `DEBUG` level; never logged at `INFO` or above (to avoid operational log noise)
- A `stub` LlmProvider implementation returns fixture `SemanticQuery` objects for deterministic testing — all integration tests use the stub

> Prompt-level enforcement ("do not generate SQL") is explicitly NOT relied upon as a security control. Structural API enforcement is the only accepted control.

---

## 7. Migration / Rollout Plan

1. **E1 (Lane E):** `CatalogContextBuilder` — formats the accessible contract subset for a principal into a system prompt; unit-tested with fixture contracts
2. **E1 (Lane E):** `ClaudeLlmProvider` implementation with prompt caching; `StubLlmProvider` for tests
3. **E2 (Lane E):** `POST /ai/v1/intent` endpoint — intent resolution and `SemanticQuery` validation
4. **E2 (Lane E):** `POST /ai/v1/explain` endpoint — response generation
5. **E3 (Lane E):** `chat` brick type in `skadi-ui-bricks`; end-to-end test with stub provider
6. **E4 (Lane E):** `source=ai_chat` audit events; `skadi_intent_resolution_seconds` metric

---

## 8. Open Questions

- Q1: Should the chat brick show the intermediate `SemanticQuery` to the user ("I interpreted your question as: PnL by book, filtered to yesterday") before executing?
- Q2: What is the row limit for result rows passed to the response generation LLM? 100 rows? 1000?
- Q3: Should intent resolution support multi-turn conversations (follow-up questions referencing prior results), and if so, how is conversation state managed?
- Q4: Should the `chat` brick support streaming responses (tokens as they arrive from the LLM) or batch responses only?
- Q5: How is the Anthropic API key rotated in production without a service restart?
