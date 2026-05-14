#!/usr/bin/env bash
set -euo pipefail

REPO="$(gh repo view --json nameWithOwner --template '{{.nameWithOwner}}')"
EPIC_NUMBER="38"

echo "Using repo: $REPO"
echo "Parent Lane C epic: #$EPIC_NUMBER"

echo "Ensuring labels exist..."

gh label create "lane/c" \
  --repo "$REPO" \
  --color "1f6feb" \
  --description "Lane C: platform contracts, skeletons, and boundaries" 2>/dev/null || true

gh label create "area:platform-contracts" \
  --repo "$REPO" \
  --color "5319e7" \
  --description "Platform contracts, service boundaries, and architectural interfaces" 2>/dev/null || true

gh label create "area:semantic-skeleton" \
  --repo "$REPO" \
  --color "0e8a16" \
  --description "Semantic layer skeletons and contracts only" 2>/dev/null || true

gh label create "type:architecture" \
  --repo "$REPO" \
  --color "fbca04" \
  --description "Architecture/design issue" 2>/dev/null || true

echo "Creating Lane C story issues..."

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C1 — Define Skadi platform boundary model" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:platform-contracts" \
  --label "type:architecture" \
  --body-file - <<EOF
# Lane C: C1 — Define Skadi platform boundary model

Parent epic: #$EPIC_NUMBER

## Objective

Define the architectural boundaries between the major Skadi platform components before implementation expands beyond the SQL Gateway.

## Scope

Document the responsibilities and non-responsibilities of:

- SQL Gateway
- Query Execution Layer
- Cache Layer
- Semantic Contract Layer
- Semantic Planner / Rule Engine, future only
- UI Brick Runtime, future only
- AI Chat Buddy, future only
- Databricks / Mesh / Gold data
- Market Risk Brain / unstructured knowledge, future integration
- BCBS239 lineage database, future integration

## Deliverables

- Markdown architecture document under \`ai/architecture/\` or equivalent
- Boundary diagram using Mermaid
- Clear "owns / does not own" table per component
- Explicit future-extension seams

## Acceptance criteria

- The document makes clear that Lane C is about contracts and skeletons, not full semantic implementation
- SQL Gateway remains focused on protocol and query-serving concerns
- Semantic layer is described as a contract boundary, not yet a full planner
- Cache layer is positioned between Databricks and higher-level consumers
- Future UI/AI systems are shown as consumers of contracts, not implemented here

## Non-goals

- No implementation of semantic planning
- No UI runtime
- No AI chatbot
- No entitlement engine
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C2 — Introduce semantic contract skeletons" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:semantic-skeleton" \
  --body-file - <<EOF
# Lane C: C2 — Introduce semantic contract skeletons

Parent epic: #$EPIC_NUMBER

## Objective

Introduce minimal Java/domain skeletons for semantic contracts without implementing a semantic planner or rule engine.

## Scope

Create contract/domain classes for concepts such as:

- SemanticEndpoint
- SemanticEntity
- SemanticMeasure
- SemanticDimension
- SemanticRuleRef
- SemanticQueryContract
- SemanticOutputShape

Exact names may be refined during implementation.

## Deliverables

- New package/module boundary for semantic contracts
- Immutable DTOs/records where appropriate
- JSON serialization/deserialization support
- Basic validation where low-risk and obvious

## Acceptance criteria

- Contracts compile cleanly
- Contracts do not depend on SQL Gateway internals
- Contracts do not implement planning or query rewriting
- Existing Maven build passes
- Tests cover basic serialization/deserialization

## Non-goals

- No semantic query planner
- No rule execution engine
- No SQL generation from semantic metadata
- No UI widget implementation
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C3 — Define query contract and output-shape metadata" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:semantic-skeleton" \
  --body-file - <<EOF
# Lane C: C3 — Define query contract and output-shape metadata

Parent epic: #$EPIC_NUMBER

## Objective

Define a stable metadata model for describing the shape of query results so future UI bricks, semantic endpoints, and AI agents can reason about returned data.

## Scope

Create metadata contracts for:

- Column name
- Display name
- Data type
- Nullable flag
- Semantic role, such as measure, dimension, timestamp, identifier
- Optional unit/currency hints
- Optional formatting hints
- Optional lineage/semantic reference IDs

## Deliverables

- Output shape DTO/model
- Query contract DTO/model
- Example JSON contract file
- Unit tests for serialization and validation

## Acceptance criteria

- Output shapes can be represented independently of JDBC ResultSet
- Contracts can describe tabular outputs suitable for Tableau, UI bricks, and API consumers
- Contracts are generic enough for future semantic endpoints
- No dependency on dashboard runtime
- No dependency on AI implementation

## Non-goals

- No UI rendering
- No Tableau-specific rendering
- No semantic query planning
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C4 — Define cache contract boundary" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:cache-s3" \
  --label "area:platform-contracts" \
  --body-file - <<EOF
# Lane C: C4 — Define cache contract boundary

Parent epic: #$EPIC_NUMBER

## Objective

Define the logical boundary between query identity, execution, cached result metadata, and physical cache storage.

## Scope

Introduce or refine contracts for:

- Logical query identity
- Normalized query fingerprint
- Cache lookup request
- Cache lookup result
- Cache write request
- Cached artifact metadata
- Cache storage location abstraction
- Result lifecycle state

## Deliverables

- Java interfaces/classes for cache boundary contracts
- Documentation showing how cache sits between Databricks and higher-level consumers
- Tests for deterministic identity/fingerprint behavior where applicable

## Acceptance criteria

- Cache identity is separated from physical storage details
- Higher layers do not need to know whether storage is local, S3, or future-compatible object storage
- Contracts are usable by SQL Gateway today and Semantic Layer later
- Existing cache behavior is not broken

## Non-goals

- No full cache rewrite
- No distributed cache protocol
- No production object-store migration
- No semantic cache planner
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C5 — Define service interfaces for semantic-aware execution" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:platform-contracts" \
  --body-file - <<EOF
# Lane C: C5 — Define service interfaces for semantic-aware execution

Parent epic: #$EPIC_NUMBER

## Objective

Create clean service seams so future semantic-aware execution can be added without coupling SQL Gateway, cache, Databricks execution, and semantic metadata too tightly.

## Scope

Define interfaces such as:

- QueryExecutionService
- QueryMetadataService
- CacheLookupService
- SemanticContractResolver
- LineageContextProvider
- ExecutionContext

Exact names may change during implementation.

## Deliverables

- Interface skeletons
- Minimal default/no-op implementations where useful
- Documentation of call flow
- Unit tests around service composition where applicable

## Acceptance criteria

- Interfaces allow SQL-first execution today
- Interfaces leave room for semantic-contract-first execution later
- No heavy implementation hidden behind the interfaces
- No dependency cycle introduced between modules
- Maven build remains clean

## Non-goals

- No semantic planner
- No lineage database integration
- No Market Risk Brain integration
- No AI agent
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C6 — Add ADRs/DQRs for platform boundary decisions" \
  --label "type:story" \
  --label "lane/c" \
  --label "type:architecture" \
  --label "area:platform-contracts" \
  --body-file - <<EOF
# Lane C: C6 — Add ADRs/DQRs for platform boundary decisions

Parent epic: #$EPIC_NUMBER

## Objective

Capture the major Lane C architectural decisions as ADRs and open design questions as DQRs.

## Scope

Add records covering:

- Why Lane C focuses on contracts/skeletons/boundaries
- Why semantic contracts are introduced before semantic planning
- Why UI brick runtime remains out of scope
- Why AI chatbot remains out of scope
- How Skadi cache is positioned between Databricks and higher-level services
- How future lineage and Market Risk Brain integrations should attach

## Deliverables

- ADR(s) under \`ai/adr/\`
- DQR(s) under \`ai/dqr/\`
- Mermaid diagram where helpful

## Acceptance criteria

- ADRs are numbered consistently
- DQRs follow the established structure: scope, impact, tie-in, risk summary
- Documents are concrete enough for future Claude Code agents to follow
- Documents do not overcommit implementation details

## Non-goals

- No code implementation required unless needed for references
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C7 — Add contract-focused tests" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:platform-contracts" \
  --body-file - <<EOF
# Lane C: C7 — Add contract-focused tests

Parent epic: #$EPIC_NUMBER

## Objective

Add tests that prove the Lane C contracts are stable, serializable, and safe for future implementation work.

## Scope

Test areas:

- Semantic contract DTO serialization/deserialization
- Output shape metadata serialization/deserialization
- Cache identity/fingerprint behavior
- No-op/default service implementations
- Module dependency boundaries where feasible

## Deliverables

- Unit tests
- Sample JSON fixtures
- Build verification

## Acceptance criteria

- Tests are deterministic
- Tests do not require Databricks
- Tests do not require S3
- Tests do not require Tableau
- Existing test suite remains green
- Maven verify passes for affected modules

## Non-goals

- No integration tests against real Databricks
- No performance tests
- No UI tests
EOF

gh issue create \
  --repo "$REPO" \
  --title "Lane C: C8 — Update dev-status and Lane C runbook" \
  --label "type:story" \
  --label "lane/c" \
  --label "area:platform-contracts" \
  --body-file - <<EOF
# Lane C: C8 — Update dev-status and Lane C runbook

Parent epic: #$EPIC_NUMBER

## Objective

Update project documentation so Lane C is easy for AI coding agents and future maintainers to understand.

## Scope

Update:

- \`ai/dev-status.md\`
- Lane C checklist/runbook
- Issue-to-PR tracking table
- Implementation sequencing guidance
- Explicit non-goals

## Deliverables

- Lane C status section
- Story checklist
- Recommended implementation order
- Notes for Claude Code agents

## Acceptance criteria

- Dev-status reflects Lane A/B completion and Lane C start
- Lane C stories are listed with issue numbers after creation
- Non-goals are visible to prevent scope creep
- Recommended Claude Code execution order is documented

## Non-goals

- No product pitch material
- No UI mockups
- No semantic implementation
EOF

echo
echo "Lane C issues created."
echo
echo "Exporting Lane C issues to ai/lane-c/lane-c-issues.json..."

mkdir -p ai/lane-c

gh issue list \
  --repo "$REPO" \
  --state all \
  --limit 500 \
  --json number,title,state,labels,assignees,milestone,createdAt,updatedAt,url,body \
  > ai/lane-c/lane-c-issues.json

python3 -m json.tool ai/lane-c/lane-c-issues.json > ai/lane-c/lane-c-issues.pretty.json || true

echo
echo "Done."
echo "Review:"
echo "  ai/lane-c/lane-c-issues.json"
echo "  ai/lane-c/lane-c-issues.pretty.json"
echo
echo "Recommended implementation order:"
echo "  C1 -> C6 -> C2 -> C3 -> C4 -> C5 -> C7 -> C8"
