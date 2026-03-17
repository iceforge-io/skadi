# Skadi AI Development Manager - GitHub Project Field Setup Checklist

Use this checklist to augment the existing **Skadi - Platform Roadmap** GitHub Project so it can serve as the runtime control plane for the Development Manager AI.

## Goal

Do **not** create a second Project at first. Extend the existing Project with the fields below, then pilot the model on active SQL Gateway work.

---

## 1. Confirm the Project

Project name:
- `Skadi - Platform Roadmap`

Repository in scope:
- `iceforge-io/skadi`

Recommended operating model:
- one board for roadmap + execution control
- existing historical issues may remain partially populated
- only issues intended for AI execution must meet the full metadata standard

---

## 2. Create Required Custom Fields

### Status
Type: Single select

Options:
- Backlog
- Ready
- Scheduled
- Assigned to Worker
- In Progress
- Blocked
- PR Open
- In Review
- Ready to Merge
- Merged
- Done
- Cancelled

### Type
Type: Single select

Options:
- Epic
- Story
- Task
- Bug
- Docs
- Chore

### Lane
Type: Single select

Options:
- SQL Gateway
- Core
- Server
- Cache
- Observability
- Build/CI
- Docs

### Module
Type: Multi-select

Options:
- skadi-server
- skadi-core
- skadi-sql-gateway
- parent-build
- test-infra
- docs

### Priority
Type: Single select

Options:
- P0
- P1
- P2
- P3

### AI Eligibility
Type: Single select

Options:
- Eligible
- Assisted Only
- Human Only

### Risk Level
Type: Single select

Options:
- Low
- Medium
- High
- Critical

### Concurrency Group
Type: Single select

Options:
- sql-protocol
- metadata-contracts
- jdbc-executor
- rowset-cache
- query-cancellation
- build-structure
- dependency-management
- test-harness
- docs-only
- observability

### Parent Issue
Type: Text

Format:
- `#123`

### Blocked By
Type: Text

Format:
- `#101, #102`

### PR Link
Type: Text

### Worker ID
Type: Text

### Branch Name
Type: Text

### Retry Count
Type: Number

### Last Manager Update
Type: Date

### Execution Mode
Type: Single select

Options:
- AI Auto
- AI Suggested
- Human

### Size
Type: Single select

Options:
- XS
- S
- M
- L

### Test Scope
Type: Single select

Options:
- Unit
- Integration
- Manual
- Mixed

---

## 3. Recommended Views

### A. Roadmap View
Purpose:
- human planning
- milestone and epic visibility

Suggested grouping:
- by Lane or Status

Suggested visible fields:
- Title
- Status
- Type
- Lane
- Priority
- Module

### B. AI Ready Queue
Purpose:
- issues immediately available for AI scheduling

Filter:
- Status = Ready
- AI Eligibility = Eligible

Visible fields:
- Title
- Priority
- Lane
- Module
- Risk Level
- Concurrency Group
- Blocked By

Sort:
- Priority descending
- then Last updated ascending

### C. Active AI Work
Purpose:
- monitor running workers and PRs

Filter:
- Status in Scheduled, Assigned to Worker, In Progress, PR Open, In Review

Visible fields:
- Title
- Status
- Worker ID
- Branch Name
- PR Link
- Lane
- Module
- Concurrency Group
- Risk Level
- Retry Count

### D. Blocked
Purpose:
- quick unblock queue

Filter:
- Status = Blocked

Visible fields:
- Title
- Blocked By
- Risk Level
- Worker ID
- Last Manager Update

### E. Merge Queue
Purpose:
- review what is ready to land

Filter:
- Status in Ready to Merge, Merged

Visible fields:
- Title
- PR Link
- Risk Level
- Lane
- Module

---

## 4. Label Alignment

Recommended label families:
- `type:*`
- `priority:*`
- `lane:*`
- `module:*`
- `risk:*`
- `ai:*`
- `cg:*`

Important note:
- Project fields are the runtime source of truth
- labels are secondary and mainly useful for search, triage, and compatibility with existing workflows

---

## 5. Minimum Metadata Rule for AI Scheduling

An issue may be scheduled by the Development Manager AI only if all are present:

- Status = Ready
- Type = Story, Task, or Bug
- Priority set
- Lane set
- Module set
- Risk Level set
- AI Eligibility = Eligible
- Concurrency Group set
- Acceptance Criteria present in issue body
- No unresolved blockers

---

## 6. First Pilot Recommendation

Start with:
- Lane = SQL Gateway
- Type in Story, Task
- AI Eligibility = Eligible
- no auto-merge
- serialized groups:
  - sql-protocol
  - metadata-contracts
  - build-structure
  - dependency-management

Pilot candidates:
- PostgreSQL metadata compatibility
- Rowset cache integration
- Query cancellation support

---

## 7. Manual Validation Checklist

Before enabling the manager agent, confirm:

- [ ] all required Project fields exist
- [ ] labels are created
- [ ] issue templates are installed under `.github/ISSUE_TEMPLATE/`
- [ ] at least 3 active stories are fully populated
- [ ] concurrency groups are assigned
- [ ] one Active AI Work view exists
- [ ] one AI Ready Queue view exists
- [ ] review/merge policy is agreed
- [ ] no second Project has been created unnecessarily

---

## 8. Operational Rule of Thumb

Use the Project as:
- roadmap board for humans
- execution control plane for the AI manager

Do not split into a second AI-only board until noise or scaling pressure makes it necessary.
