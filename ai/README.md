# AI Context Directory

This directory provides structured context for AI-assisted development of Skadi.

---

## 📚 Directory Structure

### Current System (Authoritative Truth)

These files describe what the system **actually does today**.

* `current-system-state.md` → Ground truth of current architecture and behavior
* `current-implementation.md` → Implementation details and code structure
* `dev-status.md` → What is currently in progress

👉 **AI agents MUST treat this section as the source of truth**

---

### 📋 Active Plans (What We Are Building Now)

* `plan/` → Active delivery plans (e.g., Tableau SQL Endpoint)
* `dev-milestones/` → Execution checkpoints
* `dev-prompts/` → Prompts for coding agents

👉 These represent **committed near-term work**

---

### 🏛️ Architectural Decisions

* `adr/` → Architectural Decision Records

👉 Includes:

* Accepted decisions
* Proposed future decisions (clearly marked)

---

### 🔮 Future Architecture Options (NOT CURRENT WORK)

* `future-architecture-options/`

These are **candidate future directions**, not active implementation.

Each option:

* Explores a possible evolution path
* Is not committed to delivery
* Requires explicit decision (ADR) before execution

👉 AI agents MUST NOT implement these unless explicitly instructed

---

## 🧠 Interpretation Rules for AI Agents

Priority order:

1. **Current System** → what exists
2. **Plans** → what to build now
3. **ADRs** → architectural intent
4. **Future Options** → ideas only

---

## ⚠️ Critical Rule

> If there is a conflict:
>
> * Follow **current-system-state.md**
> * NOT future-architecture-options

---

## 🎯 Example

| Scenario                  | Correct Source               |
| ------------------------- | ---------------------------- |
| How SQL is executed today | current-system-state.md      |
| What to build next        | plan/tableau-sql-endpoint.md |
| Long-term warehouse idea  | future-architecture-options/ |

---

This structure ensures:

* Safe AI-assisted development
* Clear separation of concerns
* No accidental “future architecture leakage” into current implementation
