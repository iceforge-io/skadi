# Future Architecture Options

This directory contains **potential future evolution paths** for Skadi.

## Purpose

* Explore strategic directions
* Document architectural ideas
* Provide context for future decision-making

These are **NOT part of the current roadmap**.

---

## ⚠️ Status Definitions

Each option must include:

* **Status:** Proposed / Research / Future
* **Horizon:** Near / Medium / Long term
* **Dependencies:** What must exist first
* **Trigger:** When this becomes relevant

---

## 🚫 What This Is NOT

* Not active development work
* Not part of current epics
* Not instructions for coding agents

---

## 📦 Current Options

### Skadi Warehouse Lite

Path: `skadi-warehouse-lite/`

**Concept:**
Skadi evolves into a **cache-aware SQL execution layer** capable of:

* Reading from cache instead of Delta
* Planning hybrid execution
* Delegating when needed

---

## 🔗 Relationship to Current System

Current Skadi:

* SQL endpoint over Databricks
* Caching + acceleration layer

Future Option:

* Partial or full execution engine
* Cache-aware optimizer
* Reduced dependency on external engines

---

## 🧭 When to Consider These Options

Only after:

* Stable SQL endpoint (PgWire/JDBC)
* Reliable cache correctness
* Observability + metrics
* Security and auth complete
* Production readiness achieved

---

## 🧠 AI Agent Rule

> Do NOT implement anything in this directory unless explicitly instructed.

---

## 🏁 Outcome

This directory allows Skadi to:

* Think long-term
* Stay focused short-term
* Avoid architectural confusion
