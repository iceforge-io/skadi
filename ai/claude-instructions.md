# Claude Instructions for Skadi

This repository is developed with the assistance of AI coding agents such as Claude Code.

Before making significant changes, agents should read the following documents:

* ai/system-map.md
* ai/query-flow.md
* ai/adr/
* ai/project-status/

These documents define the intended architecture and project roadmap.

---

# Architectural Principles

Skadi is designed as a **query acceleration and caching layer in front of Databricks SQL Warehouse**.

Primary goals:

1. Reduce repeated execution of expensive queries
2. Stream results efficiently using Arrow
3. Provide SQL endpoints compatible with BI tools (initially PostgreSQL protocol)

The architecture separates responsibilities into three modules:

skadi-core
Shared libraries and utilities.

skadi-server
Query execution and caching.

skadi-sql-gateway
Network protocols and SQL client compatibility.

Agents must preserve this separation.

---

# Module Responsibilities

## skadi-core

Contains:

* cache primitives
* query normalization
* query hashing
* dataset versioning
* Arrow utilities

Must NOT contain:

* Spring Boot services
* network protocol servers
* database connections

---

## skadi-server

Responsible for:

* query execution
* interaction with Databricks SQL Warehouse
* Arrow result streaming
* cache management
* cache invalidation

This module implements the execution engine.

---

## skadi-sql-gateway

Responsible for:

* SQL client connectivity
* PostgreSQL wire protocol server
* authentication
* translating client queries to execution requests

The gateway should be thin.

Execution logic belongs in skadi-server.

---

# Query Execution Model

Typical flow:

Client
↓
SQL Gateway
↓
Query normalization
↓
Cache lookup
↓

If cache hit:
return cached Arrow batches

If cache miss:
execute query on Databricks
stream Arrow results
store in cache

---

# Cache Design

Cache keys are deterministic.

Key inputs:

* normalized SQL
* query parameters
* dataset version

Cache format:

Arrow RecordBatch streams

Cache layers may include:

* memory
* disk
* S3 (future)

---

# Dataset Refresh

Some datasets refresh frequently.

Example:

Risk datasets for recent COB dates refresh every 2 hours.

Cache invalidation must account for dataset version changes.

---

# Coding Guidelines

When implementing features:

Prefer small focused classes.

Avoid duplicating logic across modules.

Shared utilities should be placed in skadi-core.

Network protocol handling belongs in skadi-sql-gateway.

Execution logic belongs in skadi-server.

---

# Implementation Guidance

Before implementing new functionality:

1. Check ai/project-status for the current roadmap
2. Verify which GitHub issue the change relates to
3. Avoid implementing features outside the current lane

For the Tableau SQL Endpoint project:

Lane A
POC connectivity and caching demonstration

Lane B
Protocol completeness and production hardening

Agents should prioritize completing the current lane.

---

# Testing Expectations

New functionality should include tests where practical.

Prefer:

* unit tests for logic
* integration tests for gateway behavior

Tests should remain lightweight.

---

# AI Behavior Expectations

Agents should:

Explain architectural decisions when making structural changes.

Avoid introducing unnecessary frameworks.

Avoid large refactors unless explicitly requested.

Prefer incremental improvements aligned with the roadmap.
