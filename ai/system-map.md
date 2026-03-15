# Skadi System Map

This document provides a high-level map of the Skadi repository for AI coding agents.

Agents should read this file before making structural changes.

---

# Repository Modules

## skadi-core

Purpose:
Shared libraries used by all Skadi components.

Responsibilities:
- cache primitives
- dataset versioning
- query hashing
- Arrow utilities
- configuration models

Does NOT contain:
- Spring Boot services
- network servers

Used by:
- skadi-server
- skadi-sql-gateway


---

## skadi-server

Purpose:
Main Skadi service responsible for:

- query execution
- result caching
- S3 storage
- Arrow streaming
- cache invalidation

Typical flow:

Client
↓
Skadi Server
↓
Databricks SQL Warehouse
↓
Arrow results streamed
↓
cached locally + optionally in S3

Key concepts:

Cache layers
1. memory
2. disk
3. S3 (future)

Primary packages:

org.iceforge.skadi.server
org.iceforge.skadi.cache
org.iceforge.skadi.execution


---

## skadi-sql-gateway

Purpose:
Expose Skadi as a SQL endpoint compatible with BI tools.

Primary initial protocol:
PostgreSQL wire protocol.

Future protocols:
- MySQL (optional)
- JDBC wrapper

Responsibilities:

- accept PostgreSQL client connections
- translate SQL requests
- call Skadi execution layer
- stream results back to client

Typical flow:

Tableau
↓
PostgreSQL wire protocol
↓
Skadi SQL Gateway
↓
Skadi execution engine
↓
Databricks SQL Warehouse
↓
Arrow results
↓
PgWire row stream


---

# Caching Model

Queries are cached using a deterministic key.

Cache key inputs:

- normalized SQL
- query parameters
- dataset version

Cache storage:

- memory
- disk
- optional S3

Cached format:

Arrow RecordBatch streams


---

# Dataset Versioning

Skadi must invalidate cache when upstream datasets refresh.

For MXL risk datasets:

dataset key =

table_name + cob_date + dataset_version

Recent dates may refresh more frequently.


---

# External Systems

Skadi interacts with:

Databricks SQL Warehouse
S3 object storage
BI tools (Tableau initially)


---

# Design Goal

Skadi acts as a **query accelerator and cache layer** in front of Databricks.

Primary goal:

Reduce repeated query execution by caching Arrow result sets.


---

# AI Implementation Guidance

When modifying code:

1. Avoid duplicating logic between modules
2. Shared utilities belong in `skadi-core`
3. Network protocols belong in `skadi-sql-gateway`
4. Execution and caching belong in `skadi-server`