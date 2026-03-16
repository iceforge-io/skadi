# Skadi Architecture

This section describes the architecture of Skadi.

Skadi is a **query acceleration and caching layer for Databricks SQL workloads**.

It improves BI performance by caching query results and serving repeated
queries without re-executing them against the warehouse.

## Key capabilities

- PostgreSQL-compatible SQL endpoint
- Arrow-based result streaming
- deterministic query caching
- dataset-aware cache invalidation
- scalable multi-node deployment

## Architecture Sections

| Document | Description |
|---|---|
| overview.md | High-level architecture and goals |
| components.md | System modules and responsibilities |
| query-flow.md | Query execution lifecycle |
| caching.md | Cache design and storage |
| dataset-versioning.md | Cache invalidation model |
| distributed-architecture.md | Multi-node deployment |
| sql-gateway.md | PostgreSQL compatibility layer |

Detailed AI design documentation exists under the `ai/` directory.
