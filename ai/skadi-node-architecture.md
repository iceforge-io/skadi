# Skadi Node Architecture

This document describes how Skadi operates in a multi-node deployment.

The goal is to allow multiple Skadi nodes to share cached query results
while minimizing repeated execution against Databricks SQL Warehouse.

---

# Single Node Architecture (POC)

Initial deployments may run a single Skadi node.

Architecture:

Client (Tableau)
↓
Skadi SQL Gateway
↓
Skadi Execution Engine
↓
Databricks SQL Warehouse

Cache layers:

Memory cache
Disk cache

All caching occurs locally.

---

# Multi-Node Architecture

In production deployments, multiple Skadi nodes may run behind a load balancer.

Example architecture:

Clients
↓
Load Balancer
↓
Skadi Node A
Skadi Node B
Skadi Node C
↓
Databricks SQL Warehouse

If each node maintains an independent cache, identical queries executed on
different nodes will still hit Databricks.

To avoid this, Skadi should support a shared cache layer.

---

# Shared Cache Layer

The recommended shared cache implementation is object storage.

Example:

AWS S3

Architecture:

Clients
↓
Load Balancer
↓
Skadi Nodes
↓
Shared S3 Cache
↓
Databricks SQL Warehouse

Query execution flow:

1. Query arrives at node
2. Node checks local cache
3. If miss, check shared S3 cache
4. If S3 hit, stream result from S3
5. If miss, execute query on Databricks
6. Store result in S3 cache
7. Store result in local cache

This allows cache reuse across nodes.

---

# Cache Hierarchy

Recommended hierarchy:

Level 1 — Memory cache

Fastest access
Small capacity

Level 2 — Disk cache

Larger capacity
Persistent across restarts

Level 3 — S3 cache

Shared across nodes
Very large capacity

---

# Cache Lookup Flow

Query arrives
↓
Compute cache key
↓
Check memory cache

If hit:
return result

If miss:
check disk cache

If hit:
return result

If miss:
check S3 cache

If hit:
download Arrow batches
populate local cache
return result

If miss:
execute query on Databricks

---

# Cache Write Flow

When a query result is produced:

1. Stream results to client
2. Store Arrow batches in local cache
3. Upload Arrow batches to S3 cache

Cache entries stored in S3 should include metadata.

Example metadata:

cache_key
row_count
creation_time
dataset_version

---

# Arrow Storage Format

Arrow results should be stored as Arrow IPC files.

Advantages:

Columnar storage
Efficient streaming
Minimal serialization overhead

Example object structure:

s3://skadi-cache/
dataset_hash/
cache_key.arrow

---

# Cache Size Management

Local caches must have eviction policies.

Recommended policies:

Memory cache → LRU
Disk cache → size-based eviction

S3 cache may use lifecycle rules for cleanup.

Example:

Delete objects older than 30 days.

---

# Cache Consistency

Cache correctness depends on dataset versioning.

Cache key includes:

normalized SQL
parameters
dataset version

If dataset version changes, cached entries automatically become invalid.

This avoids complex cache invalidation logic.

---

# Concurrent Query Handling

Multiple nodes may receive identical queries simultaneously.

Example:

User A runs dashboard
User B runs same dashboard

Both queries may miss the cache.

To avoid duplicate execution:

Nodes may implement a simple lock mechanism.

Example:

Query arrives
↓
Cache miss
↓
Acquire execution lock for cache_key

If lock acquired:
execute query

If lock exists:
wait for result to appear in cache

---

# Failure Handling

If a node crashes during execution:

Partial cache entries must not be used.

Recommended strategy:

Write results to temporary object
Finalize cache entry only after completion

---

# Deployment Model

Typical production deployment:

Load balancer
3–5 Skadi nodes
Shared S3 cache
Databricks SQL Warehouse

This architecture allows the system to scale with BI user demand.

---

# Observability

Each node should expose metrics:

cache_hit_rate
query_execution_time
databricks_queries_executed
cache_entries_created

Metrics should be accessible via a monitoring endpoint.

---

# AI Implementation Guidance

When implementing distributed cache features:

Keep the cache key consistent across nodes.

Ensure Arrow result storage format remains stable.

Prefer simple deterministic behavior over complex synchronization.

Initial implementations should focus on correctness before optimization.
