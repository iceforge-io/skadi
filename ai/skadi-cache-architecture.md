# Skadi Cache Architecture

This document describes the cache architecture used by Skadi.

Skadi acts as a query acceleration layer in front of Databricks SQL Warehouse.
Its primary goal is to avoid repeated execution of expensive analytical queries
by caching query results.

---

# Cache Objectives

The cache should:

Reduce repeated execution of identical queries
Serve repeated dashboard queries quickly
Reduce Databricks SQL warehouse load
Store results in a format that supports efficient streaming

The cache stores results as Arrow RecordBatch streams.

---

# Query Execution Flow

Query flow through Skadi:

Client (Tableau)
↓
SQL Gateway
↓
Query normalization
↓
Cache lookup

If cache hit:
Return cached Arrow batches

If cache miss:
Execute query on Databricks
Stream Arrow results
Store result in cache
Return results to client

---

# Cache Key Design

Cache keys must be deterministic.

Inputs used to compute the cache key:

Normalized SQL
Query parameters
Dataset version

Example key structure:

hash(
normalized_sql,
parameters,
dataset_version
)

The SQL normalization step removes irrelevant formatting differences.

Example:

SELECT * FROM risk
SELECT  *  FROM risk

Both produce the same cache key.

---

# Dataset Versioning

Cache entries must be invalidated when source data changes.

For Databricks risk datasets:

Example dataset identifier:

table_name + cob_date + dataset_version

Recent COB dates may refresh frequently.

Example:

T-2 data refreshed every 2 hours.

Older COB dates remain stable.

Cache keys should incorporate dataset version metadata.

---

# Cache Layers

Skadi may support multiple cache layers.

Memory cache

Fastest access
Limited capacity

Disk cache

Larger capacity
Slightly slower access

S3 cache (future)

Shared cache across Skadi nodes

Initial implementation may use memory and disk.

---

# Cache Storage Format

Cached results should be stored in Arrow format.

Advantages:

Columnar format
Efficient streaming
Direct compatibility with analytics tools

Cache entry structure:

cache_key
arrow_batches
row_count
creation_time

---

# Cache Eviction

Caches must evict old entries to control storage usage.

Possible eviction strategies:

LRU (least recently used)
TTL (time-based expiration)

Example TTL rules:

Recent COB dates → short TTL (1–2 hours)
Older COB dates → long TTL (days)

Eviction policy should be configurable.

---

# Partial Result Caching

Some queries may scan large datasets but return small results.

Example:

SELECT SUM(pnl) FROM mxl.gold_risk

Caching the result can dramatically reduce repeated execution cost.

Skadi should cache full query results, not partial fragments.

---

# Cache Logging

Cache activity should be logged.

Example log fields:

query_id
sql_hash
cache_hit
execution_time_ms
rows_returned

These metrics help demonstrate performance improvements.

---

# Example Performance Scenario

Cold query:

SELECT cob_date, SUM(pnl)
FROM mxl.gold_risk
GROUP BY cob_date

Execution time: 18 seconds

Warm query:

Same SQL executed again

Execution time: 2 seconds

Cache hit should be recorded.

---

# Cache Invalidation Strategy

When upstream datasets refresh, cached results may become stale.

Possible invalidation mechanisms:

Dataset version change detection
Manual invalidation
Time-based expiration

Initial POC may rely on TTL.

Future versions should integrate with dataset metadata.

---

# Distributed Cache (Future)

Future Skadi deployments may run multiple nodes.

Possible architecture:

Multiple Skadi nodes
↓
Shared S3 cache

Nodes check shared cache before executing queries.

This allows cluster-wide cache reuse.

---

# AI Implementation Guidance

When implementing caching features:

Ensure cache keys are deterministic.

Avoid caching partial or incomplete results.

Prefer caching Arrow RecordBatch streams.

Cache integration should remain independent of network protocol.

The SQL gateway should use the same cache as the Skadi server.
