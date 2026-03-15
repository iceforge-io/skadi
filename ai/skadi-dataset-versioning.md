# Skadi Dataset Versioning

This document describes how Skadi determines whether cached query results
are still valid when upstream datasets change.

Dataset versioning is critical to cache correctness.

If dataset changes are not detected, Skadi could return stale results.

---

# Problem

Skadi caches results of queries executed against Databricks SQL Warehouse.

Example:

SELECT cob_date, SUM(pnl)
FROM mxl.gold_risk
GROUP BY cob_date

If the underlying dataset changes, cached results must be invalidated.

However, many data lake systems (including Databricks) do not provide a simple
table version that can be used directly for cache invalidation.

Skadi therefore needs a dataset versioning strategy.

---

# Dataset Version Concept

Each dataset used by Skadi queries must have a version identifier.

Example:

dataset_key = table_name + dataset_version

Cache keys incorporate the dataset version:

hash(
normalized_sql,
parameters,
dataset_version
)

When dataset_version changes, cached results automatically become invalid.

---

# Market Risk Dataset Example

For MXL risk datasets, data is organized by COB date.

Example table:

mxl.gold_risk

Columns include:

cob_date
book
risk_factor
pnl

Data characteristics:

Older COB dates are stable.
Recent COB dates may refresh multiple times per day.

Example refresh pattern:

T-2 and T-1 data refreshed every 2 hours.

---

# Versioning Strategy Options

## Option 1 — Table Version

Some Delta tables expose version metadata.

Example:

DESCRIBE HISTORY table

If available, the latest version number can be used.

Pros:

Simple implementation.

Cons:

Not granular enough for partitioned datasets.

---

## Option 2 — Partition Versioning (Recommended)

For partitioned datasets such as risk tables, versioning should be tied to partitions.

Example partition:

cob_date

Dataset version could include:

max(cob_date_version)

or a hash derived from partition metadata.

Example:

dataset_version =
hash(
table_name,
partition_versions
)

This allows recent partitions to invalidate cache entries while
older partitions remain valid.

---

## Option 3 — External Version Metadata

Another option is maintaining dataset versions in a metadata table.

Example table:

skadi_dataset_versions

Columns:

dataset_name
dataset_version
last_updated

Pipeline updates the version when new data arrives.

Pros:

Simple and explicit.

Cons:

Requires pipeline integration.

---

# Recommended Initial Approach

For the Skadi POC, use a simplified strategy.

dataset_version =
MAX(last_modified_time)

for the underlying table.

This value can be retrieved using Databricks metadat
