# Dataset Versioning

Cache correctness requires dataset-aware invalidation.

cache_key = hash(normalized_sql, parameters, dataset_version)

Dataset version changes cause automatic cache invalidation.

Typical market‑risk datasets refresh frequently for recent COB dates,
while older partitions remain stable.
