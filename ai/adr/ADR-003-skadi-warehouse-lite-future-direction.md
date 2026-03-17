# ADR-003: Skadi Warehouse Lite (Future Direction)

## Status

Proposed (Future)

## Context

Skadi currently operates as:

* SQL gateway
* Caching layer
* Acceleration layer over Databricks

There is an opportunity to evolve Skadi into:

* A cache-aware execution engine
* A hybrid planner using cache + Delta + delegation

## Decision

We define **Skadi Warehouse Lite** as a future architectural direction:

> Skadi becomes a cache-aware SQL planner and execution layer capable of selecting between:
>
> * Cache
> * Delta storage
> * Delegated engines

## Rationale

* Reduce compute cost
* Improve performance for repeated queries
* Leverage existing cache investment
* Build differentiated product capability

## Why Not Now

Current priorities:

* SQL endpoint stability
* Compatibility with BI tools
* Observability and security
* Production readiness

## Preconditions

* Cache correctness guarantees
* Stable query normalization
* Metadata and statistics layer
* Execution abstraction

## Consequences

If adopted:

* Significant architectural expansion
* Need for optimizer and execution engine
* Increased complexity
* Strong product differentiation

## Alternatives

* Continue as cache-only layer
* Use external engines exclusively (Trino/Spark)
* Build thinner compatibility layer only

## Decision Horizon

Revisit after:

* Tableau SQL Endpoint is production-ready
* Cache hit rates justify investment
