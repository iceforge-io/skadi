# Skadi Query Normalization

This document defines how SQL should be normalized before computing cache keys.

Skadi cache hit rates depend on deterministic normalization.
If logically identical queries normalize differently, the cache will fragment
and performance benefits will collapse.

---

# Goal

Convert semantically equivalent SQL statements into the same normalized form
for cache key generation.

Normalization should remove irrelevant differences such as:

- whitespace
- casing
- redundant aliases where safely removable
- equivalent literal formatting where safely normalizable

Normalization must NOT change query meaning.

---

# Cache Key Principle

Cache keys should be based on:

- normalized SQL
- bound parameters
- dataset version

Example:

hash(
normalized_sql,
parameters,
dataset_version
)

---

# What Should Be Normalized

## 1. Whitespace

Collapse repeated whitespace into single spaces.

Example:

SELECT  *   FROM   risk

becomes:

SELECT * FROM risk

---

## 2. Leading and Trailing Whitespace

Trim surrounding whitespace.

Example:

"   SELECT * FROM risk   "

becomes:

SELECT * FROM risk

---

## 3. Keyword Casing

Normalize SQL keywords consistently.

Recommended approach:

- uppercase SQL keywords
- preserve identifier casing unless the dialect requires otherwise

Example:

select * from risk where cob_date = ?

becomes:

SELECT * FROM risk WHERE cob_date = ?

---

## 4. Line Breaks

Replace line breaks and tabs with spaces.

Example:

SELECT
cob_date,
SUM(pnl)
FROM risk

becomes:

SELECT cob_date, SUM(pnl) FROM risk

---

## 5. Redundant Parentheses

Remove only clearly redundant outer parentheses when safe.

Example:

(SELECT * FROM risk)

may become:

SELECT * FROM risk

This should be done conservatively.

---

## 6. Stable Predicate Ordering (Optional, Advanced)

In some cases, logically equivalent predicates may appear in different order.

Example:

WHERE a = ? AND b = ?

versus

WHERE b = ? AND a = ?

These are logically equivalent in many cases, but safe reordering is difficult.

Recommendation for initial implementation:

Do NOT reorder predicates.

Only normalize formatting.

---

# What Must NOT Be Normalized Aggressively

These transformations risk changing semantics and should be avoided unless using
a proper SQL parser with dialect-aware rewriting.

Do NOT blindly reorder:

- WHERE predicates
- JOIN order
- GROUP BY expressions
- ORDER BY expressions

Do NOT rewrite:

- aliases
- quoted identifiers
- function calls
- interval syntax
- date expressions

Do NOT remove:

- LIMIT
- DISTINCT
- HAVING
- ORDER BY

---

# Parameter Handling

Bound parameters should not be inlined into normalized SQL.

Preferred model:

normalized SQL:
SELECT cob_date, SUM(pnl) FROM risk WHERE cob_date BETWEEN ? AND ?

parameters:
[2026-03-01, 2026-03-15]

Cache key should include both normalized SQL and parameter values.

This avoids:

- SQL injection risks
- accidental string-format variation
- inconsistent cache keys

---

# Dialect Awareness

Skadi sits in front of Databricks and may receive SQL from BI tools through a
PostgreSQL-compatible gateway.

Normalization should preserve dialect-specific constructs unless explicitly
translated elsewhere.

Examples:

CURRENT_DATE
INTERVAL '30 day'
DATE_TRUNC('day', ts)

These should be preserved unless a dedicated dialect layer rewrites them.

---

# Recommended Initial Normalization Strategy

For the POC, keep normalization conservative.

Recommended steps:

1. trim outer whitespace
2. replace tabs/newlines with spaces
3. collapse repeated spaces
4. normalize SQL keyword casing
5. preserve identifiers, literals, and parameter placeholders exactly
6. preserve predicate order

This gives high safety with good practical cache reuse.

---

# Examples

## Example 1

Input:

select  cob_date,   sum(pnl)
from mxl.gold_risk
where book = ?

Normalized:

SELECT cob_date, SUM(pnl) FROM mxl.gold_risk WHERE book = ?

---

## Example 2

Input:

SELECT cob_date, SUM(pnl) FROM mxl.gold_risk WHERE book = ?

Normalized:

SELECT cob_date, SUM(pnl) FROM mxl.gold_risk WHERE book = ?

Same result as Example 1.

---

## Example 3

Input:

SELECT *
FROM mxl.gold_risk
LIMIT 100

Normalized:

SELECT * FROM mxl.gold_risk LIMIT 100

---

# Cache Safety Rules

A normalization rule is acceptable only if:

1. it is deterministic
2. it does not change semantics
3. it improves cache reuse
4. it is testable

If uncertain, do not normalize.

---

# Testing Requirements

Normalization should have unit tests.

Tests should verify:

- equivalent formatting normalizes to same output
- different predicates remain different
- different parameter values produce different cache keys
- LIMIT / ORDER BY / DISTINCT are preserved
- quoted identifiers are preserved

Example test pairs:

same:
- "select * from risk"
- " SELECT  *  FROM risk "

different:
- "SELECT * FROM risk LIMIT 10"
- "SELECT * FROM risk LIMIT 20"

different:
- "SELECT * FROM risk WHERE cob_date = ?"
- "SELECT * FROM risk WHERE cob_date >= ?"

---

# Future Enhancements

Possible future improvements:

- parser-based normalization
- dialect-aware canonicalization
- optional predicate canonicalization for simple AND filters
- identifier qualification normalization

These should only be added with strong tests.

---

# AI Implementation Guidance

When implementing normalization:

- prefer conservative transformations
- do not attempt semantic SQL optimization
- keep logic deterministic
- cover all normalization rules with tests

Normalization quality directly affects cache hit rates.