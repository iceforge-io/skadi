##STORY A4 is finished in `skadi-sql-gateway` as a small, rule-based SQL normalizer + 
translator with unit tests around Tableau patterns, and with the key correctness requirement: 
**parameter markers are preserved and bound correctly.**

A4 acceptance criteria coverage

- SQL normalizer (stable whitespace/casing normalization for cache keys): Done

  - Implemented in `SqlNormalizer.normalizeForKey(...)` (comment stripping, whitespace collapse, punctuation spacing, uppercase outside literals).
  - Covered by `SqlNormalizerTest`.

- Translation rules: Done (rule-based MVP)
    - LIMIT/OFFSET normalization:
        - MySQL OFFSET … LIMIT … → LIMIT … OFFSET …
        - Includes param reordering when pattern is OFFSET ? LIMIT ? so binds stay correct.
        - Covered by MySqlToDatabricksTranslatorTableauTest (swap + no-swap edge cases).
      
    - date/timestamp casts:
      - Postgres `expr::date / expr::timestamp / expr::timestamptz → CAST(expr AS date/timestamp)
      - Covered by `SqlDialectBridgeTableauTest` and `PostgresToDatabricksTranslatorTableauTest`.
    
    - identifier quoting normalization:
      - Postgres "ident" → `ident` outside literals/comments
      - Covered by tests.
      
  - Tableau patterns: Done (at the level we can do without a full SQL parser)
    - Covered in `SqlDialectBridgeTableauTest, MySqlToDatabricksTranslatorTableauTest, PostgresToDatabricksTranslatorTableauTest`.

  - Parameter markers preserved and bound correctly: Done
    - Postgres $n → ? with correct order (including out-of-order/repeated markers) via `ParameterMarkerRewriter`
    - MySQL ? preserved, and LIMIT/OFFSET rewrite reorders bind values when applicable.
    - Covered by tests above + existing `ParameterMarkerRewriterTest`.
        
  - Quality gates
      - Build/tests: ✅ `mvn -pl skadi-sql-gateway -am test` passing after the latest updates.
    
  - What’s intentionally not in A4 (future extension)
      - Full SQL parsing/AST-based translation (we’re doing safe heuristics).
      - Deep Tableau metadata-query interception (that’s A5).
      - Type mapping/row encoding (A6, already done for PG wire).