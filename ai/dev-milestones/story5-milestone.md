## Implemented STORY A5
Metadata discovery (information_schema facade) in skadi-sql-gateway so Tableau/JDBC metadata queries can be answered without hitting Databricks yet.
# What’s now implemented
Query interception layer in pgwire:
- Detects common discovery queries against:
    -`information_schema.schemata`
    - `information_schema.tables`
    - `information_schema.columns`
    - plus a couple of `pg_catalog` probes (`pg_namespace, pg_database`) and `current_database() / current_schema()`
- Returns synthetic rowsets (currently TEXT columns only, matching pgwire MVP’s RowDescription).
- 
Metadata caching (TTL):
- Added a small in-memory TTL cache (`MetadataCache`) for metadata query responses.
- Default TTL is 2 minutes right now.

Basic mapping (MVP):
- DBX catalog exposed as: main
- DBX schema exposed as: public
- PG “database” name exposed as: postgres
- 
Where the code lives

-`skadi-sql-gateway/src/main/java/org/iceforge/skadi/sqlgateway/metadata/**`
    - `MetadataQueryRouter` (pattern match + responses)
    - `MetadataCache` (TTL cache)
    - `MetadataRowSet` (tabular response abstraction)

Hooked into pgwire in:
    - `skadi-sql-gateway/src/main/java/org/iceforge/skadi/sqlgateway/pgwire/PgWireSession.java`

Tests
- Added InformationSchemaFacadeTest which connects via PostgreSQL JDBC and confirms information_schema.tables returns at least one row.
- Note: it uses `preferQueryMode=simple` to avoid depending on full extended-query protocol support yet.