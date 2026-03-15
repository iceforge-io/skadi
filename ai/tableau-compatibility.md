# Tableau Compatibility Guide for Skadi SQL Gateway

This document describes the subset of PostgreSQL behavior required for Tableau
to successfully connect and query the Skadi SQL Gateway.

The Skadi gateway does not need to implement the full PostgreSQL protocol.
Only the behaviors required by BI tools such as Tableau Desktop are necessary.

---

# Primary Goal

Allow Tableau Desktop to connect using the PostgreSQL connector and run queries
against Databricks through the Skadi SQL Gateway.

Expected connection path:

Tableau Desktop
↓
PostgreSQL Connector
↓
Skadi SQL Gateway
↓
Databricks SQL Warehouse

---

# Required PostgreSQL Protocol Features

The gateway must support the following message flow.

StartupMessage
Authentication
ParameterStatus
ReadyForQuery

Then repeated query cycles:

Query
RowDescription
DataRow
CommandComplete
ReadyForQuery

CancelRequest should also be supported.

---

# Authentication

For the POC, support simple password authentication.

Required methods:

AuthenticationOk
ErrorResponse (if authentication fails)

Future enhancements may include:

OAuth
token-based authentication
SSO integration

---

# Metadata Queries Tableau Uses

When connecting, Tableau typically runs queries like:

SELECT * FROM information_schema.tables

SELECT * FROM information_schema.columns

SELECT * FROM pg_catalog.pg_tables

SELECT * FROM pg_catalog.pg_namespace

The gateway must return valid results for these queries.

These can be synthetic responses generated from Databricks metadata.

---

# Information Schema Expectations

The following columns must exist.

information_schema.tables

table_catalog
table_schema
table_name
table_type

information_schema.columns

table_schema
table_name
column_name
data_type
ordinal_position

---

# Databricks Metadata Mapping

Databricks catalogs and schemas should map as follows.

Databricks catalog → PostgreSQL database or catalog
Databricks schema → PostgreSQL schema
Databricks table → PostgreSQL table

---

# SQL Query Execution

After metadata discovery, Tableau will execute queries such as:

SELECT
cob_date,
book,
SUM(pnl)
FROM mxl.gold_risk
WHERE cob_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY 1,2

The gateway must:

Receive SQL query
Forward query to Databricks SQL Warehouse
Stream results back to the client

---

# Data Type Mapping

Arrow or JDBC result types must be mapped to PostgreSQL types.

Common mappings:

INT32 → INTEGER
INT64 → BIGINT
FLOAT64 → DOUBLE PRECISION
VARCHAR → TEXT
BOOLEAN → BOOLEAN
TIMESTAMP → TIMESTAMP

---

# Result Streaming

The Skadi gateway should stream query results using Arrow internally,
but send rows to the client using PostgreSQL DataRow messages.

Conversion pipeline:

Databricks JDBC
↓
Arrow RecordBatch
↓
PgWire row stream

---

# Query Cancellation

Tableau may send CancelRequest messages.

The gateway should:

Locate the running query
Call JDBC Statement.cancel()
Terminate result streaming

---

# Cache Behavior

The Skadi cache sits between the gateway and Databricks.

Query execution flow:

Client query
↓
SQL normalization
↓
Cache lookup

If cache hit:

Return cached Arrow batches

If cache miss:

Execute query on Databricks
Cache Arrow result batches
Return rows to client

---

# Performance Demonstration Goal

The Skadi Tableau POC should demonstrate improved performance on repeated queries.

Example benchmark:

Cold query execution: 15 seconds
Warm cache execution: 2 seconds

Cache hits should be logged.

---

# Known Limitations for POC

The following PostgreSQL features are not required for the initial POC.

Prepared statements
Transactions
DDL support
Complex protocol extensions

Only simple query execution is required.

---

# Testing Tools

Gateway compatibility should be tested using:

Tableau Desktop
psql
DBeaver

These tools help validate PostgreSQL compatibility.

---

# AI Implementation Guidance

When implementing PostgreSQL protocol features:

Focus only on behavior required for BI connectivity.

Do not attempt to implement the full PostgreSQL server feature set.

Prefer small targeted compatibility shims.

The gateway is a protocol adapter, not a database engine.
