#!/usr/bin/env bash
# Smoke-test script for Skadi SQL Gateway.
# Verifies: HTTP health, pgwire connectivity, metadata facade, and (optionally) a real Databricks query.
set -euo pipefail

# ── defaults ─────────────────────────────────────────────────────────────────
HOST="${SKADI_HOST:-localhost}"
PGWIRE_PORT="${SKADI_PGWIRE_PORT:-15432}"
HTTP_PORT="${SKADI_HTTP_PORT:-8090}"
USER="${SKADI_USER:-demo}"
PASSWORD="${SKADI_PASSWORD:-}"
DATABRICKS_TABLE="${SKADI_TEST_TABLE:-}"   # optional: schema.table for real data query

PASS=0
FAIL=0
SKIP=0

# ── helpers ──────────────────────────────────────────────────────────────────
green() { printf '\033[0;32m✓ %s\033[0m\n' "$*"; }
red()   { printf '\033[0;31m✗ %s\033[0m\n' "$*"; }
yellow(){ printf '\033[0;33m~ %s\033[0m\n' "$*"; }

pass() { green "$1"; ((PASS++)); }
fail() { red   "$1"; ((FAIL++)); }
skip() { yellow "$1 (skipped)"; ((SKIP++)); }

psql_query() {
    local sql="$1"
    if [[ -n "$PASSWORD" ]]; then
        PGPASSWORD="$PASSWORD" psql -h "$HOST" -p "$PGWIRE_PORT" -U "$USER" postgres \
            --no-password -t -A -c "$sql" 2>&1
    else
        psql -h "$HOST" -p "$PGWIRE_PORT" -U "$USER" postgres \
            --no-password -t -A -c "$sql" 2>&1
    fi
}

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --host HOST          Gateway host (default: localhost)
  --port PORT          pgwire port (default: 15432)
  --http-port PORT     HTTP/Actuator port (default: 8090)
  --user USER          Database user (default: demo)
  --password PASS      User password (leave empty for trust mode)
  --table SCHEMA.TABLE Run a real Databricks query against this table (optional)
  -h, --help           Show this message

Environment variables:
  SKADI_HOST, SKADI_PGWIRE_PORT, SKADI_HTTP_PORT, SKADI_USER, SKADI_PASSWORD, SKADI_TEST_TABLE
EOF
    exit 0
}

# ── argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --host)       HOST="$2"; shift 2 ;;
        --port)       PGWIRE_PORT="$2"; shift 2 ;;
        --http-port)  HTTP_PORT="$2"; shift 2 ;;
        --user)       USER="$2"; shift 2 ;;
        --password)   PASSWORD="$2"; shift 2 ;;
        --table)      DATABRICKS_TABLE="$2"; shift 2 ;;
        -h|--help)    usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Skadi SQL Gateway Smoke Test"
echo " Host:  $HOST"
echo " Ports: pgwire=$PGWIRE_PORT  http=$HTTP_PORT"
echo " User:  $USER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ── 1. HTTP health ────────────────────────────────────────────────────────────
echo ""
echo "[ HTTP health checks ]"

HEALTH_URL="http://${HOST}:${HTTP_PORT}/actuator/health"
if curl -sf --max-time 5 "$HEALTH_URL" -o /tmp/skadi_health.json; then
    STATUS=$(python3 -c "import json,sys; d=json.load(open('/tmp/skadi_health.json')); print(d.get('status',''))" 2>/dev/null || echo "")
    if [[ "$STATUS" == "UP" ]]; then
        pass "Overall health is UP"
    else
        fail "Overall health returned: $STATUS"
    fi
else
    fail "Cannot reach $HEALTH_URL"
fi

PGWIRE_HEALTH_URL="http://${HOST}:${HTTP_PORT}/actuator/health/pgWire"
if curl -sf --max-time 5 "$PGWIRE_HEALTH_URL" -o /tmp/skadi_pgwire_health.json; then
    PGWIRE_STATUS=$(python3 -c "import json,sys; d=json.load(open('/tmp/skadi_pgwire_health.json')); print(d.get('status',''))" 2>/dev/null || echo "")
    if [[ "$PGWIRE_STATUS" == "UP" ]]; then
        pass "pgWire health is UP"
    else
        fail "pgWire health returned: $PGWIRE_STATUS"
    fi
else
    fail "Cannot reach $PGWIRE_HEALTH_URL"
fi

PING_URL="http://${HOST}:${HTTP_PORT}/ping"
PING_RESP=$(curl -sf --max-time 5 "$PING_URL" 2>/dev/null || echo "")
if [[ "$PING_RESP" == "ok" ]]; then
    pass "/ping returned ok"
else
    fail "/ping returned: $PING_RESP"
fi

# ── 2. pgwire connectivity ────────────────────────────────────────────────────
echo ""
echo "[ pgwire connectivity ]"

if ! command -v psql &>/dev/null; then
    skip "psql not found — skipping pgwire tests (install postgresql-client)"
else
    RESULT=$(psql_query "SELECT 1" 2>&1)
    if echo "$RESULT" | grep -q "^1$"; then
        pass "SELECT 1 returned 1"
    else
        fail "SELECT 1 failed: $RESULT"
    fi

    RESULT=$(psql_query "SELECT current_database()" 2>&1)
    if echo "$RESULT" | grep -q "postgres"; then
        pass "current_database() returned postgres"
    else
        fail "current_database() returned: $RESULT"
    fi

    RESULT=$(psql_query "SELECT version()" 2>&1)
    if echo "$RESULT" | grep -qi "postgresql"; then
        pass "version() returned PostgreSQL-compatible string"
    else
        fail "version() returned: $RESULT"
    fi

    # ── 3. metadata facade ────────────────────────────────────────────────────
    echo ""
    echo "[ metadata facade ]"

    RESULT=$(psql_query "SELECT count(*) FROM information_schema.tables" 2>&1)
    if echo "$RESULT" | grep -qE "^[0-9]+$"; then
        TABLE_COUNT=$RESULT
        if [[ "$TABLE_COUNT" -gt 0 ]]; then
            pass "information_schema.tables returned $TABLE_COUNT rows"
        else
            fail "information_schema.tables returned 0 rows — metadata facade may not be loaded"
        fi
    else
        fail "information_schema.tables query failed: $RESULT"
    fi

    RESULT=$(psql_query "SELECT count(*) FROM information_schema.columns" 2>&1)
    if echo "$RESULT" | grep -qE "^[0-9]+$"; then
        pass "information_schema.columns returned $RESULT rows"
    else
        fail "information_schema.columns query failed: $RESULT"
    fi

    # ── 4. Databricks real query (optional) ───────────────────────────────────
    echo ""
    echo "[ Databricks connectivity ]"

    if [[ -z "$DATABRICKS_TABLE" ]]; then
        skip "No --table specified; skipping real Databricks query test"
    else
        RESULT=$(psql_query "SELECT COUNT(*) FROM $DATABRICKS_TABLE" 2>&1)
        if echo "$RESULT" | grep -qE "^[0-9]+$"; then
            pass "SELECT COUNT(*) FROM $DATABRICKS_TABLE returned $RESULT"
        else
            fail "Query against $DATABRICKS_TABLE failed: $RESULT"
        fi
    fi
fi

# ── summary ───────────────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo " Results: $PASS passed  |  $FAIL failed  |  $SKIP skipped"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [[ "$FAIL" -gt 0 ]]; then
    echo " SMOKE TEST FAILED"
    exit 1
else
    echo " SMOKE TEST PASSED"
    exit 0
fi
