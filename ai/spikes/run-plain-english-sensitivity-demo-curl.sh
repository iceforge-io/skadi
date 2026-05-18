#!/usr/bin/env bash
set -euo pipefail

# Plain-English Market Risk Sensitivity Demo — curl runner
#
# Requires: curl
# Optional: jq (pretty-prints JSON; falls back to raw output)
#
# Usage:
#   bash ai/spikes/run-plain-english-sensitivity-demo-curl.sh
#
# Override base URL:
#   SKADI_BASE_URL=http://myhost:8080 bash ai/spikes/run-plain-english-sensitivity-demo-curl.sh

SKADI_BASE_URL="${SKADI_BASE_URL:-http://localhost:8080}"
ENDPOINT="${SKADI_BASE_URL}/api/semantic/v1/query/demo/execute"

# Use jq for pretty output if available, otherwise cat
if command -v jq >/dev/null 2>&1; then
  pretty() { jq; }
else
  pretty() { cat; }
fi

hr() { echo; echo "─────────────────────────────────────────"; echo; }

echo "Skadi Plain-English Sensitivity Demo"
echo "Base URL : ${SKADI_BASE_URL}"
echo "Endpoint : ${ENDPOINT}"

hr
echo "Query 1: Exposure grid — delta by legal entity and risk class"
echo "Prompt  : Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15"
hr

curl -s -X POST "${ENDPOINT}" \
  -H 'Content-Type: application/json' \
  -d '{"text":"Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15","limit":500}' \
  | pretty

hr
echo "Query 2: Time series — last 30 days rates DV01 by desk"
echo "Prompt  : Show the last 30 days of rates DV01 by desk for CIB"
hr

curl -s -X POST "${ENDPOINT}" \
  -H 'Content-Type: application/json' \
  -d '{"text":"Show the last 30 days of rates DV01 by desk for CIB","limit":500}' \
  | pretty

hr
echo "Done. Check validation.allowed and metadata.executionStatus in each response."
