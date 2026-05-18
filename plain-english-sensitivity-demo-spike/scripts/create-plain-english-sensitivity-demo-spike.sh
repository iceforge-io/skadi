#!/usr/bin/env bash
set -euo pipefail

REPO="iceforge-io/skadi"

# Optional: create labels if missing
ensure_label() {
  local name="$1"
  local color="$2"
  local desc="$3"

  # Idempotent: create if missing, update if present.
  gh label create "$name" \
    --repo "$REPO" \
    --color "$color" \
    --description "$desc" \
    --force
}

ensure_label "spike" "FBCA04" "Time-boxed proof-of-concept or discovery implementation"
ensure_label "semantic-demo" "0E8A16" "Plain-English semantic query demo work"
ensure_label "market-risk" "1D76DB" "Market risk domain work"
ensure_label "databricks" "5319E7" "Databricks integration"
ensure_label "ai-eligible" "BFDADC" "Suitable for AI implementation agent"
ensure_label "backend" "0052CC" "Backend implementation"
ensure_label "docs" "0075CA" "Documentation change"

EPIC_BODY="$(cat <<'EOB'
## Goal

Deliver a working demo this week that lets a user define market-risk sensitivity views in plain English and return either:

1. exposure grids
2. historical time series

from configured Databricks views through Skadi.

## Demo Thesis

Skadi should prove the semantic path end-to-end:

```text
plain English
→ semantic request
→ governed contract validation
→ whitelisted SQL template
→ Skadi semantic execution
→ Databricks result
→ grid / time-series response
```

## Scope

This spike is intentionally narrow.

Implement only two governed demo query shapes:

1. `risk_sensitivity_exposure_grid`
2. `risk_sensitivity_historical_timeseries`

The demo should support prompts such as:

```text
Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15
Show the last 30 days of rates DV01 by desk for CIB
Show vega exposure by risk factor for CIB on 2026-05-15
```

## Non-Goals

Do not implement:

- general NL-to-SQL
- arbitrary SQL generation
- arbitrary Databricks table discovery
- full semantic planner
- buddy-chat runtime
- frontend chat UI
- entitlement engine
- lineage integration
- SQL gateway convergence
- Tableau integration for this spike

## Design Guardrail

The LLM or plain-English adapter must not generate SQL.

It may only produce a structured semantic request. SQL must come from Skadi-owned whitelisted templates backed by governed semantic contracts.

## Required Stories

- Add market-risk sensitivity demo contracts
- Add demo semantic request DTOs
- Add plain-English intent adapter for demo queries
- Add whitelisted SQL template renderer for sensitivity views
- Add demo semantic query execute endpoint
- Add demo runbook and sample prompts

## Acceptance Criteria

- Two semantic demo contracts exist.
- Plain-English demo prompts map to structured semantic requests.
- Semantic requests validate against contract metadata before execution.
- Only whitelisted SQL templates are rendered.
- Databricks view names are configured, not hallucinated.
- Execution uses the existing semantic execution path through Skadi.
- Results return as grid or time-series shaped JSON.
- Demo runbook explains setup, config, prompts, and known limitations.
EOB
)"

echo "Creating spike epic..."
EPIC_URL="$(gh issue create \
  --repo "$REPO" \
  --title "SPIKE: Plain-English Market Risk Sensitivity Query Demo" \
  --label "spike,semantic-demo,market-risk,databricks,ai-eligible" \
  --body "$EPIC_BODY")"

EPIC_NUM="${EPIC_URL##*/}"
echo "Epic created: #$EPIC_NUM $EPIC_URL"

create_story() {
  local title="$1"
  local labels="$2"
  local body="$3"

  echo "Creating story: $title"
  gh issue create \
    --repo "$REPO" \
    --title "$title" \
    --label "$labels" \
    --body "$body"
}

create_story \
"STORY: Spike — Add market-risk sensitivity demo contracts" \
"spike,semantic-demo,market-risk,backend,ai-eligible" \
"$(cat <<EOB
Parent spike: #$EPIC_NUM

## Goal

Add the minimal governed semantic contracts needed for the plain-English market-risk sensitivity demo.

## Scope

Add two demo contracts:

\`\`\`text
risk_sensitivity_exposure_grid
risk_sensitivity_historical_timeseries
\`\`\`

Each contract should define representative measures and dimensions for market-risk sensitivity data.

Suggested measures:

- delta_exposure
- dv01
- vega
- gamma
- curvature, optional

Suggested dimensions:

- cob_date
- legal_entity
- business_unit
- desk
- risk_class
- risk_factor
- currency
- product

For this spike, only mark a conservative subset as groupable/filterable.

## Required Metadata

Include business-facing metadata where supported:

- label
- description
- intended usage
- caveats
- grain
- output shape
- owner/version placeholder
- Databricks source view reference placeholder/config key

## Non-Goals

- Do not implement query execution.
- Do not implement a semantic planner.
- Do not add arbitrary SQL generation.
- Do not require all production contracts to follow this demo shape.

## Acceptance Criteria

- Exposure-grid contract loads successfully.
- Historical-time-series contract loads successfully.
- Existing minimal contract fixtures still load.
- ContractRegistry can return both demo contracts.
- Metadata endpoints expose the contracts if the API is available.
- Validation can recognize measures, dimensions, groupable flags, and filterable flags.
- Relevant Maven tests pass.
EOB
)"

create_story \
"STORY: Spike — Add demo semantic request DTOs" \
"spike,semantic-demo,market-risk,backend,ai-eligible" \
"$(cat <<EOB
Parent spike: #$EPIC_NUM

## Goal

Add the small request/response DTOs needed to carry interpreted plain-English demo requests through validation, SQL-template rendering, and execution.

## Scope

Add DTOs representing:

- original user text
- contract id
- measure
- groupBy dimensions
- filters
- output shape
- date range / COB date
- limit
- confidence
- interpretation warnings
- validation result
- execution metadata
- returned columns and rows

Example exposure-grid semantic request:

\`\`\`json
{
  "contractId": "risk_sensitivity_exposure_grid",
  "measure": "delta_exposure",
  "groupBy": ["legal_entity", "risk_class"],
  "filters": {
    "org": "CIB",
    "currency": "USD",
    "cob_date": "2026-05-15"
  },
  "outputShape": "grid"
}
\`\`\`

Example time-series semantic request:

\`\`\`json
{
  "contractId": "risk_sensitivity_historical_timeseries",
  "measure": "dv01",
  "groupBy": ["desk"],
  "filters": {
    "org": "CIB",
    "risk_class": "rates",
    "date_range": "last_30_days"
  },
  "outputShape": "timeseries"
}
\`\`\`

## Non-Goals

- Do not implement interpretation logic in this story.
- Do not execute Databricks queries in this story.
- Do not add a full semantic planner.
- Do not expose raw SQL in the public request DTO.

## Acceptance Criteria

- DTOs compile.
- DTOs can represent exposure-grid requests.
- DTOs can represent historical-time-series requests.
- DTOs can represent allowed/denied validation outcomes.
- DTOs can represent result metadata and rows.
- Unit tests cover basic construction/serialization if consistent with repo style.
- Relevant Maven tests pass.
EOB
)"

create_story \
"STORY: Spike — Add plain-English intent adapter for demo queries" \
"spike,semantic-demo,market-risk,backend,ai-eligible" \
"$(cat <<EOB
Parent spike: #$EPIC_NUM

## Goal

Add a tiny plain-English adapter that maps demo prompts to structured semantic requests.

## Scope

Create an endpoint or service such as:

\`\`\`text
POST /semantic/v1/query/interpret
\`\`\`

Input:

\`\`\`json
{
  "text": "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15"
}
\`\`\`

Output:

\`\`\`json
{
  "contractId": "risk_sensitivity_exposure_grid",
  "measure": "delta_exposure",
  "groupBy": ["legal_entity", "risk_class"],
  "filters": {
    "org": "CIB",
    "currency": "USD",
    "cob_date": "2026-05-15"
  },
  "outputShape": "grid",
  "confidence": "HIGH"
}
\`\`\`

## Implementation Guidance

Use a pragmatic adapter for the spike:

1. deterministic fallback phrase matching for 8–12 demo prompts
2. optional LLM-backed interpretation only if already easy to wire
3. output must be structured JSON only
4. no SQL generation

Suggested supported phrase coverage:

- delta exposure
- dv01
- vega
- by legal entity
- by risk class
- by desk
- by risk factor
- for CIB
- USD
- rates
- on YYYY-MM-DD
- last 30 days
- history / trend / time series

## Non-Goals

- Do not build a general natural-language parser.
- Do not generate SQL.
- Do not support arbitrary measures or dimensions.
- Do not implement buddy-chat runtime.

## Acceptance Criteria

- At least 6 demo prompts map to expected semantic requests.
- Unknown prompt returns a safe unsupported/ambiguous response.
- Adapter never returns SQL.
- Adapter uses only known contract ids, measures, dimensions, and output shapes.
- Unit tests cover successful and unsupported interpretations.
- Relevant Maven tests pass.
EOB
)"

create_story \
"STORY: Spike — Add whitelisted SQL template renderer for sensitivity views" \
"spike,semantic-demo,market-risk,databricks,backend,ai-eligible" \
"$(cat <<EOB
Parent spike: #$EPIC_NUM

## Goal

Render SQL for the two demo query shapes using only Skadi-owned whitelisted templates.

## Scope

Add a renderer for:

\`\`\`text
risk_sensitivity_exposure_grid
risk_sensitivity_historical_timeseries
\`\`\`

Databricks view names must come from configuration, for example:

\`\`\`properties
skadi.semantic.demo.exposure-view=main.market_risk.v_sensitivity_exposure_demo
skadi.semantic.demo.history-view=main.market_risk.v_sensitivity_history_demo
\`\`\`

Exposure-grid template shape:

\`\`\`sql
select
  <grouping columns>,
  sum(<measure column>) as <measure alias>
from <configured exposure view>
where <validated filters>
group by <grouping columns>
order by <grouping columns>
limit :limit
\`\`\`

Historical-time-series template shape:

\`\`\`sql
select
  cob_date,
  <grouping columns>,
  sum(<measure column>) as <measure alias>
from <configured history view>
where cob_date between :start_date and :end_date
  and <validated filters>
group by cob_date, <grouping columns>
order by cob_date, <grouping columns>
limit :limit
\`\`\`

## Guardrails

- No free-form SQL.
- No free-form table names.
- No free-form column names.
- Only contract-approved measures can render.
- Only contract-approved groupBy dimensions can render.
- Only contract-approved filter dimensions can render.
- Use bounded limit defaults.
- Keep generated SQL observable for debugging, but avoid logging sensitive values if repo standards say to redact.

## Non-Goals

- Do not implement a full SQL planner.
- Do not implement joins.
- Do not implement arbitrary expression support.
- Do not change skadi-sql-gateway.

## Acceptance Criteria

- Exposure-grid semantic request renders expected SQL.
- Historical-time-series semantic request renders expected SQL.
- Invalid measure is rejected before rendering.
- Invalid groupBy dimension is rejected before rendering.
- Invalid filter dimension is rejected before rendering.
- View names are taken from config.
- Unit tests cover allowed and denied render paths.
- Relevant Maven tests pass.
EOB
)"

create_story \
"STORY: Spike — Add demo semantic query execute endpoint" \
"spike,semantic-demo,market-risk,databricks,backend,ai-eligible" \
"$(cat <<EOB
Parent spike: #$EPIC_NUM

## Goal

Add the end-to-end demo endpoint that takes plain English, interprets it, validates it, renders a whitelisted SQL template, executes it through the existing Skadi semantic execution path, and returns grid/time-series JSON.

## Scope

Add endpoint such as:

\`\`\`text
POST /semantic/v1/query/demo/execute
\`\`\`

Input:

\`\`\`json
{
  "text": "Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15"
}
\`\`\`

Response shape:

\`\`\`json
{
  "requestText": "...",
  "semanticRequest": {},
  "validation": {
    "allowed": true
  },
  "execution": {
    "contractId": "risk_sensitivity_exposure_grid",
    "queryId": "...",
    "cacheStatus": "UNKNOWN_OR_HIT_OR_MISS",
    "rowCount": 123
  },
  "columns": [],
  "rows": []
}
\`\`\`

## Required Flow

\`\`\`text
plain English
→ interpret
→ validate against semantic contract
→ render whitelisted SQL template
→ execute through semantic execution / skadi-server seam
→ map result to grid or time-series response
\`\`\`

## Configuration

Add safe demo flags:

\`\`\`properties
skadi.semantic.demo.enabled=false
skadi.semantic.demo.default-limit=500
skadi.semantic.demo.exposure-view=
skadi.semantic.demo.history-view=
\`\`\`

Demo endpoint should not be active accidentally unless enabled.

## Non-Goals

- Do not change skadi-sql-gateway.
- Do not add Tableau integration.
- Do not implement general NL-to-SQL.
- Do not implement full entitlement enforcement.
- Do not implement frontend UI.

## Acceptance Criteria

- Demo endpoint is config gated.
- Valid exposure-grid prompt executes end-to-end.
- Valid time-series prompt executes end-to-end.
- Invalid/unsupported prompt returns safe failure.
- Validation failure prevents execution.
- SQL renderer failure prevents execution.
- Execution errors return credential-safe messages.
- Tests cover happy path with mocked execution.
- Relevant Maven tests pass.
EOB
)"

create_story \
"STORY: Spike — Add demo runbook and sample prompts" \
"spike,semantic-demo,market-risk,databricks,docs,ai-eligible" \
"$(cat <<EOB
Parent spike: #$EPIC_NUM

## Goal

Create a concise runbook that makes the plain-English market-risk sensitivity demo repeatable.

## Scope

Create:

\`\`\`text
ai/spikes/plain-english-sensitivity-demo.md
\`\`\`

Include:

- spike objective
- architecture flow
- required Databricks views
- required config
- supported demo prompts
- expected semantic request JSON
- expected grid response shape
- expected time-series response shape
- curl examples
- troubleshooting
- known limitations
- explicit non-goals

## Required Demo Prompts

Include at least these:

\`\`\`text
Show USD delta exposure by legal entity and risk class for CIB on 2026-05-15
Show the last 30 days of rates DV01 by desk for CIB
Show vega exposure by risk factor for CIB on 2026-05-15
Show gamma exposure by desk and currency for CIB on 2026-05-15
\`\`\`

## Known Limitations Section

Must explicitly state:

- SQL is template-rendered, not LLM-generated.
- Only two query shapes are supported.
- Only configured Databricks views are supported.
- No arbitrary table discovery.
- No full semantic planner.
- No production entitlement engine.
- No SQL gateway convergence.

## Acceptance Criteria

- Runbook exists.
- Runbook includes config example.
- Runbook includes curl examples.
- Runbook includes supported prompts.
- Runbook explains expected result shapes.
- Runbook documents known limitations.
- Runbook is usable by a demo operator without reading code.
EOB
)"

echo
echo "Done."
echo "Epic: #$EPIC_NUM $EPIC_URL"
echo
echo "Review created issues:"
echo "gh issue list --repo $REPO --search \"Plain-English Market Risk Sensitivity Query Demo\" --state all"
