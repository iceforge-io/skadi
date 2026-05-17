#!/usr/bin/env bash
set -euo pipefail

# ------------------------------------------------------------------------------
# Skadi Lane E Roadmap Setup
#
# Requirements:
#   gh auth status
#   jq installed
#
# Usage:
#   chmod +x scripts/create-lane-e-roadmap.sh
#   scripts/create-lane-e-roadmap.sh
#
# Optional overrides:
#   OWNER=iceforge-io REPO=skadi PROJECT_NUMBER=1 scripts/create-lane-e-roadmap.sh
# ------------------------------------------------------------------------------

OWNER="${OWNER:-iceforge-io}"
REPO="${REPO:-skadi}"
PROJECT_NUMBER="${PROJECT_NUMBER:-1}"
MILESTONE="${MILESTONE:-Lane E — Semantic Execution Activation}"

PROJECT_OWNER="${PROJECT_OWNER:-$OWNER}"
REPO_SLUG="$OWNER/$REPO"

echo "Using repo:        $REPO_SLUG"
echo "Using project:     $PROJECT_OWNER Project #$PROJECT_NUMBER"
echo "Using milestone:   $MILESTONE"
echo

command -v gh >/dev/null || { echo "ERROR: gh CLI is required"; exit 1; }
command -v jq >/dev/null || { echo "ERROR: jq is required"; exit 1; }

gh auth status >/dev/null

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

ensure_label() {
  local name="$1"
  local color="$2"
  local description="$3"

  if gh api "repos/$REPO_SLUG/labels?per_page=100" --paginate \
      --jq '.[].name' | grep -Fxq "$name"; then
    echo "Label exists: $name"
    return 0
  fi

  echo "Creating label: $name"

  if gh label create "$name" \
      --repo "$REPO_SLUG" \
      --color "$color" \
      --description "$description" >/dev/null 2>&1; then
    return 0
  fi

  # Defensive: another run/user may have created it between check and create.
  if gh api "repos/$REPO_SLUG/labels?per_page=100" --paginate \
      --jq '.[].name' | grep -Fxq "$name"; then
    echo "Label exists after create race: $name"
    return 0
  fi

  echo "ERROR: failed to create label: $name" >&2
  return 1
}

ensure_milestone() {
  local title="$1"

  if gh api "repos/$REPO_SLUG/milestones?state=all&per_page=100" --paginate \
      --jq '.[].title' | grep -Fxq "$title"; then
    echo "Milestone exists: $title"
  else
    echo "Creating milestone: $title"
    gh api "repos/$REPO_SLUG/milestones" \
      --method POST \
      -f title="$title" \
      -f description="Lane E activates semantic execution behind the existing skadi-server execution seam while keeping SQL gateway convergence separate." \
      >/dev/null
  fi
}

issue_exists_url_by_title() {
  local title="$1"

  gh issue list \
    --repo "$REPO_SLUG" \
    --state all \
    --search "$title in:title" \
    --limit 200 \
    --json number,title,url \
  | jq -r --arg title "$title" \
      '[.[] | select(.title == $title)] | sort_by(.number) | .[0].url // empty'
}

create_issue_if_missing() {
  local title="$1"
  local body_file="$2"
  local labels="$3"

  local existing_url
  existing_url="$(issue_exists_url_by_title "$title" || true)"

  if [[ -n "$existing_url" ]]; then
    echo "Issue exists: $title" >&2
    echo "$existing_url"
    return 0
  fi

  echo "Creating issue: $title" >&2

  gh issue create \
    --repo "$REPO_SLUG" \
    --title "$title" \
    --body-file "$body_file" \
    --milestone "$MILESTONE" \
    --label "$labels"
}

PROJECT_JSON="$(gh project view "$PROJECT_NUMBER" --owner "$PROJECT_OWNER" --format json)"
PROJECT_ID="$(echo "$PROJECT_JSON" | jq -r '.id')"

if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "null" ]]; then
  echo "ERROR: Could not resolve project id for $PROJECT_OWNER Project #$PROJECT_NUMBER"
  exit 1
fi

echo "Resolved project id: $PROJECT_ID"
echo

PROJECT_FIELDS_JSON="$(gh project field-list "$PROJECT_NUMBER" --owner "$PROJECT_OWNER" --format json)"

field_id_by_name() {
  local field_name="$1"

  echo "$PROJECT_FIELDS_JSON" \
    | jq -r --arg name "$field_name" '.fields[] | select(.name == $name) | .id' \
    | head -n 1
}

option_id_by_field_and_name() {
  local field_name="$1"
  local option_name="$2"

  echo "$PROJECT_FIELDS_JSON" \
    | jq -r \
      --arg field "$field_name" \
      --arg option "$option_name" \
      '.fields[]
       | select(.name == $field)
       | .options[]?
       | select(.name == $option)
       | .id' \
    | head -n 1
}

add_to_project() {
  local issue_url="$1"

  echo "Adding to project: $issue_url" >&2

  local item_id
  item_id="$(
    gh project item-add "$PROJECT_NUMBER" \
      --owner "$PROJECT_OWNER" \
      --url "$issue_url" \
      --format json \
      | jq -r '.id'
  )"

  if [[ -z "$item_id" || "$item_id" == "null" ]]; then
    echo "ERROR: Could not add issue to project: $issue_url" >&2
    return 1
  fi

  echo "$item_id"
}

set_single_select_field() {
  local item_id="$1"
  local field_name="$2"
  local option_name="$3"

  local field_id
  local option_id

  field_id="$(field_id_by_name "$field_name")"
  option_id="$(option_id_by_field_and_name "$field_name" "$option_name")"

  if [[ -z "$field_id" || "$field_id" == "null" ]]; then
    echo "Skipping missing project field: $field_name"
    return 0
  fi

  if [[ -z "$option_id" || "$option_id" == "null" ]]; then
    echo "Skipping missing option '$option_name' for field '$field_name'"
    return 0
  fi

  echo "Setting project field: $field_name = $option_name"

  gh project item-edit \
    --id "$item_id" \
    --project-id "$PROJECT_ID" \
    --field-id "$field_id" \
    --single-select-option-id "$option_id" \
    >/dev/null
}

set_roadmap_fields() {
  local item_id="$1"
  local lane="$2"
  local work_type="$3"
  local module="$4"
  local priority="$5"
  local ai_eligible="$6"
  local concurrency_group="$7"
  local status="${8:-Ready}"

  set_single_select_field "$item_id" "Status" "$status"
  set_single_select_field "$item_id" "Lane" "$lane"
  set_single_select_field "$item_id" "Work Type" "$work_type"
  set_single_select_field "$item_id" "Module" "$module"
  set_single_select_field "$item_id" "Priority" "$priority"
  set_single_select_field "$item_id" "AI Eligible" "$ai_eligible"
  set_single_select_field "$item_id" "Concurrency Group" "$concurrency_group"
}

print_project_field_options_hint() {
  local report_file="$TMP_DIR/project-field-options.txt"

  echo "$PROJECT_FIELDS_JSON" \
    | jq -r '
      .fields[]
      | select(.options != null)
      | "\nFIELD: " + .name + "\nOPTIONS:\n" + ([.options[].name] | map("  - " + .) | join("\n"))
    ' > "$report_file"

  echo
  echo "Project field option report: $report_file"
  echo "If options were skipped, compare the script values with the report above."
}

# ------------------------------------------------------------------------------
# Labels and milestone
# ------------------------------------------------------------------------------

ensure_label "lane-e" "5319E7" "Lane E semantic execution activation"
ensure_label "semantic-execution" "0E8A16" "Semantic execution path and delegation"
ensure_label "architecture" "1D76DB" "Architecture decision or boundary work"
ensure_label "adr" "7057ff" "Architecture Decision Record"
ensure_label "dqr" "BFD4F2" "Design Question Record"
ensure_label "server" "FBCA04" "skadi-server related work"
ensure_label "semantic" "006B75" "skadi-semantic related work"
ensure_label "guardrail" "D93F0B" "Scope control or architectural guardrail"
ensure_label "future-lane" "C5DEF5" "Future lane placeholder or deferred work"

ensure_milestone "$MILESTONE"

# ------------------------------------------------------------------------------
# Issue bodies
# ------------------------------------------------------------------------------

LANE_E_PARENT_BODY="$TMP_DIR/lane-e-parent.md"
cat > "$LANE_E_PARENT_BODY" <<'EOF'
## Purpose

Lane E activates semantic execution in Skadi.

This lane picks up the skeletons and boundaries created in earlier lanes and turns the semantic execution path into a real runtime capability.

## Lane Decision

Semantic-first execution delegates to `skadi-server` through the existing `SkadiServerQueryExecutionService` seam.

SQL gateway convergence is explicitly out of scope for this lane.

## Scope

Lane E includes:

- resolving DQR-002
- recording the semantic execution delegation decision as a new ADR
- creating a Lane E activation boundary document
- implementing `SkadiServerQueryExecutionService`
- proving that semantic execution can delegate to `skadi-server`
- preserving the SQL gateway as-is

## Out of Scope

Lane E does not:

- modify `skadi-sql-gateway`
- route SQL gateway traffic through `skadi-server`
- merge gateway and server execution ownership
- introduce pgwire/mysql semantic awareness
- solve full gateway convergence

## Guardrail

Semantic activation and SQL gateway convergence are separate architectural moves.

This lane activates semantic execution only.
EOF

E1_BODY="$TMP_DIR/e1.md"
cat > "$E1_BODY" <<'EOF'
## Purpose

Resolve DQR-002 and activate the semantic execution path that was deliberately left as a skeleton in Lane C and guarded through Lane D.

This is the first implementation story for Lane E.

## Required Decision

Resolve:

```text
ai/dqr/DQR-002-semantic-execution-delegation.md
```

Chosen option:

> Partial convergence: semantic execution delegates to `skadi-server`; SQL gateway remains on its existing direct execution path for now.

This means:

- `skadi-semantic` calls `skadi-server`
- `skadi-server` remains the execution/cache owner for this semantic path
- `skadi-sql-gateway` is not changed
- full SQL gateway convergence is deferred to a later dedicated issue/lane

## Required Documentation

### 1. Resolve DQR-002

Update:

```text
ai/dqr/DQR-002-semantic-execution-delegation.md
```

Set status to resolved and record the selected option.

### 2. Add new ADR

Create a new ADR:

```text
ai/adr/ADR-0XX-semantic-execution-delegates-to-skadi-server.md
```

Decision:

> Semantic execution will delegate to `skadi-server` via the existing `SkadiServerQueryExecutionService` seam. SQL gateway convergence is intentionally deferred and must not be combined with semantic execution activation.

Use the next valid ADR number after ADR cleanup.

If ADR-013 is dropped as duplicate of ADR-012, this can become the new ADR-013. Otherwise use ADR-014.

### 3. Create Lane E activation boundary doc

Create:

```text
ai/lane-e/lane-e-activation-boundary.md
```

The boundary doc must define:

- what Lane E activates
- what Lane E must not activate
- ownership boundaries between `skadi-semantic`, `skadi-server`, and `skadi-sql-gateway`
- failure behavior if `skadi-server` is unavailable
- config-gating expectations
- test boundaries
- explicit statement that SQL gateway convergence is out of scope

## Implementation Scope

Activate:

```text
skadi-semantic/src/main/java/org/iceforge/skadi/semantic/service/SkadiServerQueryExecutionService.java
```

Replace the current `UnsupportedOperationException` skeleton with a real implementation that delegates to `skadi-server`.

Expected behavior:

- accept a `QueryExecutionRequest`
- convert it into the existing `skadi-server` query API request shape
- call `POST /api/v1/queries`
- return a `QueryExecutionResult`
- handle unavailable `skadi-server` cleanly
- preserve the `QueryExecutionService` interface as the seam

## Configuration

Add semantic execution client config under a namespace such as:

```properties
skadi.semantic.execution.enabled=false
skadi.semantic.execution.skadi-server-base-url=http://localhost:8080
skadi.semantic.execution.timeout-ms=30000
```

Defaults must be safe.

Semantic execution must not become active accidentally in tests or local startup unless explicitly enabled.

## Explicitly Out of Scope

Do not change:

```text
skadi-sql-gateway/
```

Do not:

- route SQL gateway traffic through `skadi-server`
- remove gateway JDBC execution
- merge gateway cache ownership with server cache ownership
- alter pgwire/mysql behavior
- introduce SQL gateway semantic awareness
- resolve gateway convergence in this issue

This issue may mention that prior execution-alignment debt is partially addressed for semantic execution, but gateway convergence remains future work.

## Tests

Add tests covering:

- `SkadiServerQueryExecutionService` no longer throws `UnsupportedOperationException`
- request mapping from `QueryExecutionRequest` to the skadi-server API request
- response mapping back to `QueryExecutionResult`
- failure behavior when `skadi-server` is unreachable
- timeout behavior
- disabled/config-gated behavior if applicable
- no dependency from `skadi-sql-gateway` to `skadi-semantic`
- no source changes under `skadi-sql-gateway`

## Acceptance Criteria

- [ ] DQR-002 is marked resolved
- [ ] New ADR records the decision
- [ ] `ai/lane-e/lane-e-activation-boundary.md` exists
- [ ] `SkadiServerQueryExecutionService` has a real implementation
- [ ] semantic execution delegates to `skadi-server`
- [ ] SQL gateway remains unchanged
- [ ] tests prove the delegation path
- [ ] tests or checks prove no gateway convergence was introduced
- [ ] dev-status is updated with Lane E state
- [ ] Maven verify passes

## Guardrail Statement

Lane E activates semantic execution only.

It does not converge the SQL gateway.

Gateway convergence is a separate architectural decision with a larger blast radius and must be handled in a future dedicated lane or issue.
EOF

GATEWAY_FUTURE_BODY="$TMP_DIR/gateway-future.md"
cat > "$GATEWAY_FUTURE_BODY" <<'EOF'
## Purpose

Capture SQL gateway convergence as a separate future architectural move.

This issue exists to prevent Lane E semantic activation from expanding into SQL gateway convergence.

## Context

Lane E activates semantic execution by delegating from `skadi-semantic` to `skadi-server` through the `SkadiServerQueryExecutionService` seam.

That does not imply that `skadi-sql-gateway` should immediately route through the same path.

## Future Decision Required

Before implementation, decide whether SQL gateway execution should:

- continue direct execution
- delegate through `skadi-server`
- share cache ownership differently
- expose semantic awareness
- remain a separate protocol-facing compatibility surface

## Explicitly Not Part of Lane E

Do not implement this as part of:

```text
Lane E: E1 — Resolve semantic execution delegation and activate SkadiServerQueryExecutionService
```

## Acceptance Criteria

- [ ] A future DQR or ADR is created for SQL gateway convergence
- [ ] blast radius is evaluated separately
- [ ] pgwire/mysql compatibility impact is analyzed
- [ ] cache ownership impact is analyzed
- [ ] Tableau compatibility impact is analyzed
EOF

# ------------------------------------------------------------------------------
# Create issues
# ------------------------------------------------------------------------------

LANE_E_PARENT_TITLE="Skadi Platform: Lane E — Semantic Execution Activation"
E1_TITLE="Lane E: E1 — Resolve semantic execution delegation and activate SkadiServerQueryExecutionService"
GATEWAY_FUTURE_TITLE="Future Lane: Resolve SQL gateway convergence separately from semantic activation"

LANE_E_PARENT_URL="$(create_issue_if_missing "$LANE_E_PARENT_TITLE" "$LANE_E_PARENT_BODY" "lane-e,semantic-execution,architecture")"
E1_URL="$(create_issue_if_missing "$E1_TITLE" "$E1_BODY" "lane-e,semantic-execution,semantic,server,adr,dqr")"
GATEWAY_FUTURE_URL="$(create_issue_if_missing "$GATEWAY_FUTURE_TITLE" "$GATEWAY_FUTURE_BODY" "future-lane,guardrail,architecture")"

echo
echo "Created/found issues:"
echo "  Lane E parent:       $LANE_E_PARENT_URL"
echo "  E1:                  $E1_URL"
echo "  Gateway future:      $GATEWAY_FUTURE_URL"
echo

# ------------------------------------------------------------------------------
# Add to project and set fields
# ------------------------------------------------------------------------------

echo "Adding issues to project and setting roadmap fields..."
echo

LANE_E_PARENT_ITEM_ID="$(add_to_project "$LANE_E_PARENT_URL")"
set_roadmap_fields "$LANE_E_PARENT_ITEM_ID" "Lane E" "EPIC" "Platform" "High" "Yes" "Lane E" "Ready"

E1_ITEM_ID="$(add_to_project "$E1_URL")"
set_roadmap_fields "$E1_ITEM_ID" "Lane E" "SINGLE_STORY" "Semantic" "High" "Yes" "Lane E" "Ready"

GATEWAY_ITEM_ID="$(add_to_project "$GATEWAY_FUTURE_URL")"
set_roadmap_fields "$GATEWAY_ITEM_ID" "Future" "DESIGN" "SQL Gateway" "Medium" "Yes" "Future" "Ready"

print_project_field_options_hint

echo
echo "Done."
echo
E1_NUMBER="${E1_URL##*/}"

echo
echo "Next recommended command:"
echo "  gh issue view $E1_NUMBER --repo $REPO_SLUG --web"
