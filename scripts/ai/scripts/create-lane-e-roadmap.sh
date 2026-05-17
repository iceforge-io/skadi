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
#   ./ai/scripts/create-lane-e-roadmap.sh
#
# Defaults assume:
#   owner: iceforge-io
#   repo:  skadi
#   project: 1
# ------------------------------------------------------------------------------

OWNER="${OWNER:-iceforge-io}"
REPO="${REPO:-skadi}"
PROJECT_NUMBER="${PROJECT_NUMBER:-1}"
MILESTONE="${MILESTONE:-Lane E — Semantic Execution Activation}"

PROJECT_OWNER="${PROJECT_OWNER:-$OWNER}"
PROJECT_OWNER_TYPE="${PROJECT_OWNER_TYPE:-organization}" # organization or user

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

  if gh label list --repo "$REPO_SLUG" --json name --jq '.[].name' | grep -Fxq "$name"; then
    echo "Label exists: $name"
  else
    echo "Creating label: $name"
    gh label create "$name" \
      --repo "$REPO_SLUG" \
      --color "$color" \
      --description "$description" >/dev/null
  fi
}

ensure_milestone() {
  local title="$1"

  if gh api "repos/$REPO_SLUG/milestones?state=all" --paginate \
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
    --json title,url \
    --jq --arg title "$title" '.[] | select(.title == $title) | .url' \
    | head -n 1
}

create_issue_if_missing() {
  local title="$1"
  local body_file="$2"
  local labels="$3"

  local existing_url
  existing_url="$(issue_exists_url_by_title "$title" || true)"

  if [[ -n "$existing_url" ]]; then
    echo "Issue exists: $title"
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

get_project_json() {
  if [[ "$PROJECT_OWNER_TYPE" == "user" ]]; then
    gh project view "$PROJECT_NUMBER" \
      --owner "$PROJECT_OWNER" \
      --format json
  else
    gh project view "$PROJECT_NUMBER" \
      --owner "$PROJECT_OWNER" \
      --format json
  fi
}

PROJECT_JSON="$(get_project_json)"
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
  echo "$PROJECT_FIELDS_JSON" | jq -r --arg name "$field_name" '.fields[] | select(.name == $name) | .id' | head -n 1
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

  gh project item-add "$PROJECT_NUMBER" \
    --owner "$PROJECT_OWNER" \
    --url "$issue_url" \
    --format json \
    | jq -r '.id'
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
  local work_type="$2"
  local module="$3"
  local priority="$4"
  local ai_eligible="$5"
  local concurrency_group="$6"
  local status="${7:-Ready}"

  set_single_select_field "$item_id" "Status" "$status"
  set_single_select_field "$item_id" "Lane" "Lane E"
  set_single_select_field "$item_id" "Work Type" "$work_type"
  set_single_select_field "$item_id" "Module" "$module"
  set_single_select_field "$item_id" "Priority" "$priority"
  set_single_select_field "$item_id" "AI Eligible" "$ai_eligible"
  set_single_select_field "$item_id" "Concurrency Group" "$concurrency_group"
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