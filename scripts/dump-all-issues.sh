#!/bin/bash
mkdir -p ai/lane-c

REPO="$(gh repo view --json nameWithOwner -q .nameWithOwner)"

gh issue list \
  --repo "$REPO" \
  --state all \
  --limit 500 \
  --json number,title,state,labels,assignees,milestone,projectItems,createdAt,updatedAt,url,body \
  | jq . 
