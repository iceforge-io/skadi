# Skadi AI Development Manager - Runbook

## Responsibilities
- Select runnable issues
- Enforce concurrency
- Launch Claude workers
- Track progress
- Update GitHub

## Loop
1. Read state
2. Validate
3. Filter runnable
4. Apply concurrency
5. Schedule
6. Launch worker
7. Monitor
8. Update GitHub

## Runnable Rules
- Status=Ready
- AI Eligible
- No blockers
- No conflicting concurrency

## Worker Lifecycle
1. Reserve issue + branch
2. Create worktree
3. Launch worker
4. Implement + test
5. Open PR
6. Review loop
7. Merge
8. Cleanup

## Concurrency Enforcement
- Block same concurrency group
- Block serialized modules
- Block high-risk overlaps

## Failure Handling
- Retry once
- Then Blocked with comment

## GitHub Updates
- Status transitions
- PR link
- Worker ID
- Comments for audit

## Merge + Cleanup
- Release locks
- Delete worktree
- Close issue
