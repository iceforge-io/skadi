# Skadi Semantic JDBC Demo Handoff

This bundle contains an implementation-ready handoff for adding a demo-only semantic execution mode that talks directly to Databricks through the JDBC path.

The core decision:

```text
Plain-English request
  -> semantic demo intent adapter
  -> governed semantic contract / validation
  -> constrained SQL renderer
  -> direct Databricks JDBC execution
  -> small tabular response DTO
```

This is a demo/spike path only. It must not become the long-term production semantic execution architecture.

## Files

- `scripts/create-semantic-jdbc-demo-issue.sh` — creates the GitHub issue.
- `prompts/claude-code-semantic-jdbc-demo.md` — Claude Code implementation prompt.
- `docs/semantic-jdbc-demo-runbook.md` — implementation runbook and module boundaries.
- `docs/guardrails.md` — architectural guardrails.
- `config/application-semantic-jdbc-demo.properties` — example runtime config.

## Suggested use

```bash
cd /path/to/skadi
bash /path/to/this-bundle/scripts/create-semantic-jdbc-demo-issue.sh
```

Then give `prompts/claude-code-semantic-jdbc-demo.md` to Claude Code from the Skadi repo root.
