# Skadi — Design Question Records

DQRs track open architectural questions that are not yet decided. They differ from ADRs:
an ADR records a decision that has been made; a DQR records a question that must be
answered before a specific future story or lane can begin.

Once a DQR is resolved, it is marked **Resolved** and the answer is recorded — either
here or in the ADR that captures the final decision. Resolved DQRs are never deleted.

---

| DQR | Question | Status | Blocking |
| --- | --- | --- | --- |
| [DQR-001](DQR-001-contract-definition-format.md) | Contract definition format (YAML, JSON, Java-only, or other) | Open | post-Lane C loading implementation |
| [DQR-002](DQR-002-semantic-execution-delegation.md) | Semantic execution delegation — when and how `skadi-semantic` calls `skadi-server` | Open | post-C5 activation |
| [DQR-003](DQR-003-lineage-market-risk-brain-seams.md) | Lineage and Market Risk Brain integration seams | Open | Lane E |

---

See [ai/adr/README.md](../adr/README.md) for accepted and proposed architecture decisions.
