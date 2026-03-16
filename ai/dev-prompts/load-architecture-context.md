Before continuing implementation, read the architecture documentation in the repository.

Read the following files:

ai/claude-instructions.md
ai/system-map.md
ai/query-flow.md
ai/skadi-cache-architecture.md
ai/skadi-query-normalization.md
ai/skadi-dataset-versioning.md
ai/skadi-node-architecture.md
ai/tableau-compatibility.md
ai/tableau-query-patterns.md
ai/skadi-architecture-diagram.md

Also read the human documentation under:

docs/architecture/

After reading them, produce a short summary covering:

1. The intended Skadi architecture
2. Responsibilities of each module
3. The query execution lifecycle
4. Cache key generation
5. Dataset versioning and cache invalidation
6. The role of the PostgreSQL SQL gateway
7. How the system scales across nodes

Then compare this intended architecture with the current codebase.

Identify:
- mismatches
- missing components
- areas where the implementation diverges from the architecture.