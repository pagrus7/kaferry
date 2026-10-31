# Simplicity review

Review the supplied scope for opportunities to delete, simplify or consolidate code, tests and documentation. Follow the [shared workflow](README.md) for follow-ups and coverage evidence.

- Keep the smallest solution satisfying current requirements. Challenge speculative features, unused options, unnecessary state, wrappers and duplicate representations. Retained complexity needs a current requirement, demonstrated failure or compatibility contract.
- Prefer direct control flow and fewer independent states. Complexity scores are clues; preserve correctness, useful component boundaries, dependency injection and meaningful failure handling.
- Inspect repeated support operations separately from domain-specific scenario steps. Compare direct code, standard tools, existing dependencies and small helpers within the affected scope. Recommend reuse only when total writing, reading, debugging and maintenance cost improves; show representative sites and a concrete replacement. Moving repetition alone is not simplification.
- In tests, remove redundant scenarios, assertion-free coverage chasing, excessive setup, broad mocks and helpers that hide behavior. Preserve regression detection, cleanup and concurrency safety.
- In docs, remove repetition, stale detail, backstory and files serving no current user or contributor question. Preserve necessary instructions, limits and compatibility guarantees.
- For applicable coverage follow-ups, inspect the reviewed scope, gap record and raw evidence. Independently assess every retained gap against callers, invariants and required behavior; report agreement or disagreement and useful removal or test options.

Report concrete simplifications or evidence for keeping the reviewed complexity. No findings is valid.
