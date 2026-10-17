# Simplicity review

Review the supplied changes for opportunities to delete, simplify or consolidate implementation, tests and documentation.

- Default to the smallest solution that satisfies current requirements. Challenge speculative features, unused options, unnecessary state, wrappers, framework code, duplicate representations and abstractions justified only by possible future use. Require a current requirement, demonstrated failure or compatibility contract to keep them.
- Prefer direct control flow and fewer independent states. Flag avoidable branching, nesting, oversized components and repeated logic. Lines of code and cyclomatic complexity are clues, not targets; do not trade clarity or correctness for a smaller score.
- Keep focused components and useful interfaces for actual responsibilities, dependency injection or independent testing. Do not flatten good boundaries, remove meaningful failure handling or invent another abstraction to make something look cleaner.
- In tests, remove redundant scenarios, assertion-free coverage chasing, excessive setup, broad mocks and helper layers that hide the behavior. Preserve useful regression detection and the testing pyramid.
- In docs, remove repetition, stale detail, backstory and files that answer no current user or contributor question. Preserve necessary instructions, limits and compatibility guarantees.
