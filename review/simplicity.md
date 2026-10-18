# Simplicity review

Review the supplied changes for opportunities to delete, simplify or consolidate implementation, tests and documentation.

- Default to the smallest solution that satisfies current requirements. Challenge speculative features, unused options, unnecessary state, wrappers, framework code, duplicate representations and abstractions justified only by possible future use. Require a current requirement, demonstrated failure or compatibility contract to keep them.
- Prefer direct control flow and fewer independent states. Flag avoidable branching, nesting, oversized components and repeated logic. Lines of code and cyclomatic complexity are clues, not targets; do not trade clarity or correctness for a smaller score.
- Keep focused components and useful interfaces for actual responsibilities, dependency injection or independent testing. Do not flatten good boundaries, remove meaningful failure handling or invent another abstraction to make something look cleaner.
- Separate necessary behavior from the small support operations used to implement or test it. Repeating scenario steps is different from repeating generic scaffolding; inspect the operations separately even when their arguments are domain-specific.
- Evaluate reuse at individual sites and as a consistent practice across the reviewed scope. Look for shared work hidden behind different names or arguments; do not stop at the most frequent operation. Check directly related components and existing dependencies. State where broader reuse helps and where direct code remains clearer.
- Compare direct code, standard utilities, existing dependencies, focused helpers and maintained libraries. For useful candidates, give representative locations, a rough occurrence count and a small replacement. Weigh total writing, reading, debugging, dependency and migration costs. Recommend a consistent default and justified exceptions when benefits apply broadly. Reformatting or relocating repetition alone does not establish simplification.
- In tests, remove redundant scenarios, assertion-free coverage chasing, excessive setup, broad mocks and helper layers that hide the behavior. Inspect whole changed test functions and their fixtures; preserve useful regression detection, cleanup, concurrency safety and the testing pyramid.
- In docs, remove repetition, stale detail, backstory and files that answer no current user or contributor question. Preserve necessary instructions, limits and compatibility guarantees.

Report concrete simplifications or evidence for keeping the reviewed complexity, including when there are no findings.
