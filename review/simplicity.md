# Simplicity review

Review the supplied changes for opportunities to delete, simplify or consolidate implementation, tests and documentation.

- Default to the smallest solution that satisfies current requirements. Challenge speculative features, unused options, unnecessary state, wrappers, framework code, duplicate representations and abstractions justified only by possible future use. Require a current requirement, demonstrated failure or compatibility contract to keep them.
- Prefer direct control flow and fewer independent states. Flag avoidable branching, nesting, oversized components and repeated logic. Lines of code and cyclomatic complexity are clues, not targets; do not trade clarity or correctness for a smaller score.
- Keep focused components and useful interfaces for actual responsibilities, dependency injection or independent testing. Do not flatten good boundaries, remove meaningful failure handling or invent another abstraction to make something look cleaner.
- Separate necessary behavior from the small support operations used to implement or test it. Repeating scenario steps is different from repeating generic scaffolding; inspect the operations separately even when their arguments are domain-specific.
- For the most repeated handwritten support operation, give representative locations and a rough occurrence count. Compare it with an actual standard or ecosystem utility: name the candidate and show a small replacement, or justify keeping the current code. A necessary component or lifecycle does not by itself justify the surrounding machinery.
- Evaluate selective reuse through language features, existing helpers or maintained libraries. Weigh the effort to write, read and debug custom code against dependency maintenance and actual production or test impact. Reuse need not require a framework or a wholesale rewrite. Reformatting or relocating repetition alone does not establish simplification.
- In tests, remove redundant scenarios, assertion-free coverage chasing, excessive setup, broad mocks and helper layers that hide the behavior. Inspect whole changed test functions and their fixtures; preserve useful regression detection, cleanup, concurrency safety and the testing pyramid.
- In docs, remove repetition, stale detail, backstory and files that answer no current user or contributor question. Preserve necessary instructions, limits and compatibility guarantees.

Report concrete simplifications or evidence for keeping the reviewed complexity, including when there are no findings.
