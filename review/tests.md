# Tests review

Review the supplied behavior changes, tests and files used by tests or CI, including documented examples.

- Check that assertions would detect a plausible regression in observable behavior. Flag tests that only execute code, repeat the implementation or succeed after cancellation without verifying completion.
- Identify useful gaps in failure handling, boundaries, ownership, progress, ordering, shutdown, restart and compatibility of saved progress, checkpoints and query fingerprints with the previous version. For each gap, propose the smallest useful scenario and its expected outcome. Do not chase a percentage or demand tests for unreachable states, simple getters or trivial pass-throughs.
- Reuse focused fixtures and narrow handwritten mocks or injected ports. Prefer independent component tests; use fewer integration and end-to-end tests for real boundaries. Flag duplication, oversized helpers, excessive setup and unnecessary abstractions.
- Concurrency tests need observable synchronization and bounded waits; prefer fake clocks to sleeps. Check cleanup, goroutine leaks, race risks and nondeterministic assumptions.
- Use coverage reports when provided, distinguishing Go statement coverage from Codecov line coverage and patch coverage. Never infer test success or coverage from a test's presence.
