# Tests review

Review behavior changes, tests and files used by tests or CI, including documented examples. Follow the [shared workflow](README.md) for scope, follow-ups and evidence.

- Require assertions that detect plausible regressions in observable behavior. Flag execution-only tests, implementation mirrors and cancellation tests that never verify completion. Failure messages should show the expected and observed result.
- Check failure handling, boundaries, ownership, progress, ordering, shutdown, restart and saved-state compatibility where affected. For useful gaps, propose the smallest scenario and expected outcome. Do not chase coverage of unreachable states, simple getters or trivial pass-throughs.
- Keep domain expectations visible; use existing assertions and fixtures for routine setup, checks and cleanup. Prefer focused components with narrow handwritten mocks and fewer integration tests for real boundaries. Flag excessive setup or helpers that hide behavior; broader reuse analysis belongs to the simplicity review.
- Concurrency tests need observable synchronization, bounded waits and cleanup. Prefer fake clocks to sleeps; check leaks, races and nondeterministic assumptions.
- Verify completed final-head CI and coverage under the shared process. Distinguish Go statement coverage from Codecov line and patch coverage; inspect raw evidence rather than trusting percentages or collector output. For retained changed gaps, trace callers and invariants and justify testing, removal or retention. Do not invent calls solely to cover unreachable code.
