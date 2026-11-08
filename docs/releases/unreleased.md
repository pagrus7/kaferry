# Kaferry unreleased

## Changes

- Organize application code and tests under `src/`, documentation under `docs/`, and repository tooling under `tools/`. Keep runnable tutorial inputs under `examples/` and one Go module at the repository root.
- Run rebalance churn qualification with the selected executable, including release archives, so its record and saved-progress checks cover that binary rather than an in-process build of the checkout.
- Retain qualification storage ownership markers when data deletion fails, so remaining data can still be checked before later cleanup.

## Upgrade

- For a release containing this layout, source installations use `go install github.com/pagrus7/kaferry/src/cmd/kaferry@VERSION`, with `CGO_ENABLED=0`. Earlier release tags retain their original `cmd/kaferry` path. Downloaded binaries and container commands keep the same names and behavior.
- Run repository Python commands from the root with `python3 -m tools.CATEGORY.MODULE`. Use `make broker` for the Kafka fixtures now stored under `src/tests/`.
