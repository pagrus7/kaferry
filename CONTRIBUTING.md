# Contributing to Kaferry

Read [contributor instructions](AGENTS.md) before changing the project.

| Directory | Contents |
| --- | --- |
| `src/cmd/` | Kaferry and the example-bundle generator |
| `src/internal/` | Application packages, component tests and test data |
| `src/tests/` | System tests and broker qualification fixtures |
| `docs/` | User guides, design contracts, release notes, review guidance and site assets |
| `examples/` | Editable inputs for the downloadable archive tutorial |
| `tools/` | CI, release, qualification, review and documentation tools |

The repository has one Go module, with `go.mod` at the root. Go tests and helper commands that import `src/internal/` packages belong under `src/`.

Run commands from the repository root with Go and Python 3.11+ available:

```sh
make build
make fast
```

`make fast` runs formatting checks, vet, race-enabled component tests, Python tests, documentation checks, source installation and file-copy smoke tests. It does not need Kafka. See [test targets and prerequisites](docs/design/components.md#test-targets) for broker suites.

Run Python tools as modules, for example `python3 -m tools.docs.docs`. Their tests live beside them; run all with `python3 -m unittest discover -s tools -t .`.

[Component boundaries](docs/design/components.md) · [Review process](docs/development/reviews/README.md) · [Release procedure](docs/releases/README.md) · [Publishing endurance results](docs/design/qualification.md#publish-endurance-results).

## Go lint

Install the pinned [golangci-lint v2.15.0 binary](https://golangci-lint.run/docs/welcome/install/local/) with `curl` and `sh`, then lint from the repository root:

```sh
curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b ./bin v2.15.0
make lint GOLANGCI_LINT=./bin/golangci-lint
```

The target verifies [.golangci.yml](.golangci.yml) and checks all Go packages in the repository. Existing and new findings fail the check.

The Go lint check includes test files in the default Go build, except that function length and duplicate-code findings exclude `_test.go` files. The separate duplication check below still scans tests in Go and Python, including files behind build tags. `make fast` continues to run `go vet` and the formatting check.

## Duplication checks

Install [jscpd 5.4.2](https://github.com/kucherenko/jscpd/releases/tag/v5.4.2), make the binary available as `jscpd`, and run `make duplicates`. It uses the same [.jscpd.json](.jscpd.json) as CI to scan Go and Python under `src/` and `tools/`, including tests with any build tags. Comments are skipped; consistent identifier renaming and changes to literal values still match. The default comparison is the locally available `origin/main`; use `make duplicates DUPLICATION_BASE=<base-sha>` for another base, or `JSCPD=/path/to/jscpd` for a downloaded binary.

The quality job compares both revisions with the current scan settings and fails on new clones of at least 50 tokens and 5 lines. Existing matches form the comparison baseline. JSON/SARIF reports remain in `build-results/duplication` even when the gate fails; the Actions summary lists both locations of each new match. Fix the duplication or ask the simplicity reviewer to approve a narrow `jscpd:ignore-start` / `jscpd:ignore-end` comment block with a reason when sharing would harm test clarity or correctness. Configuration changes and suppressions need the same review as code.
