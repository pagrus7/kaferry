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

[Component boundaries](docs/design/components.md) · [Review process](docs/development/reviews/README.md) · [Release procedure](docs/releases/README.md) · [Publishing endurance results](docs/endurance-results.md).
