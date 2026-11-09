# Contributing to Kaferry

Read [AGENTS.md](AGENTS.md) for project conventions and [component boundaries](docs/design/components.md) before changing implementation.

| Directory | Contents |
| --- | --- |
| `src/cmd/`, `src/internal/` | Commands, application packages and component tests |
| `src/tests/` | System tests and broker fixtures |
| `tools/` | CI, documentation, qualification and release tools |
| `docs/`, `examples/` | Documentation and editable tutorial inputs |

The repository has one Go module at its root. Tests and helpers importing `src/internal/` packages belong under `src/`.

## Setup

Use Go 1.27.3, Python 3.11+ and a C compiler for race tests. Run commands from the repository root:

```sh
make build
make check
```

`make check` runs vet and race-enabled component tests without Kafka. For the full local check, install the lint and duplication tools below, then run `make fast`. It also checks documentation, Python tools, source installation and file-copy smoke tests. Vulnerability checks need network access. Broker suites require Docker Engine 25+ and Compose 2.20.2+ for startup healthcheck polling.

## Go lint

Install the pinned golangci-lint binary, then use it for lint or the full local check:

```sh
curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b ./bin v2.15.0
make lint GOLANGCI_LINT=./bin/golangci-lint
make fast GOLANGCI_LINT=./bin/golangci-lint
```

[.golangci.yml](.golangci.yml) defines the checks. Existing and new findings fail lint; function-length and Go-duplication checks exclude test files.

## Duplication checks

Install [jscpd 5.4.2](https://github.com/kucherenko/jscpd/releases/tag/v5.4.2) as `jscpd`, or set `JSCPD=/path/to/jscpd`. Fetch `origin/main` before running `make duplicates` or `make fast`; use `DUPLICATION_BASE=<base-sha>` to match a PR's base.

The check scans Python, including tests, and fails on new clones of at least 50 tokens and 5 lines. Reports remain in `build-results/duplication`, including on failure. Fix duplication or explain a narrow `jscpd:ignore-start` / `jscpd:ignore-end` block when sharing would harm clarity or correctness. Suppressions and configuration changes need review.

## Documentation

README introduces installation and examples; [usage](docs/usage.md) covers workflows; [reference](docs/reference.md) defines settings and results. Design documents explain implementation contracts. Keep user workflows self-contained, with prerequisites and caveats beside the affected examples. Follow the writing guidance in [AGENTS.md](AGENTS.md).

README is capped at 5,500 characters; usage has no shared character cap. Check links, shell syntax and documented SQL:

```sh
python3 -m tools.docs.docs
go test ./src/internal/plan -run 'TestDocumentedExamples|TestUsageCommandSQL'
```

Use placeholders without contacting example brokers; invalid SQL examples use inline code. Edit the archive tutorial under `examples/archive-demo`; `make smoke` checks its commands. See [example packaging](docs/releases/README.md#example-download) for release assets. Generated Avro files and archives stay outside Git.

## Tests

Run targeted package tests while editing. Prefer focused component tests with handwritten mocks; use real Kafka for broker behavior. Follow Given/When/Then phases and use `testify/require` for routine assertions. Keep fatal assertions in the test goroutine. The [component guide](docs/design/components.md#starting-a-component-change) points to contracts and focused tests.

Run Python tools as modules; their tests live beside them:

```sh
python3 -m unittest discover -s tools -t .
```

The replicated fixture workflow checks cleanup diagnostics in the pinned default and newest broker images without starting Kafka. With Docker running and those images from [the catalog](src/tests/qualification/matrix.json) cached, repeat the check with:

```sh
python3 -m unittest -v tools.qualification.storage_census_images_test
```

This check runs isolated containers with no network and stays outside Docker-free Python test discovery. Missing images fail the check.

The same workflow checks source-port reservation in the pinned Toxiproxy image. With that image cached, run:

```sh
python3 -m unittest -v tools.qualification.proxy_port_reservation_images_test
```

This broker-free check uses disposable private network namespaces. A finite test-only source-port pool reproduces the unreserved collision, then verifies the default and isolated project reservations against the running kernel while upstream sockets remain open.

### Test targets

[Makefile](Makefile) defines the commands. Start disposable brokers with `make broker` before integration, fault, load or soak tests, and remove them afterward with `make broker-down`. Before `make fault`, run `make qualification-setup` with the same Python major/minor version to install its independent librdkafka observer.

| Command | Scope |
| --- | --- |
| `make check` | Vet and race-enabled component tests; no broker |
| `make fast` | Full local checks; lint tools required, no broker |
| `make replicated-helpers` | Tagged publisher, audit and ownership-exposure helper regressions under race; no broker |
| `make integration` | CLI copying, fidelity, resume and rebalances |
| `make fault` | Outages, process failures and ownership loss |
| `make load`, `make soak` | Bounded load and sustained-copy checks |
| `make qualification` | Authentication, ACLs and replicated failover; starts its own fixtures |
| `make fuzz`, `make benchmark` | Parser/archive/lifecycle fuzzing and codec/replay benchmarks |

`make fast` and the hosted fast check run `make replicated-helpers`. It retains Go JSON results and requires a completed pass for every selected helper; an empty, skipped or mismatched selection fails. The real zombie-worker broker scenario runs separately in replicated qualification.

For broker matrices, endurance runs and retained evidence, use the [qualification guide](docs/design/qualification.md). To request performance checks in CI, add `ci:performance` to the PR. To compare revisions locally, use the [comparison command](docs/design/qualification.md#paired-performance).

## Submit a change

Keep the patch focused, add tests for changed behavior and update affected user instructions. Run the relevant local checks; full local suites are optional when CI will repeat them. Follow the [review process](docs/development/reviews/README.md) before marking the PR ready. After changing a PR's base branch, push a new commit so CI compares the correct revisions.

For release work, follow the [release procedure](docs/releases/README.md).
