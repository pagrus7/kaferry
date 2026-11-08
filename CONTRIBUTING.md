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

Run commands from the repository root with Go, Python 3.11+ and a C compiler for race tests available. `make fast` also needs [golangci-lint](#go-lint), [jscpd](#duplication-checks), and a locally available `origin/main` for the duplication comparison:

```sh
make build
make fast
```

`make fast` runs CI's wording, formatting, Go lint, Python duplication and Go vulnerability gates, followed by race-enabled component tests, Python tests, documentation checks, source installation and file-copy smoke tests. It does not need Kafka. See [test targets and prerequisites](#test-targets) for broker suites.

Use Go 1.27.3 and `DUPLICATION_BASE=<base-sha>` to match CI's toolchain and comparison base. Vulnerability checks use pinned govulncheck v1.9.0, downloaded and cached by Go, and need access to the vulnerability database. CodeQL and dependency review run separately in GitHub.

Run Python tools as modules, for example `python3 -m tools.docs.docs`. Their tests live beside them; run all with `python3 -m unittest discover -s tools -t .`.

[Component boundaries](docs/design/components.md) · [Review process](docs/development/reviews/README.md) · [Release procedure](docs/releases/README.md) · [Publishing endurance results](docs/design/qualification.md#publish-endurance-results).

## Go lint

Install the pinned [golangci-lint v2.15.0 binary](https://golangci-lint.run/docs/welcome/install/local/) with `curl` and `sh`, then lint from the repository root:

```sh
curl -sSfL https://golangci-lint.run/install.sh | sh -s -- -b ./bin v2.15.0
make lint GOLANGCI_LINT=./bin/golangci-lint
make fast GOLANGCI_LINT=./bin/golangci-lint
```

The target verifies [.golangci.yml](.golangci.yml) and checks all Go packages in the repository. Existing and new findings fail the check.

The Go lint check includes vet and test files in the default Go build, except that function length and duplicate-code findings exclude `_test.go` files. The separate duplication check below covers Python, including test files. `make fast` uses this lint gate for vet; standalone `make check` runs vet and race tests without requiring golangci-lint.

## Duplication checks

Install [jscpd 5.4.2](https://github.com/kucherenko/jscpd/releases/tag/v5.4.2), make the binary available as `jscpd`, and run `make duplicates`. It uses the same [.jscpd.json](.jscpd.json) as CI to scan Python under `src/` and `tools/`, including test files. Comments are skipped; consistent identifier renaming and changes to literal values still match. The default comparison is the locally available `origin/main`; use `make duplicates DUPLICATION_BASE=<base-sha>` for another base, or `JSCPD=/path/to/jscpd` for a downloaded binary.

The quality job compares both revisions with the current scan settings and fails on new clones of at least 50 tokens and 5 lines. Existing matches form the comparison baseline. JSON/SARIF reports remain in `build-results/duplication` even when the gate fails; the Actions summary lists both locations of each new match. Fix the duplication or use a narrow `jscpd:ignore-start` / `jscpd:ignore-end` comment block with a reason when sharing would harm test clarity or correctness. Configuration changes and suppressions need the same review as code.

## Documentation

Keep one authoritative explanation per topic: README introduces installation and runnable examples; [usage](docs/usage.md) covers operating procedures; [reference](docs/reference.md) defines settings, inspection, metrics and the single exit-code table. Design documents hold implementation contracts and optional background. Usage and reference must stand alone: keep the instructions, guarantees and limits needed to run safely without reading `decisions.md`.

Explain who does what, when, under which conditions and what the reader should do. Define project terms, use human-readable ranges and qualify configurable defaults. Keep prerequisites beside setup and caveats beside affected examples; state the affected inputs, dependency, protection and remaining risk together. Link exact contracts and upstream issues, not source-code lines. Avoid broad readiness labels, backstory, unnecessary mechanism detail and unrequested workarounds. Broker upgrades do not remove existing records.

Before finishing docs, comments or diagnostics, verify changed claims and reread them as a current user. Walk usage changes through preparation, first run, input forms, stopping, resuming and checking results. Remove repetition before shortening explanations into jargon. README is capped at 5,500 characters; usage has no shared character cap.

Run `python3 -m tools.docs.docs` for local links and shell syntax, and `go test ./src/internal/plan -run 'TestDocumentedExamples|TestUsageCommandSQL'` for fenced and embedded SQL. Use placeholders without contacting example brokers; invalid SQL examples use inline code.

The archive tutorial uses editable SQL, TOML and JSON under `examples/archive-demo`. Build its temporary bundle with `go run ./src/cmd/package-examples .cache/archive-demo.tar.gz`; generated Avro files and archives stay outside Git. `make smoke` runs the documented tutorial commands. Release checks use `KAFERRY_TUTORIAL_BUNDLE` to test the exact downloadable asset; see the [release procedure](docs/releases/README.md#example-download).

## Tests

Most tests exercise pure functions or individual components with handwritten mocks. Kafka and filesystem failures use injected ports; file-component tests use real files, and app tests replace Fx factories. Fewer [broker/CLI tests](src/tests/system) cover complete copy paths, rebalances and restart. PR checks also build the base revision, verify resume or explicit format rejection in both directions, and check archive interoperability. Functional tests use Given/When/Then phases. Use `testify/require` for routine assertions; keep direct domain checks where clearer. Fatal assertions run in the test goroutine.

### Test targets

Run from the repository root with Python 3 available. [Makefile](Makefile) defines these targets; [suite definitions](tools/ci/suites.py) share Go test selection and required execution evidence with CI and standalone qualification. Kafka fixtures need Docker and Compose.

| Command | Scope / prerequisite | CI owner |
| --- | --- | --- |
| `make check` | Vet and race-enabled unit/mock/component tests, kfake and shutdown subprocesses; no broker | Every PR/main; all release platforms |
| `make fast` | Wording, formatting, Go lint, Python duplication, vulnerabilities, race/script/docs checks, build/licenses and file-copy smoke; golangci-lint and jscpd required | Optional before push; CI runs these entrypoints |
| `make broker` | Start disposable source/destination Kafka clusters | Fixture setup |
| `make integration` | Broker/CLI paths, fidelity, resume and rebalances; start brokers first | Every PR/main; release |
| `make fault` | Outages and ownership loss; start brokers first | Code PRs, nightly, weekly, release |
| `make load` | Bounded Kafka/archive copy caps/fidelity, overlap-cache behavior and file-copy metric overhead; start brokers first | Every PR/main, weekly, release |
| `make soak` | Streaming fidelity, lag, memory and shutdown; start brokers first | Weekly and release, 30 minutes |
| `make qualification` | Security/ACLs and replicated failover; starts TLS/SASL fixture | Code PRs, nightly, weekly, release |
| `make fuzz` | SQL parser, Avro decoder and capture/replay state; 100,000 iterations per target | Nightly; weekly |
| `make benchmark` | Avro blocks and overlapping replay: CPU/allocations | Nightly; weekly |
| `make broker-down` | Stop disposable clusters | Fixture cleanup |

Time-window arithmetic, boundaries and overflow use deterministic unit tests in `make check`. Set `FUZZ_ITERATIONS` to change the per-target fuzz budget; failures remain fatal.

### Coverage limits

- Most broker tests start after their group coordinator is ready. `TestKafkaQuickStart` starts its own fresh broker and exercises the first group lookup before the offsets topic exists.
- Component tests in `make check` withhold coordinator, join, offset, commit and fetch replies and check cleanup.

See [client limits](docs/design/client-compatibility.md#operational-limits) for deployment constraints.

To run the documented [local Kafka playground](docs/usage.md#local-kafka-playground) alone, use Docker with port 9092 free and no `kaferry-kafka-demo` container. No fixture brokers are needed:

```sh
KAFERRY_QUICKSTART=1 go test -count=1 -tags=integration -run '^TestKafkaQuickStart$' -v ./src/tests/system
```

PR and release integration jobs enable this test and verify that it ran; ordinary local integration runs skip it unless requested.

### Test results

[Standalone qualification lab](docs/design/qualification.md) covers broker/authentication matrices, published-version contracts, managed-service inputs and multi-day local runs.

[tools/ci/ci.py](tools/ci/ci.py) writes logs, environment/result JSON and measurements to ignored `build-results/`, setting `KAFERRY_REPORT_DIR`. Direct load/soak commands need that variable to save metrics.

The dedicated [application endurance profile](docs/design/qualification.md#endurance-on-a-persistent-host) uses 24 hours and 24/32 partitions with achieved-load gates; it is outside the scheduled CI suites. Soak defaults to two minutes of publishing locally. `KAFERRY_SOAK_DURATION` overrides it (minimum one minute); every run keeps the process alive for ten seconds after the later of the scheduled publishing end and final source submission, before shutdown and final reconciliation.

```sh
make broker
make build
python3 -m tools.ci.ci load make load
KAFERRY_SOAK_DURATION=30m python3 -m tools.ci.ci soak make soak
python3 -m tools.ci.ci summary
make broker-down
```

To include destination stalls, after `make broker`, use its separate destination cluster:

```sh
KAFKA_TEST_DEST_BROKERS=127.0.0.1:40092 \
KAFERRY_SOAK_DEST_STALL_CONTAINER=kaferry-compat-kafka-dr-1 \
KAFERRY_SOAK_DURATION=30m \
python3 -m tools.ci.ci soak make soak
```

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) runs on main pushes, PRs and manual dispatch. Bounded load measurements run in parallel with unit and broker-integration tests and appear in the `load` job summary and `checks-load` artifact: throughput, sampled RSS, work caps and logs. Summaries list the slowest tests. [Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) select fault/security, fuzz and benchmarks nightly; the weekly selection adds load and a 30-minute soak. Unchanged suites reuse recent successful executions under the [scheduled qualification rules](docs/design/qualification.md#compatibility-matrix); weekly fuzzing always runs. Manual dispatch runs all suites; its publishing duration, ten seconds without new input, and cleanup must fit the 40-minute test timeout. Release assets retain a concise [qualification summary](docs/releases/README.md#qualification-summary); detailed artifacts retain evidence for 30 days; each test invocation runs once and failures stay visible in Actions.

PRs also run quality/security checks and [additional integrity suites](.github/workflows/risk.yml). Only the prose allowlist skips fault and security/failover tests; unknown paths run both. The ordinary PR suites always run. Strict branch protection applies to admins; CodeQL errors and High/Critical security findings block merging.

Use targeted package tests during local edits; full local suites are optional when CI will repeat them. Keep real-Kafka tests for broker behavior; move tests only after comparing executed names and the combined coverage on the same revision.

After retargeting a PR to a different base branch, push a new commit so CI compares the new base and head.

### Quality indicators

| Indicator | Where / meaning |
| --- | --- |
| CI | README badge; formatting and documentation checks, vet, race-enabled tests, build, license-notice validation and broker integration |
| Go coverage | README badge and Codecov PR/file view show line coverage; Actions reports total Go statement coverage per suite. All Go packages are instrumented; Codecov combines unit/mock/file-component and broker integration coverage, and its patch/project checks fail PRs when changed-line or total coverage falls below the thresholds in `codecov.yml` |
| Static analysis | [Quality workflow](https://github.com/pagrus7/kaferry/actions/workflows/quality.yml): golangci-lint, including Staticcheck, for Go; CodeQL for Go, Python and workflows |
| Security | govulncheck fails on reachable Go advisories; CodeQL findings and Dependabot alerts appear in GitHub Security. Dependency review checks PRs; updates require review |
| Complexity / duplication | [Go lint](#go-lint) checks cognitive complexity, nested conditions, function length and Go duplication; findings across all Go packages fail CI. The quality summary/artifact reports normalized Python clones ≥50 tokens and ≥5 lines, including tests. New clones fail CI; existing clones remain review signals. Run [duplication checks](#duplication-checks) locally |
| Extended tests | README badge; load, fault, ACL/failover, fuzz, codec benchmarks and a 30-minute soak with destination stalls. Soak duration is configurable on manual dispatch |
| Performance | Dated SHA/environment JSON and summaries: throughput, sampled RSS/growth, lag, caps, shutdown, ns/op and allocations. Prior successful measurements compare only matching settings; shared runners provide no regression gate |

Missing measurements are unavailable. Coverage and vulnerability scans describe Go code; badges report only the listed checks.

### Paired performance

[Performance runs](.github/workflows/performance.yml) are separate from required PR checks. They reuse Kafka, disabled-metrics Avro file-copy, and optional archive capture/replay/file-copy assertions, with larger inputs, a warm-up per binary and six interleaved pairs, alternating which binary runs first on one runner. Summaries show median head/base throughput and the pair range; raw metrics, binary/toolchain identities and broker images stay in the `performance` artifact for 30 days. Kafka input is capped at 160,000 records; file/archive input at 200,000. Archive comparisons require Avro support in both revisions.

Nightly runs start with A/A calibration (the same binary on both sides). Review at least 20 calibration runs before setting the repository variable `PERFORMANCE_MODE=comparison`; this enables changed-main comparisons against the last retained compatible scheduled measurement. No compatible baseline starts another calibration. Expired data requires a fresh baseline. Manual dispatch accepts a base revision; adding `ci:performance` opts a PR into the configured mode. In comparison mode, PRs compare against the merge base. No performance threshold blocks merging; choose warnings only after measuring A/A noise.
