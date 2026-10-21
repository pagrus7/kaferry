# Component design

Contributor reference for boundaries, ownership and checks. See [copy behavior](decisions.md) and [syntax](grammar.md).

## Composition

Constructors receive dependencies through narrow interfaces. [Fx modules](../internal/app/modules.go) compose the application; domain packages do not depend on Fx. Records, cursors and pure helper functions use concrete types.

Command parses, configures and plans; Engine acquires adapters; Runner copies through shared source, sink and progress contracts.

Explain and status use the same planning stages and read-only metadata/archive interfaces; explain also uses the Kafka positioner. Text and JSON render the same report; inspection never acquires execution adapters or locks.

SQL syntax and validation errors share `sql.Diagnostic`, which carries a source position and preserves its cause. AST positions stay out of normalized plans, fingerprints and checkpoints. The CLI adds SQL file/stdin names; errors report token categories and fixed syntax hints without echoing input values or SQL excerpts. Caller-created ASTs without positions produce plain messages; configuration and operational errors keep their own context.

## Components

| Component | Package | Responsibility | Dependency boundary |
| --- | --- | --- | --- |
| SQL parser | [sql](../internal/sql) | Tokenize/parse; report input positions | `sql.Parser` |
| Configuration | [config](../internal/config) | Read TOML and defaults | `config.Reader`, `FileReader` |
| Planner | [plan](../internal/plan) | Validate, merge connections, compile time bounds and resolve environment values | `plan.Builder`, `Environment` |
| Command | [app](../internal/app) | Order planning stages; classify failures | Parser, Reader, Builder, Engine |
| Engine | [app](../internal/app) | Validate endpoints/progress; acquire and close query resources | KafkaFactory, FileFactory, RunnerFactory |
| Copy inspection | [inspect](../internal/inspect) | Preview endpoints, progress and starting positions without writes | KafkaFactory, Files, PositionerFactory |
| Archive inspection | [file/inspection.go](../internal/file/inspection.go) | Read headers and checkpoint identity without locks or recovery | inspect.Files |
| Runner | [copy](../internal/copy) | Consume, filter, submit, process outcomes/lifecycle events | Source, Sink, ProgressStore, ProgressTracker, Clock |
| Partition completion | [copy/partition.go](../internal/copy/partition.go) | Decide timestamp stops, tip sampling and waits for a fixed sampled end | Pure assignment state; runner performs I/O and logs outcomes |
| Flow control | [copy/flow.go](../internal/copy/flow.go) | Hold unsent input; decide admission, worker-wide pause and commit cadence | Narrow work-count port; runner applies partition pauses and saves progress |
| Progress tracker | [copy](../internal/copy) | Track consumed/resolved prefixes, ownership and caps | ProgressTracker, fresh instance through ProgressFactory |
| Kafka metadata | [franz](../internal/franz) | Inspect identities, offsets and optional destination config | sourceMetadata, destinationMetadata, MetadataClient |
| Kafka contracts | [kafkaport](../internal/kafkaport) | Endpoint interfaces and saved-progress identity checks | Connection, SourceProgress; client libraries stay in adapters |
| Commit metadata codec | [commitmeta](../internal/commitmeta) | Encode/decode the versioned commit format, without Kafka I/O | `commitmeta.Codec`; private wire types, independent golden/invalid-input tests |
| Kafka positioning | [kafkaport](../internal/kafkaport) | Resolve saved/start positions and windows; batch lookups | PositionerFactory, AssignmentPositioner, OffsetReader, TimeReader; client conversions in franz |
| Kafka input | [franz](../internal/franz) | Hand off callbacks/fetches to the core loop; position, pause and prove gaps | ConsumerClient, sourceMetadata, gapReader, rebalanceBridge |
| Kafka output | [franz](../internal/franz) | Validate records, bound admission and report asynchronous deliveries | ProducerClient, destinationMetadata, Clock |
| Kafka commits | [franz](../internal/franz) | Save positions, eligible stops and, for file outputs, file ID and extent; reject commits without a group member ID and generation | offsetCommitter, copy.OutputProgress |
| Observability | [observability](../internal/observability) | Export immutable copy snapshots and actual save results through OTLP/HTTP; bound cleanup and redact diagnostics | `copy.Observer`, per-query Session/Factory, environment/file ports; SDK stays in adapter |
| Shutdown budget | [app/shutdown.go](../internal/app/shutdown.go) | Enforce whole-process stop deadline; identify the blocked operation | StopObserver, Shutdown; CLI alone owns termination |
| File adapters | [file](../internal/file) | Encode/decode frames, snapshot input, recover output, persist checkpoints | ArchiveDecoder, ArchiveEncoder, CheckpointStore, I/O ports |
| File publication | [file](../internal/file) | Publish a durable header without replacing another creator | Publisher, private filesystem port |
| CLI | [cmd](../cmd/kaferry), [app](../internal/app) | Parse flags, compose components and handle signals | Command/Engine; Fx at composition only |

## Ownership

- Factories create fresh mutable runner/progress/adapter state for each query; assignment state is generation-specific.
- The engine manages each query's resources. Adapters manage Kafka clients, files and locks.
- [Endpoint creation](decisions.md#creation) defines validation, output ownership and progress-save order. Adapters close after the runner saves confirmed progress; Kafka source close performs no saves.
- Runner mutates progress in one event loop. Late outcomes from an old assignment cannot advance current progress.
- Sources provide owned record buffers; sinks retain them until the corresponding write outcome.
- Source cursors are distinct from record provenance. Progress persists confirmed next positions and identity metadata.
- Output confirmation precedes progress persistence: Kafka delivery or file fsync. No component saves past unresolved work.

## Conventions

- Priorities: correctness, simplicity, throughput, CI duration, binary size.
- Single-purpose structs behind narrow interfaces; keep core logic Fx-free. Lines of code and cyclomatic complexity are signals, not targets.
- Keep user documentation example-based. CI caps [README](../README.md) at 5,500 characters and enforces fewer than 10,000 across README, [usage](../docs/usage.md) and [file format](../docs/file-format.md). Grammar tests parse and plan the fenced SQL examples; invalid examples use inline code.

## Tests

Most tests exercise pure functions or individual components with handwritten mocks. Kafka and filesystem failures use injected ports; file-component tests use real files, and app tests replace Fx factories. Fewer [broker/CLI tests](../tests/compat) cover complete copy paths, rebalances and restart. PR checks also build the base revision, verify resume or explicit format rejection in both directions, and check archive interoperability. Functional tests use Given/When/Then phases. Use `testify/require` for routine assertions; keep direct domain checks where clearer. Fatal assertions run in the test goroutine.

### Test targets

Run from the repository root. [Makefile](../Makefile) defines these targets; Kafka fixtures need Docker and Compose.

| Command | Scope / prerequisite | CI owner |
| --- | --- | --- |
| `make check` | Vet and race-enabled unit/mock/component tests, kfake and shutdown subprocesses; no broker | Every PR/main; all release platforms |
| `make fast` | Local fast checks: formatting, check, script/docs checks, build/licenses and file-copy smoke | Optional before push; CI runs these entrypoints |
| `make broker` | Start disposable source/destination Kafka clusters | Fixture setup |
| `make integration` | Broker/CLI paths, fidelity, resume and rebalances; start brokers first | Every PR/main; release |
| `make fault` | Outages and ownership loss; start brokers first | Code PRs, nightly, weekly, release |
| `make load` | Bounded Kafka-copy caps/fidelity and file-copy metric overhead; start brokers first | Every PR/main, weekly, release |
| `make soak` | Streaming fidelity, lag, memory and shutdown; start brokers first | Weekly and release, 30 minutes |
| `make qualification` | Security/ACLs and replicated failover; starts TLS/SASL fixture | Code PRs, nightly, weekly, release |
| `make fuzz` | SQL parser, CBOR/Avro decoders; 100,000 iterations per target | Nightly; weekly |
| `make benchmark` | CBOR framing and Avro blocks: CPU/allocations | Nightly; weekly |
| `make broker-down` | Stop disposable clusters | Fixture cleanup |

Time-window arithmetic, boundaries and overflow use deterministic unit tests in `make check`. Set `FUZZ_ITERATIONS` to change the per-target fuzz budget; failures remain fatal.

### Coverage limits

- Broker tests start after their group coordinator is ready; they do not cover cold coordinator startup.
- Component tests in `make check` withhold coordinator, join, offset, commit and fetch replies and check cleanup.

See [client limits](client-compatibility.md#operational-limits) for deployment constraints.

### Test results

[scripts/ci.py](../scripts/ci.py) writes logs, environment/result JSON and measurements to ignored `build-results/`, setting `KAFERRY_REPORT_DIR`. Direct load/soak commands need that variable to save metrics.

Soak defaults to two minutes locally. `KAFERRY_SOAK_DURATION` overrides it (minimum one minute).

```sh
make broker
make build
python3 scripts/ci.py load make load
KAFERRY_SOAK_DURATION=30m python3 scripts/ci.py soak make soak
python3 scripts/ci.py summary
make broker-down
```

To include destination stalls, after `make broker`, use its separate destination cluster:

```sh
KAFKA_TEST_DEST_BROKERS=127.0.0.1:40092 \
KAFERRY_SOAK_DEST_STALL_CONTAINER=kaferry-compat-kafka-dr-1 \
KAFERRY_SOAK_DURATION=30m \
python3 scripts/ci.py soak make soak
```

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) runs on main pushes, PRs and manual dispatch. Bounded load measurements appear in the summary and `checks-kafka` artifact: throughput, sampled RSS, work caps and logs. Summaries list the slowest tests. [Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) run nightly unless a successful scheduled run already tested the same main commit; the weekly full run always includes a 30-minute soak. Manual dispatch runs all suites with a configurable soak duration (keep it below the 40-minute test timeout). Artifacts retain evidence for 30 days; each test invocation runs once and failures stay visible in Actions.

PRs also run quality/security checks and [additional integrity suites](../.github/workflows/risk.yml). Only the prose allowlist skips fault and security/failover tests; unknown paths run both. The ordinary PR suites always run. Strict branch protection applies to admins; CodeQL errors and High/Critical security findings block merging.

Use targeted package tests during local edits; full local suites are optional when CI will repeat them. Keep real-Kafka tests for broker behavior; move tests only after comparing executed names and the combined coverage on the same revision.

`python3 scripts/ci_latency.py` reports recent CI wall time (including queues, excluding local review/description updates) and runner minutes. Percentiles need 20 complete heads without reruns or cancellations; both are counted separately. A six-minute PR p95 is a target, not a gate. Track reruns, escaped regressions and local review time when investigating delays; tighten checks around actual failures before adding broad suites.

After retargeting a PR to a different base branch, push a new commit so CI compares the new base and head.

### Quality indicators

| Indicator | Where / meaning |
| --- | --- |
| CI | README badge; formatting and documentation checks, vet, race-enabled tests, build, license-notice validation and broker integration |
| Go coverage | README badge and Codecov PR/file view show line coverage; Actions summarizes weighted Go statement coverage per suite. All Go packages are instrumented; Codecov combines unit/mock/file-component and broker integration coverage |
| Static analysis | [Quality workflow](https://github.com/pagrus7/kaferry/actions/workflows/quality.yml): Staticcheck and CodeQL for Go, Python and workflows |
| Security | govulncheck fails on reachable Go advisories; CodeQL findings and Dependabot alerts appear in GitHub Security. Dependency review checks PRs; updates require review |
| Complexity / duplication | Quality summary/artifact: gocyclo maximum/top ten, exact clones ≥100 tokens and ≥10 lines. Identical tools scan the PR base; no arbitrary score gates |
| Extended tests | README badge; load, fault, ACL/failover, fuzz, codec benchmarks and a 30-minute soak with destination stalls. Soak duration is configurable on manual dispatch |
| Performance | Dated SHA/environment JSON and summaries: throughput, sampled RSS/growth, lag, caps, shutdown, ns/op and allocations. Prior successful measurements compare only matching settings; shared runners provide no regression gate |

Missing measurements are unavailable. Coverage and vulnerability scans describe Go code; badges report only the listed checks.

### Paired performance

[Performance runs](../.github/workflows/performance.yml) are separate from required PR checks. They reuse the Kafka default-copy and disabled-metrics file-copy assertions, with larger inputs, a warm-up per binary and six interleaved pairs, alternating which binary runs first on one runner. Summaries show median head/base throughput and the pair range; raw metrics, binary/toolchain identities and broker images stay in the `performance` artifact for 30 days. Kafka input is capped at 160,000 records; file input at 200,000 records.

Nightly runs start with A/A calibration (the same binary on both sides). Review at least 20 calibration runs before setting the repository variable `PERFORMANCE_MODE=comparison`; this enables changed-main comparisons against the last retained compatible scheduled measurement. No compatible baseline starts another calibration. Expired data requires a fresh baseline. Manual dispatch accepts a base revision; adding `ci:performance` opts a PR into the configured mode. In comparison mode, PRs compare against the merge base. No performance threshold blocks merging; choose warnings only after measuring A/A noise.

### Local reviews

Each PR needs independent **docs**, **tests** and **simplicity** reviews. [AGENTS.md](../AGENTS.md) defines when to run them and what to record; [review/](../review/README.md) holds the shared workflow and criteria.

Fetch the intended base from the repository root before starting:

```sh
git fetch origin main
```

Reviewers inspect files without running tests. The tests reviewer reads GitHub CI and downloads coverage into an ignored evidence cache; source edits remain forbidden. The client needs GitHub read access and cache-write permission. Blocked collection leaves coverage review pending. Run tests separately. Calls count against the active client's subscription limits.

**Codex:** use its ChatGPT login (`codex login`). The [native agent definitions](../.codex/agents) use your Codex model settings. [Codex agents](https://learn.chatgpt.com/docs/agent-configuration/subagents).

```sh
codex login status
```

After confirming ChatGPT login, from the repository root:

```sh
codex exec 'Follow review/README.md; run all three reviewers against origin/main.'
```

The command uses your configured [permissions](https://learn.chatgpt.com/docs/sandboxing); forced read-only mode can block evidence downloads. For an interactive review, ask your existing Codex session to follow `review/README.md` for your base.

**Claude Code:** use subscription login (`claude auth login --claudeai`). Remove API credentials so they do not override it. [Authentication](https://code.claude.com/docs/en/authentication).

```sh
unset ANTHROPIC_API_KEY ANTHROPIC_AUTH_TOKEN
claude auth status
```

Continue only if the auth method is `claude.ai`:

```sh
CLAUDE_CODE_DISABLE_BACKGROUND_TASKS=1 \
  claude -p '/review-local'
```

The command waits for all three reports. In an existing Claude Code session, use `/review-local [base]`. The [agent settings](../.claude/agents) select Opus 5.5 with high effort; your account needs access to that model. The [command adapter](../.claude/skills/review-local/SKILL.md) loads the shared workflow.

### Release evidence

Release archives contain the binary, MIT license and notices for linked Go modules, subpackage licenses and the Go runtime. `--version` reports the build version, commit, Go and franz-go version. Production builds use `CGO_ENABLED=0`; broker fixtures also use franz-go; Linux race checks require CGO and a C compiler.

The workflow builds Linux AMD64/ARM64 static binaries and macOS AMD64/ARM64 binaries, then produces per-platform SPDX SBOMs, archive provenance and binary SBOM attestations. Each extracted archive passes version and file-copy tests. Linux Kafka, fault, load, security/failover and a 30-minute soak must pass before publishing. CLI tests use the extracted binary and verify its digest; component and in-process application tests execute checkout code. [Notices generation](../scripts/notices.py) inspects actual linked packages and fails if required licenses cannot be found.

For downloads with attestation bundles, verify archive provenance with `gh attestation verify kaferry-VERSION-PLATFORM.tar.gz --repo pagrus7/kaferry`, and the extracted binary's SBOM with `gh attestation verify kaferry --repo pagrus7/kaferry --predicate-type https://spdx.dev/Document/v2.3`. Attestations establish build origin. Downloads without these bundles can be checked against `SHA256SUMS` for file integrity.

Manual dispatch defaults to validating builds without publishing. Publishing requires a `v*` tag; the workflow checks that the built commit matches the tag. [Release procedure and note format](../releases/README.md).
