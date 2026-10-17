# Component design

Contributor reference for boundaries, ownership and checks. See [copy behavior](decisions.md) and [syntax](grammar.md).

## Composition

Constructors receive dependencies through narrow interfaces. [Fx modules](../internal/app/modules.go) compose the application; domain packages do not depend on Fx. Records, cursors and pure helper functions use concrete types.

Command parses, configures and plans; Engine acquires adapters; Runner copies through shared source, sink and progress contracts.

SQL syntax and validation errors share `sql.Diagnostic`, which carries a source position and preserves its cause. AST positions stay out of normalized plans, fingerprints and checkpoints. The CLI adds SQL file/stdin names; errors report token categories and fixed syntax hints without echoing input values or SQL excerpts. Caller-created ASTs without positions produce plain messages; configuration and operational errors keep their own context.

## Components

| Component | Package | Responsibility | Dependency boundary |
| --- | --- | --- | --- |
| SQL parser | [sql](../internal/sql) | Tokenize/parse; report input positions | `sql.Parser` |
| Configuration | [config](../internal/config) | Read TOML and defaults | `config.Reader`, `FileReader` |
| Planner | [plan](../internal/plan) | Validate, merge connections, compile time bounds and resolve environment values | `plan.Builder`, `Environment` |
| Command | [app](../internal/app) | Order planning stages; classify failures | Parser, Reader, Builder, Engine |
| Engine | [app](../internal/app) | Validate endpoints/progress; acquire and close query resources | KafkaFactory, FileFactory, RunnerFactory |
| Runner | [copy](../internal/copy) | Consume, filter, submit, process outcomes/lifecycle events | Source, Sink, ProgressStore, ProgressTracker, Clock |
| Partition completion | [copy/partition.go](../internal/copy/partition.go) | Decide timestamp stops, tip sampling and waits for a fixed sampled end | Pure assignment state; runner performs I/O and logs outcomes |
| Flow control | [copy/flow.go](../internal/copy/flow.go) | Hold unsent input; decide admission, worker-wide pause and commit cadence | Narrow work-count port; runner applies partition pauses and saves progress |
| Progress tracker | [copy](../internal/copy) | Track consumed/resolved prefixes, ownership and caps | ProgressTracker, fresh instance through ProgressFactory |
| Kafka metadata | [franz](../internal/franz) | Inspect identities, offsets and optional destination config | sourceMetadata, destinationMetadata, MetadataClient |
| Kafka contracts and progress format | [kafkaport](../internal/kafkaport) | Endpoint interfaces and strict saved-progress encoding | Connection, SourceProgress; client libraries stay in adapters |
| Kafka positioning | [kafkaport](../internal/kafkaport) | Resolve saved/start positions and windows; batch lookups | PositionerFactory, AssignmentPositioner, OffsetReader, TimeReader; client conversions in franz |
| Kafka input | [franz](../internal/franz) | Hand off callbacks/fetches to the core loop; position, pause and prove gaps | ConsumerClient, sourceMetadata, gapReader, rebalanceBridge |
| Kafka output | [franz](../internal/franz) | Validate records, bound admission and report asynchronous deliveries | ProducerClient, destinationMetadata, Clock |
| Kafka commits | [franz](../internal/franz) | Save positions and, for file outputs, file ID and extent; reject commits without a group member ID and generation | offsetCommitter, copy.OutputProgress |
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
- Keep user documentation example-based. CI caps [README](../README.md) at 5,500 characters and enforces fewer than 10,000 across README, [usage](../docs/usage.md) and [file format](../docs/file-format.md). Grammar tests parse and plan exactly five fenced SQL examples; invalid examples use inline code.

## Tests

Most tests exercise pure functions or individual components with handwritten mocks. Kafka and filesystem failures use injected ports; file-component tests use real files, and app tests replace Fx factories. Fewer [broker/CLI tests](../tests/compat) cover complete copy paths, rebalances and restart. PR checks also build the base revision and verify Kafka progress, file checkpoints and archives in both directions. Functional tests use Given/When/Then phases.

### Test targets

Run from the repository root. [Makefile](../Makefile) defines these targets; Kafka fixtures need Docker and Compose.

| Command | Scope / prerequisite |
| --- | --- |
| `make check` | Vet and race-enabled unit/mock/component tests, including kfake protocol tests and shutdown subprocesses; no external broker |
| `make broker` | Start disposable source/destination Kafka clusters |
| `make integration` | Broker/CLI paths, fidelity, resume and rebalances; run `make broker` first |
| `make fault` | Outages and ownership loss; run `make broker` first |
| `make load` | Bounded Kafka-copy throughput and caps; run `make broker` first |
| `make soak` | Streaming fidelity, lag, memory and shutdown; run `make broker` first |
| `make qualification` | Security/ACLs and replicated failover; starts its own TLS/SASL fixture |
| `make fuzz` | SQL parser, frames and record decoding; default 100,000 iterations per target |
| `make benchmark` | CBOR framing CPU/allocation measurements |
| `make broker-down` | Stop the disposable source/destination clusters |

Time-window arithmetic, boundaries and overflow use deterministic unit tests in `make check`. Set `FUZZ_ITERATIONS` to change the per-target fuzz budget; failures remain fatal.

### Coverage limits

- Broker fixtures wait for a ready group coordinator: plaintext fixtures use the Java CLI; TLS fixtures use consumer `Committed`. Broker tests therefore do not cover cold coordinator startup.
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

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) runs on main pushes, PRs and manual dispatch. [Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) run weekly and on manual dispatch. Artifacts retain logs and measurements for 30 days.

### Quality indicators

| Indicator | Where / meaning |
| --- | --- |
| CI | README badge; formatting and documentation checks, vet, race-enabled tests, build, license-notice validation and broker integration |
| Go coverage | README badge and Codecov PR/file view show line coverage; Actions summarizes weighted Go statement coverage per suite. All Go packages are instrumented; Codecov combines unit/mock/file-component and broker integration coverage |
| Static analysis | [Quality workflow](https://github.com/pagrus7/kaferry/actions/workflows/quality.yml): Staticcheck and CodeQL for Go, Python and workflows |
| Security | govulncheck fails on reachable Go advisories; CodeQL findings and Dependabot alerts appear in GitHub Security. Dependency review checks PRs; updates require review |
| Complexity / duplication | Quality summary/artifact: gocyclo maximum/top ten, exact clones ≥100 tokens and ≥10 lines. Identical tools scan the PR base; no arbitrary score gates |
| Extended tests | README badge; load, fault, ACL/failover, fuzz, CBOR benchmarks and a 30-minute soak with destination stalls. Soak duration is configurable on manual dispatch |
| Performance | Dated SHA/environment JSON and summaries: throughput, sampled RSS/growth, lag, caps, shutdown, ns/op and allocations. Prior successful measurements compare only matching settings; shared runners provide no regression gate |

Missing measurements are unavailable. Coverage and vulnerability scans describe Go code; badges report only the listed checks.

### PR reviews

The **Docs review** and **Test review** checks run after the `checks` workflow for each PR, including forks. They use Claude Opus 5.5 at HIGH effort, with the reusable prompts in [.claude/agents](../.claude/agents). Each result names its PR commit; newer commits get new reviews.

Reports appear in the PR checks and Actions summary, with JSON/Markdown artifacts retained for 30 days. Findings are advisory. Missing credentials, provider errors and unfinished reviews fail the check. The agents inspect source and tests without executing them; CI runs tests separately.

Setup requires an `ANTHROPIC_API_KEY` repository secret and this workflow on the default branch. The normal Claude CLI login is not used. CI gives each reviewer a US$3 API budget, 20 turns and ten minutes; model usage is billed to the API account. To retry, manually run **PR reviews** in Actions with the PR number.

Automatic runs use default-branch code and prompts; manual runs use the selected workflow ref. PR source is read from snapshots with commands, hooks and MCP disabled. GitHub tokens stay out of the model process.

For local review with Claude Code, supply the diff to the same agents:

```sh
mkdir -p .cache
git diff origin/main...HEAD > .cache/pr-review.patch
claude --agent docs-reviewer -p 'Review .cache/pr-review.patch and the affected source.'
claude --agent test-reviewer -p 'Review .cache/pr-review.patch and the affected source.'
```

### Release evidence

Release archives contain the binary, MIT license and notices for linked Go modules, subpackage licenses and the Go runtime. `--version` reports the build version, commit, Go and franz-go version. Production builds use `CGO_ENABLED=0`; Confluent is linked only into broker test fixtures.

The workflow builds Linux AMD64/ARM64 static binaries and macOS AMD64/ARM64 binaries, then produces per-platform SPDX SBOMs, archive provenance and binary SBOM attestations. [Notices generation](../scripts/notices.py) inspects actual linked packages and fails if required licenses cannot be found.

For downloads with attestation bundles, verify archive provenance with `gh attestation verify kaferry-VERSION-PLATFORM.tar.gz --repo pagrus7/kaferry`, and the extracted binary's SBOM with `gh attestation verify kaferry --repo pagrus7/kaferry --predicate-type https://spdx.dev/Document`. Attestations establish build origin. Downloads without these bundles can be checked against `SHA256SUMS` for file integrity.

Manual dispatch defaults to validating builds without publishing. Publishing requires a `v*` tag; the workflow checks that the built commit matches the tag. [Release procedure and note format](../releases/README.md).
