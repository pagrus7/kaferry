# Component design

Contributor reference for boundaries, ownership and checks. See [copy behavior](decisions.md) and [syntax](grammar.md).

## Composition

Constructors receive dependencies through narrow interfaces. [Fx modules](../internal/app/modules.go) compose the application; domain packages do not depend on Fx. Passive records, cursors and pure helpers stay concrete.

Command parses, configures and plans; Engine acquires adapters; Runner copies through shared source, sink and progress contracts.

## Components

| Component | Package | Responsibility | Dependency boundary |
| --- | --- | --- | --- |
| SQL parser | [sql](../internal/sql) | Tokenize/parse; report input positions | `sql.Parser` |
| Configuration | [config](../internal/config) | Read TOML and defaults | `config.Reader`, `FileReader` |
| Planner | [plan](../internal/plan) | Validate, merge connections, compile time bounds and resolve environment values | `plan.Builder`, `Environment` |
| Command | [app](../internal/app) | Order planning stages; classify failures | Parser, Reader, Builder, Engine |
| Engine | [app](../internal/app) | Validate endpoints/progress; acquire and close query resources | KafkaFactory, FileFactory, RunnerFactory |
| Runner | [copy](../internal/copy) | Consume, filter, submit, process outcomes/lifecycle events | Source, Sink, ProgressStore, ProgressTracker, Clock |
| Progress tracker | [copy](../internal/copy) | Track consumed/resolved prefixes, ownership and caps | ProgressTracker, fresh instance through ProgressFactory |
| Kafka metadata | [kafka](../internal/kafka) | Inspect topic/group identities, offsets and optional destination config | SourceMetadata, DestinationMetadata, AdminClient |
| Kafka positioning | [kafka](../internal/kafka) | Resolve saved/start positions and windows; batch lookups | PositionerFactory, AssignmentPositioner, OffsetReader, TimeReader |
| Kafka adapters | [kafka](../internal/kafka) | Poll/assign/pause, produce/deliver, commit progress | ConsumerClient, ProducerClient, AssignmentStore |
| File adapters | [file](../internal/file) | Encode/decode frames, snapshot input, recover output, persist checkpoints | ArchiveDecoder, ArchiveEncoder, CheckpointStore, I/O ports |
| File publication | [file](../internal/file) | Publish a durable header without replacing another creator | Publisher, private filesystem port |
| CLI | [cmd](../cmd/kaferry), [app](../internal/app) | Parse flags, compose components and handle signals | Command/Engine; Fx at composition only |

## Ownership

- Factories create fresh mutable runner/progress/adapter state for each query; assignment state is generation-specific.
- Engine owns query resources and closes them in reverse acquisition order. Adapters own their native clients, files and locks.
- Runner mutates progress in one event loop. Late outcomes from an old assignment cannot advance current progress.
- Sources provide owned record buffers; sinks retain them until the corresponding write outcome.
- Source cursors are distinct from record provenance. Progress persists confirmed next positions and identity metadata.
- Output confirmation precedes progress persistence: Kafka delivery or file fsync. No component saves past unresolved work.

## Conventions

- Priorities: correctness, simplicity, throughput, CI duration, binary size.
- Single-purpose structs behind narrow interfaces; keep core logic Fx-free. Lines of code and cyclomatic complexity are signals, not targets.
- Keep user documentation example-based. CI enforces fewer than 10,000 characters across [README](../README.md) and [file format](../docs/file-format.md). Grammar tests parse and plan exactly five fenced SQL examples; invalid examples use inline code.

## Tests

Most tests exercise pure functions or individual components with handwritten mocks. Native-client and filesystem failures use injected ports; file-component tests use real files, and app tests replace Fx factories. Fewer [broker/CLI tests](../tests/compat) cover complete copy paths, rebalances and restart. Functional tests use Given/When/Then phases.

### Test targets

Run from the repository root. [Makefile](../Makefile) defines these targets; Kafka fixtures need Docker and Compose.

| Command | Scope / prerequisite |
| --- | --- |
| `make check` | Vet and race-enabled unit/mock/component tests, including native subprocess regressions; no external broker |
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

Soak defaults to two minutes locally; `KAFERRY_SOAK_DURATION` overrides it (minimum one minute). The extended-test workflow defaults to 30 minutes, configurable on manual dispatch, and stalls the destination. Plaintext fixtures warm the group coordinator with the Java CLI; TLS fixtures check readiness with consumer `Committed`. Broker integration therefore does not cover cold coordinator startup; isolated mock-cluster subprocesses test coordinator outages. [Native limits](client-compatibility.md#known-issues).

### Test results

[scripts/ci.py](../scripts/ci.py) writes logs, environment/result JSON and measurements to ignored `build-results/`, setting `KAFERRY_REPORT_DIR`. Direct load/soak commands need that variable to save metrics.

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

[CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) runs on main pushes, PRs and manual dispatch. [Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) run load, soak, fault, security/failover, fuzz and benchmarks weekly and on manual dispatch. Artifacts retain logs and measurements for 30 days.

### Quality indicators

| Indicator | Where / meaning |
| --- | --- |
| CI | README badge; vet, race-enabled tests, build and broker integration |
| Go coverage | README badge and Codecov PR/file view show line coverage; Actions summarizes weighted Go statement coverage per suite. All Go packages are instrumented; Codecov combines unit/mock/file-component and broker integration coverage. Native C/C++ is excluded |
| Static analysis | [Quality workflow](https://github.com/pagrus7/kaferry/actions/workflows/quality.yml): Staticcheck and CodeQL for Go, Python and workflows |
| Security | govulncheck fails on reachable Go advisories; CodeQL findings and Dependabot alerts appear in GitHub Security. Dependency review checks PRs; updates require review |
| Complexity / duplication | Quality summary/artifact: gocyclo maximum/top ten, exact clones ≥100 tokens and ≥10 lines. Identical tools scan the PR base; no arbitrary score gates |
| Extended tests | README badge; weekly default 30-minute soak plus load, fault, ACL/failover, fuzz and CBOR benchmarks |
| Performance | Dated SHA/environment JSON and summaries: throughput, sampled RSS/growth, lag, caps, shutdown, ns/op and allocations. Prior successful measurements compare only matching settings; shared runners provide no regression gate |

Missing measurements are unavailable. Coverage and Go vulnerability scans exclude native C/C++; badges report only the listed checks.

### Release evidence

The release workflow produces per-platform SPDX SBOMs, native inventory/license text, archive provenance and binary SBOM attestations. Syft inventories the binary; the native supplement identifies the selected librdkafka archive and observed version strings. Bundled licenses do not prove every component was linked; native vulnerability coverage is incomplete.

For releases built by the current workflow, verify archive provenance with `gh attestation verify kaferry-VERSION-PLATFORM.tar.gz --repo pagrus7/kaferry`, and the extracted binary's SBOM with `gh attestation verify kaferry --repo pagrus7/kaferry --predicate-type https://spdx.dev/Document`. Attestations establish build origin. Releases through v0.2.0 have checksums/build information but lack these SBOMs and attestations.

Pushing a `v*` tag publishes after successful builds. Manual dispatch defaults to validation only; publishing requires selecting the version tag as the workflow ref. Checkout must match the attested workflow SHA. [Release procedure and note format](../releases/README.md).
