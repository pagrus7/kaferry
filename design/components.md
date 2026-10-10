# Component design

Contributor guide to responsibilities, interfaces, resource ownership and testing. Query behavior is specified in [decisions](decisions.md); command syntax in [grammar](grammar.md).

## Composition

Constructors receive dependencies through narrow interfaces. [Fx modules](../internal/app/modules.go) compose the application; domain packages do not depend on Fx. Passive records, cursors and pure helpers stay concrete.

Command parses, loads configuration and builds a plan before calling Engine. Engine obtains per-query adapters from factories and supplies them to Runner. Kafka and file adapters implement the same source, sink and progress contracts.

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
- Keep user documentation example-based. CI enforces fewer than 10,000 characters across [README](../README.md) and [file format](../docs/file-format.md).

## Tests

- Most tests exercise pure functions or one component with handwritten dependency mocks; native clients and filesystem failures are injected through their ports.
- Component tests use real files and replace Fx factories to verify composition, ownership and cleanup.
- Fewer [broker/CLI tests](../tests/compat) verify complete copy paths, rebalances and restart. Functional tests use Given/When/Then phases; load and targeted edge cases run at the cheapest useful layer.

### Test targets

Run from the repository root. [Makefile](../Makefile) defines these targets; Kafka fixtures need Docker and Compose.

| Command | Scope / prerequisite |
| --- | --- |
| `make check` | Vet and race-enabled unit/mock/file-component tests; no broker |
| `make broker` | Start disposable source/destination Kafka clusters |
| `make integration` | Broker/CLI paths, fidelity, resume and rebalances; run `make broker` first |
| `make fault` | Outages and ownership loss; run `make broker` first |
| `make load` | Bounded Kafka-copy throughput and caps; run `make broker` first |
| `make soak` | Streaming fidelity, lag, memory and shutdown; run `make broker` first |
| `make qualification` | Security/ACLs and replicated failover; starts its own TLS/SASL fixture |
| `make fuzz` | Parser, time bounds, frames and record decoding |
| `make benchmark` | CBOR framing CPU/allocation measurements |
| `make broker-down` | Stop the disposable source/destination clusters |

Soak defaults to two minutes locally; `KAFERRY_SOAK_DURATION` overrides it (minimum one minute). The qualification workflow defaults to 30 minutes (overridable on manual dispatch) and also stalls the destination. Fixture coordinator warmup is not a [production remedy](client-compatibility.md#known-defects-in-v010).

### Test results

[scripts/ci.py](../scripts/ci.py) captures command logs, environment/result JSON and load/soak metrics in ignored `build-results/`. It sets `KAFERRY_REPORT_DIR`; bare `make load`/`make soak` write no metric files unless that variable is set.

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

[CI checks](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml) run on pushes to main, pull requests and manual dispatch. [Qualification](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml) runs load, soak, fault, security/failover, fuzz and benchmark suites weekly and on manual dispatch. Both retain build artifacts for 30 days; measurements belong there, not in tracked status documents.
