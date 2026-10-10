# Component design

Responsibilities, interfaces and resource ownership. Query behavior is specified in [decisions](decisions.md); command syntax in [grammar](grammar.md).

## Composition

Constructors receive dependencies through narrow interfaces. [Fx modules](../internal/app/modules.go) compose the application; domain packages do not depend on Fx. Passive records, cursors and pure helpers stay concrete.

Command parses, loads configuration and builds a plan before calling Engine. Engine obtains per-query adapters from factories and supplies them to Runner. Kafka and file adapters implement the same source, sink and progress contracts.

## Components

| Component | Responsibility | Dependency boundary |
| --- | --- | --- |
| SQL parser | Tokenize/parse; report input positions | `sql.Parser` |
| Configuration | Read TOML and defaults | `config.Reader`, `FileReader` |
| Planner | Validate, merge connections, resolve environment/time expressions | `plan.Builder`, `Environment` |
| Command | Order planning stages; classify failures | Parser, Reader, Builder, Engine |
| Engine | Validate endpoints and progress; acquire and close query resources | KafkaFactory, FileFactory, RunnerFactory |
| Runner | Consume, filter, submit, process outcomes and lifecycle events | Source, Sink, ProgressStore, ProgressTracker, Clock |
| Progress tracker | Track consumed/resolved prefixes, ownership and caps | ProgressTracker, fresh instance through ProgressFactory |
| Kafka metadata | Inspect topic/group identities, offsets and optional destination configuration | SourceMetadata, DestinationMetadata, AdminClient |
| Kafka positioning | Resolve saved/start positions and windows; batch lookups | PositionerFactory, AssignmentPositioner, OffsetReader, TimeReader |
| Kafka adapters | Poll/assign/pause, produce/deliver, commit progress | ConsumerClient, ProducerClient, AssignmentStore |
| File adapters | Encode/decode frames, snapshot input, recover output, persist checkpoints | ArchiveDecoder, ArchiveEncoder, CheckpointStore, I/O ports |
| File publication | Publish a durable header without replacing another creator | Publisher, private filesystem port |
| CLI | Parse flags, compose components and handle signals | Command/Engine; Fx at composition only |

## Ownership

- Factories create fresh mutable runner/progress/adapter state for each query; assignment state is generation-specific.
- Engine owns query resources and closes them in reverse acquisition order. Adapters own their native clients, files and locks.
- Runner mutates progress in one event loop. Late outcomes from an old assignment cannot advance current progress.
- Sources provide owned record buffers; sinks retain them until the corresponding write outcome.
- Source cursors are distinct from record provenance. Progress persists confirmed next positions and identity metadata.
- Output confirmation precedes progress persistence: Kafka delivery or file fsync. No component saves past unresolved work.

## Independent tests

- Most tests exercise pure functions or one component with handwritten dependency mocks; native clients and filesystem failures are injected through their ports.
- Component tests use real files and replace Fx factories to verify composition, ownership and cleanup.
- Fewer broker/CLI tests in `tests/compat` verify complete copy paths, rebalances and restart. Functional tests use Given/When/Then phases.
- Fault, load, soak and security suites run separately through Makefile targets. Build logs and generated reports hold their results.
