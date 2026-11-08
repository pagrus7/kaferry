# Component design

Contributor reference for boundaries, ownership and checks. See [copy behavior](decisions.md) and [syntax](grammar.md).

## Composition

Constructors receive dependencies through narrow interfaces. [Fx modules](../../src/internal/app/modules.go) compose the application; domain packages do not depend on Fx. Records, cursors and pure helper functions use concrete types.

Command parses, configures and plans; Engine acquires adapters; Runner copies through shared source, sink and progress contracts. The application maps worker configuration to `copy.Settings`; the runner consumes only its own limits and standard durations. Plan defines which completion rules are enabled and whether saved stops depend on timestamp assumptions. Runner and archive replay apply those rules to resolved partition windows; inspection uses them for reports.

Explain and status use the same planning stages and read-only metadata/archive interfaces; explain also uses the Kafka positioner. Text and JSON render the same report; inspection never acquires execution adapters or locks.

SQL syntax and validation errors share `sql.Diagnostic`, which carries a source position and preserves its cause. AST positions stay out of normalized plans, fingerprints and checkpoints. The CLI adds SQL file/stdin names; errors report token categories and fixed syntax hints without echoing input values or SQL excerpts. Caller-created ASTs without positions produce plain messages; configuration and operational errors keep their own context.

## Components

| Component | Package | Responsibility | Dependency boundary |
| --- | --- | --- | --- |
| SQL parser | [sql](../../src/internal/sql) | Tokenize/parse; report input positions | `sql.Parser` |
| Configuration | [config](../../src/internal/config) | Read TOML and defaults; decode worker durations | `config.Reader`, `FileReader` |
| Planner | [plan](../../src/internal/plan) | Validate, merge connections, compile time bounds and resolve environment values | [Builder, Environment and Plan](../../src/internal/plan/types.go) |
| Command | [app](../../src/internal/app) | Order planning stages; classify failures | Parser, Reader, Builder, Engine |
| Engine | [app](../../src/internal/app) | Validate endpoints/progress; acquire and close query resources | KafkaFactory, ArchiveFactory, RunnerFactory |
| Copy inspection | [inspect](../../src/internal/inspect) | Preview endpoints, progress and starting positions without writes | KafkaFactory, Archives, PositionerFactory |
| Archive inspection | [archive/io/inspection.go](../../src/internal/archive/io/inspection.go) | Read archive headers, sizes and replay checkpoints without locks or recovery | inspect.Archives |
| Runner | [copy](../../src/internal/copy) | Consume, filter, submit, process outcomes/lifecycle events | [Source, Sink, ProgressStore and Clock](../../src/internal/copy/ports.go); [ProgressTracker](../../src/internal/copy/progress.go) |
| Partition completion | [copy/partition.go](../../src/internal/copy/partition.go) | Decide timestamp stops, tip sampling and waits for a fixed sampled end | Pure assignment state; runner performs I/O and logs outcomes |
| Flow control | [copy/flow.go](../../src/internal/copy/flow.go) | Hold input and its EOF before tracking; decide admission, capacity pauses and commit cadence | Narrow work-count port; runner pauses full partitions and checkpoints at a record/byte cap, after one second or when the next input cannot fit |
| Progress tracker | [copy/progress.go](../../src/internal/copy/progress.go) | Track consumed/resolved prefixes, ownership and caps | ProgressTracker, fresh instance through ProgressFactory |
| Kafka metadata | [kafka/franz](../../src/internal/kafka/franz) | Inspect identities, offsets and optional destination config | sourceMetadata, destinationMetadata, MetadataClient |
| Kafka contracts | [kafka](../../src/internal/kafka) | Endpoint interfaces and saved-progress identity checks | Connection, copy.ProgressStore; client libraries stay in adapters |
| Commit metadata codec | [kafka/commitmeta](../../src/internal/kafka/commitmeta) | Encode/decode the versioned commit format, without Kafka I/O | `commitmeta.Codec`; private wire types, independent golden/invalid-input tests |
| Kafka offset lookup | [kafka/offset_lookup.go](../../src/internal/kafka/offset_lookup.go) | Batch named boundary/timestamp queries and reject incomplete results | OffsetLookupFactory through Fx; connection-bound raw OffsetReader and TimeReader |
| Kafka positioning | [kafka](../../src/internal/kafka) | Resolve saved/start positions and windows; batch lookups | PositionerFactory, AssignmentPositioner, OffsetLookupFactory, OffsetLookup; raw offset/time ports and client conversions in franz |
| Kafka input | [kafka/franz](../../src/internal/kafka/franz) | Hand off callbacks/fetches to the core loop; position, pause and prove gaps | ConsumerClient, sourceMetadata, gapReader, rebalanceBridge, PartitionValidator |
| Kafka output | [kafka/franz](../../src/internal/kafka/franz) | Validate records, bound admission and report asynchronous deliveries | ProducerClient, destinationMetadata, Clock |
| Kafka commits | [kafka/franz](../../src/internal/kafka/franz) | Save positions and eligible stops; reject commits without a group member ID and generation | offsetCommitter, copy.ProgressStore; committer owns assignment metadata |
| Observability | [observability](../../src/internal/observability) | Export immutable copy snapshots and actual save results through OTLP/HTTP; bound cleanup and redact diagnostics | `copy.Observer`, per-query Session/Factory, environment/file ports; SDK stays in adapter |
| Shutdown budget | [app/shutdown.go](../../src/internal/app/shutdown.go) | Enforce whole-process stop deadline; identify the blocked operation | StopObserver, Shutdown; CLI alone owns termination |
| Archive adapters | [archive/io](../../src/internal/archive/io) | Convert full provenance, batch confirmations, save replay stops and publish final files | Input, Output, Capture, Replay; format libraries stay outside copy |
| Avro codec | [archive/avro](../../src/internal/archive/avro) | Preserve raw records and source identity in bounded compressed blocks | Standard I/O ports; fixed schema and independent Avro interoperability tests |
| Archive capture | [archive/capture](../../src/internal/archive/capture) | Route records and rotate by size, age, rewind and file budget | Segments, Clock; no Kafka or copy-loop state |
| Archive storage | [archive/capture](../../src/internal/archive/capture) | Lock writer namespaces, save durable boundaries, publish and recover files | Filesystem/DataFile, StateStore; Journal uses AtomicWriter for synced replacement |
| Archive replay | [archive/replay](../../src/internal/archive/replay) | Pin completed inputs, merge source offsets and save confirmed physical cursors | Snapshot, Blocks, Checkpoints; bounded Pool and read-only Catalog |
| Checkpoints | [checkpoint](../../src/internal/checkpoint) | Read bounded regular files, derive safe group filenames, encode canonical JSON with checksums and replace files durably | ReadFile, AtomicWriter, private filesystem port; owners open/close files and validate their state |
| CLI | [cmd](../../src/cmd/kaferry), [app](../../src/internal/app) | Parse flags, compose components and handle signals | Command/Engine; Fx at composition only |

## Starting a component change

Start with the affected contract and its focused tests below. Follow callers and adapters when changing ownership, ordering, errors or persisted identity. Review the full patch; these entry points guide investigation, not review exclusions.

| Change | Contract and implementation | Focused evidence |
| --- | --- | --- |
| Source/sink behavior | [Core ports](../../src/internal/copy/ports.go), [record and event types](../../src/internal/copy/record.go) | [Rejection and asynchronous errors](../../src/internal/copy/policy_test.go), [draining and save failures](../../src/internal/copy/runner_boundary_test.go) |
| Admission and held input | [Flow](../../src/internal/copy/flow.go); runner owns submission retries after tracking | [Capacity](../../src/internal/copy/flow_test.go), [revocation and queue deadlines](../../src/internal/copy/runner_regression_test.go), [input/EOF ordering](../../src/internal/copy/policy_test.go) |
| Confirmed and saved positions | [Tracker contract](../../src/internal/copy/progress.go), [implementation](../../src/internal/copy/tracker.go) | [Gaps and stale deliveries](../../src/internal/copy/progress_test.go), [stop persistence](../../src/internal/copy/stop_test.go) |
| Kafka commit ownership | [Committer](../../src/internal/kafka/franz/commit.go), [assignment sequence](../../src/internal/kafka/franz/source_lifecycle.go) | [Commit snapshots and errors](../../src/internal/kafka/franz/commit_test.go), [assignment and revocation](../../src/internal/kafka/franz/source_test.go) |
| Plan inputs and identity | [Plan types](../../src/internal/plan/types.go), [identity contract](decisions.md#identity) | [Locations and redaction](../../src/internal/plan/diagnostic_test.go), [fingerprint compatibility](../../src/internal/plan/fingerprint_test.go) |
| Replay resource limits | [Pool](../../src/internal/archive/replay/pool.go), [reader](../../src/internal/archive/replay/reader.go) | [Bounded resources and restart](../../src/internal/archive/replay/files_test.go), [ordering and cleanup](../../src/internal/archive/replay/order_test.go) |

## Ownership

- Factories create fresh mutable runner/progress/adapter state for each query; assignment state is generation-specific.
- The engine manages each query's resources. Adapters manage Kafka clients, files and locks.
- [Archive capture](decisions.md#files) defines validation, output ownership and progress-save order. Adapters close after the runner saves confirmed progress; Kafka source close performs no saves.
- Runner mutates progress in one event loop. Late outcomes from an old assignment cannot advance current progress.
- Sources provide owned record buffers; sinks retain them until the corresponding write outcome.
- Source cursors are distinct from record provenance. Preserved Kafka routing uses provenance; archive replay cursors use one virtual partition. The engine checks layouts before progress saves; Kafka assignments recheck through `PartitionValidator`. Replay validates its fixed selection before its initial checkpoint. Progress persists confirmed next positions and identity metadata.
- Output confirmation precedes progress persistence: Kafka delivery or file fsync. No component saves past unresolved work.

## Conventions

Implementation is AI-driven with Codex and Claude Code under human direction.

- Priorities: correctness, simple design, throughput, CI duration, binary size.
- Single-purpose structs behind narrow interfaces; keep core logic Fx-free. Keep functions focused and control flow readable; lint thresholds are defined in [.golangci.yml](../../.golangci.yml).
- Follow the [documentation guidance](../../CONTRIBUTING.md#documentation) for user-facing changes.

Testing commands and evidence are in the [contributor guide](../../CONTRIBUTING.md#tests).

The shared Go setup caches modules and compiled packages by runner architecture, Go version, workload, `go.sum` and commit, restoring the newest matching workload entry. Main pushes and manual checks compile the binary and broker fixtures into the shared `pr-brokers` cache used by integrity, compatibility and parallel-lab jobs. The compile steps match each workload's tags, race instrumentation and CGO setting so a new PR can restore main's compilation results. Successful jobs refresh their cache for later pushes.

PR and main checks run `TestKafkaQuickStart` in the load job after bounded measurements finish, using the built binary and a fresh broker on port 9092. Both the integration job and the load job verify their required tests ran. The shared integration suite also enables and verifies the quick start for releases; ordinary local integration runs skip it unless `KAFERRY_QUICKSTART=1` is set.

### Local reviews

Follow [docs/development/reviews/](../development/reviews/README.md) for required reviewers, formatting-only exceptions and follow-ups.

Fetch the intended base from the repository root before starting:

```sh
git fetch origin main
```

Follow the [reviewer contract](../development/reviews/README.md#reviewer-contract). The client needs GitHub read access and cache-write permission for evidence collection. Calls count against the active client's subscription limits.

**Codex:** use its ChatGPT login (`codex login`). The [native agent definitions](../../.codex/agents) use your Codex model settings. [Codex agents](https://learn.chatgpt.com/docs/agent-configuration/subagents).

```sh
codex login status
```

After confirming ChatGPT login, from the repository root:

```sh
codex exec 'Follow docs/development/reviews/README.md against origin/main.'
```

The command uses your configured [permissions](https://learn.chatgpt.com/docs/sandboxing); forced read-only mode can block evidence downloads. For an interactive review, ask your existing Codex session to follow `docs/development/reviews/README.md` for your base.

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

The command waits for the required reports. In an existing Claude Code session, use `/review-local [base]`. The [agent settings](../../.claude/agents) select Opus 5.5 with high effort; your account needs access to that model. The [command adapter](../../.claude/skills/review-local/SKILL.md) loads the shared workflow.

### Release evidence

Follow the [release procedure](../releases/README.md#build-and-validation) for build and validation requirements, and [installation verification](../install.md#release-binaries) for downloaded binaries.

Linker values take precedence over embedded Go build information. Otherwise, the CLI and metrics use the embedded module version, including pseudo-versions and dirty markers; an absent or `(devel)` version reports `dev`. Commit information comes from embedded VCS metadata, with `-dirty` for a modified checkout, or reports `unknown` when unavailable. Version reporting does not query Git or the network.
