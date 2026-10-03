# Implementation plan

Stages for building `decisions.md` and `grammar.md`; semantics live there, not here.

## Structure

Single-purpose structs, narrow interfaces at replaceable boundaries, constructor injection. Fx wires subsystems at the composition root only. No interfaces for passive data, no plugin framework.

| Component | Responsibility | Boundary |
| --- | --- | --- |
| SQL | Tokenize and parse; report input positions | Parser returning an AST |
| Configuration | Load TOML, merge inline properties, resolve `ENV`, redact secrets | Loader, environment lookup |
| Planner | Validate semantics, normalize the window, produce an immutable plan | Planner, injected clock |
| Runner | Consume, filter, submit bounded batches, handle lifecycle events | Source, sink, progress store |
| Progress | Ordered consumed positions, per-record outcomes, ownership, caps | Tracker, free of Kafka types |
| Kafka adapters | Poll/assign/pause/commit, produce, delivery outcomes, metadata | Source, sink, commit adapters |
| File adapters | Snapshot input, durable output, framing, checkpoints, locks | Source, sink, codec, checkpoint |
| CLI/composition | Flags, adapter selection, Fx wiring, signals, exit codes | Start, run, stop |

Packages: `cmd/kafka-fwd2`, `internal/sql`, `internal/config`, `internal/plan`, `internal/copy`, `internal/kafka`, `internal/file`, `internal/app`. Split further only when responsibilities or dependencies justify it.

## Core contracts

- A record owns key/value bytes, ordered headers, timestamp/type, and optional provenance; null versus empty survives every adapter.
- The source cursor (Kafka partition/offset or file frame position) is separate from provenance.
- Sources emit records and assignment/revocation events; pausing never stops the Kafka poll loop from servicing events.
- Sinks report one result per submitted record; buffers are not reused while writes are outstanding.
- Progress stores persist confirmed next positions plus identity metadata: Kafka commits and atomic file checkpoints sit behind one boundary.
- Filtered and skipped records count as resolved; written records resolve on delivery success or fsync. Never commit past an unresolved consumed position.
- Caps cover all uncommitted work, including confirmed records awaiting commit; client queues are bounded separately.
- Outcomes are fenced by assignment generation so late callbacks cannot advance progress. Mutable progress state lives in one event loop.

## 1. Command planning

- Recursive-descent parser for the full grammar; TOML loader; `ENV` resolution; alias resolution including `default`.
- Planner rules from `grammar.md`; window normalization to `[lo, hi)`; normalized `WHERE` form for the fingerprint (cluster IDs are added at startup).

Gate: table-driven tests from every `grammar.md` example, boundary-time tests, parser fuzzing. No broker.

## 2. Delivery and progress core

Runner against fake sources, sinks, clocks, and progress stores.

- Distinct submission, confirmation, and durable-progress states; bounded batches; per-record results; filtering; skipping; caps with pause/resume.
- Coalesced per-worker commits (one in flight, 1 s or 50% of a worker cap) with per-partition release.
- Per-partition cutoff stop, tip completion, and trigger-record commit position.
- Shutdown and revocation drains (10 s), revocation timeout as worker failure, idle exit in bounded runs, rebalance-empty assignment not treated as completion.

Gate: deterministic state-machine tests for out-of-order acknowledgments, partial batch failure, offset gaps, failed and partially failed commits, every cap, shutdown, revocation, and late outcomes. Durable progress never passes unresolved work.

## 3. File adapters and file→file

- Header and frame codec with bounded allocation; full record-map fidelity.
- Source snapshot at open, partial-frame end, change detection, and `ON_SOURCE_FILE_CHANGED`.
- Output append in submission order, fsync per confirmed batch, directory fsyncs, tail recovery, header identity checks on resume.
- Checkpoints: safe names, exclusive locks, atomic replacement, stored identities, `ONE_OFF` removal on success. Reject self-copies including path aliases.
- Crash ordering between output and checkpoint may replay but never lose acknowledged records.

Gate: component tests on real temporary files with injected write/fsync/rename failures; subprocess crash/resume; codec fuzzing; lock conflicts, growing sources, changed sources, corruption, oversized frames.

## 4. Kafka adapters

- One poll loop per worker, read-committed isolation, explicit offsets, generation-aware assignment handling.
- Startup metadata: cluster IDs (fingerprint, self-copy), destination existence, timestamp type, cleanup policy, retention.
- Commit metadata: fingerprint checks, foreign-commit rejection, partition-0 window initialization and barrier, missing-anchor failure.
- Start positions via commits, timestamp lookup, or `START`; tip detection under read-committed, verified around open transactions.
- One idempotent producer per worker; `murmur2_random` default; protected settings; queue-full handling that preserves order; delivery timeout; retention-risk checks per batch.
- Map timestamp, transient, and fatal failures to the error policies.

Gate: mocked-client adapter tests plus disposable-broker component tests: null/empty fidelity, timestamp types, delivery errors, Java-compatible routing, partition-count changes, read-committed behavior, generation fencing of the partition-0 initialization commit.

## 5. End-to-end verification

- All four directions with byte-level comparison, allowing only documented timestamp exceptions.
- Multiple workers: reassignment, crash, completion, more workers than partitions, concurrent `NOW()` startup reaching one window.
- Kill between durable output and commit: duplicates only, no loss; per-partition order outside the documented replay exceptions.
- Out-of-order, boundary, future, and missing timestamps; retention risk; cutoff-trigger replay; new source partitions; absent, expired, and foreign commits; empty topics; stalled transactions; outages; commit failures at full caps; fingerprint mismatch; every exit code.

Gate: scenarios prove the recorded semantics using controlled fixtures rather than timing sleeps.

## 6. Packaging and measurement

- Example-based user documentation under 10,000 characters, covering duplicates, ordering limits, retention risk, routing, schema compatibility, file recovery, and errors.
- Redacted logs: group, resolved window, skip positions and counts, retention-risk counts, retries, commits, partition changes, terminal causes. Basic counters and gauges, no observability framework.
- Reproducible builds with documented Go/CGO/librdkafka requirements for verified targets.
- Race and static checks, dependency review, malformed-input fuzzing.
- Load tests over key/header/payload mixes, partition counts, and slow sinks: throughput, CPU, peak memory, commit lag, duplicate exposure, shutdown time. Set cap defaults from these results.
- Fast unit/mock tests on every change; component/e2e CI jobs; scheduled load and fault runs.

Gate: reproducible release candidate, documented operating limits, bounded memory under load, no unexplained loss or premature commit in fault runs.

## Out of scope

Payload decoding or transformation, Schema Registry remapping, topic auto-creation, coordinator or done-marker service, segmented files, viewer, general SQL engine, throughput targets before a baseline.
