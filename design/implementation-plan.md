# Implementation plan

Stages for building `decisions.md` and `grammar.md`; semantics live there, not here.

## Structure

Constructor injection; Fx wires subsystems at the composition root only. No interfaces for passive data, no plugin framework.

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
- Progress stores persist confirmed next positions plus identity metadata (window, file IDs, extent E); Kafka commits and atomic file checkpoints sit behind one boundary.
- Filtered and skipped records count as resolved; written records resolve on delivery success or fsync. Never commit past an unresolved consumed position.
- Caps cover all uncommitted work, including confirmed records awaiting commit; client queues are bounded separately.
- Outcomes are fenced by assignment generation so late callbacks cannot advance progress. Mutable progress state lives in one event loop; revocation commits confirmed progress inside the rebalance callback and releases without waiting for deliveries.

## 0. Client compatibility spike

A short spike against a disposable broker, before Stage 2, with pinned `confluent-kafka-go`/librdkafka and broker versions. It answers only the client assumptions the design rests on:

- Whether delivery reports attribute a batch rejection to one record.
- Incremental assign/unassign under cooperative-sticky; committing inside the rebalance callback, including commit timeout and ownership loss with the blocking `CommitOffsets`.
- Fetching committed offsets for partitions the worker does not own; describing group members and subscriptions before subscribing, and the permissions both need.
- Key, value, header, and timestamp fidelity through the async producer.
- `message.max.bytes` at boundary sizes: single records with key and headers, and batches, across supported compression and protocols.
- `auto.offset.reset=error` behavior on out-of-range positions.
- Timestamp lookup isolation with open and aborted transactions, including a lookup with no match while a transaction is open.
- The consumed cursor advancing past a trailing commit marker, and past a trailing aborted batch plus abort marker, with no later data; committing that cursor; a new owner resuming there.
- Source topic IDs through `AdminClient.DescribeTopics`, including the required permission.

Gate: concise recorded outcomes and minimum supported broker, librdkafka, and `confluent-kafka-go` versions in `decisions.md`; feature introduction versions are inputs, not proof. A negative or uncertain result reopens the affected rule before Stage 2: without a safe cursor at a trailing control record the tip rule reopens, and without topic IDs the recreation guard becomes a documented limitation. Not a prototype and not a substitute for later tests.

## 1. Command planning

- Recursive-descent parser for the full grammar; TOML loader; `ENV` resolution; alias resolution including `default`.
- Planner rules from `grammar.md`; window normalization to `[lo, hi)`; normalized `WHERE` form for the fingerprint (cluster IDs are added at startup).

Gate: table-driven tests from every `grammar.md` example, boundary-time tests, parser fuzzing. No broker.

## 2. Topic→topic slice

One worker, literal window, real broker: consume, filter, produce, and commit through the first Kafka adapters and core path. Later stages generalize it; it is not a throwaway prototype.

Gate: a bounded copy against a disposable broker matches byte for byte.

## 3. Delivery and progress core

Runner against fake sources, sinks, clocks, and progress stores.

- Distinct submission, confirmation, and durable-progress states; bounded batches; per-record results; filtering; skipping; caps with pause/resume.
- Coalesced per-worker commits with per-partition release.
- Per-partition stops with stop-reason logging, tip completion against a sampled H within `tip_wait`, and the trigger-record commit position.
- Shutdown drain, failing on timeout; revocation release with late outcomes discarded; idle exit in bounded runs; empty assignments during a rebalance not treated as completion.
- Record-error taxonomy per policy.

Gate: deterministic state-machine tests for out-of-order acknowledgments, partial batch failure, offset gaps, failed and partially failed commits, every cap, shutdown, revocation, and late outcomes. Durable progress never passes unresolved work, and stays intact when a later record fails. A single-partition worker never waits on the timer only because the worker threshold exceeds the partition cap.

## 4. File adapters and file→file

- Magic, framed header, and frame codec with bounded allocation; full record-map fidelity.
- Source snapshot at open, partial-frame end, and fatal change detection.
- Output initialization and recovery: locked temporary header, no-clobber publish, directory fsync, stored destination ID and E before records, failure on an output without stored identity, append in submission order with fsync per confirmed batch, restart validation against E, and tail truncation.
- Locks on the output inode from creation and on the per-group checkpoint lock file; none on source data.
- Checkpoints: safe names, atomic replacement, stored identities, `ONE_OFF` removal on success. Self-copy rejection including path aliases.

Gate: component tests on real temporary files with injected write/fsync/rename failures; subprocess crash/resume, including a crash at every initialization step, replaying but never losing acknowledged records; torn and zero-filled tails past E truncated, damage before E and files shorter than E failing; codec fuzzing; lock conflicts across checkpoint renames and between concurrent creators; a source read while another group writes it; changed sources; corruption; oversized frames.

## 5. Kafka adapters

- One poll loop per worker, read-committed isolation, explicit offsets, generation-aware assignment; classic protocol with protected `cooperative-sticky` and incremental assign/unassign.
- Pre-subscribe group check; startup metadata: cluster IDs, source topic IDs, destination existence and configuration.
- Commit metadata: fingerprint, topic ID, and window checks on every partition; window adoption or local resolution; `fid` and E on every partition for Kafka→file. Lock/create/validate output before subscribing; commit assigned initial positions with `fid`/E before consuming records.
- Start positions: commits, timestamp lookup, `START` and its fallback once the group has progress, expired and above-end commits, no-match lookups at the LSO, the possible-incompleteness warning; tip completion through a sampled high watermark.
- One idempotent producer per worker: `murmur2_random` default, protected settings including the capped `message.max.bytes`, order-preserving queue-full handling, delivery timeout, timestamp-bound checks.
- Failure mapping to the error policies; deterministic authentication and authorization failures fail without retry.

Gate: mocked-client adapter tests plus disposable-broker component tests: null/empty fidelity, timestamp types, delivery errors, Java-compatible routing, partition-count changes, read-committed behavior. Timestamp lookup with non-monotonic and equal timestamps, segment boundaries, and aborted or open transactions. A mixed batch with one broker-rejected timestamp: no innocent record reported skipped, and the idempotent producer's following behavior recorded. Cooperative rebalances while workers join, finish, and idle-exit. Expired commits with `START = LATEST` resume at the log start. A `GROUP` naming another application's group fails before joining it.

## 6. End-to-end verification

- All four directions with byte-level comparison, allowing only documented timestamp exceptions.
- Multiple workers: reassignment, crash, completion, more workers than partitions, concurrent `NOW()` startup with logged per-partition windows, a sole-worker rerun finishing an orphaned partition.
- Kill between durable output and commit: duplicates only, no loss; per-partition order outside the documented replay exceptions.
- Out-of-order, boundary, future, and missing timestamps; cutoff-trigger replay; new source partitions, including under `START = LATEST`; absent, expired, and foreign commits; commits above the log end; a source topic recreated past its commit; transactional partitions ending in commit or abort markers; retention overtaking a running partition; revocation with deliveries in flight duplicating within caps, never losing; empty topics; stalled transactions; outages; commit failures at full caps; fingerprint mismatch; every exit code.

Gate: scenarios prove the recorded semantics using controlled fixtures rather than timing sleeps.

## 7. Packaging and measurement

- Example-based user documentation under 10,000 characters, covering duplicates (including on rebalance), ordering per destination partition, unchecked destination retention, routing, schema compatibility, file recovery and deleting a header-only output after a crash, partial output after a later failure, confirming a bounded group with a sole-worker rerun, single-machine file endpoints, a future-dated record ending a partition, retained-only copying and expired-commit gaps, per-partition `NOW()` windows and fresh windows after offset expiry, and undetected destination recreation.
- Redacted logs: group, windows, stop reasons and triggers, skip positions and counts, producer-stamped timestamp counts, retries, commits, partition changes, terminal causes. Basic counters and gauges, no observability framework.
- Reproducible builds with documented Go/CGO/librdkafka requirements for verified targets.
- Race and static checks, dependency review, malformed-input fuzzing.
- Load tests over key/header/payload mixes, partition counts, and slow sinks: throughput, CPU, peak memory, commit lag, duplicate exposure, shutdown time. Set cap defaults and protected producer queue limits together from these results.
- Fast unit/mock tests on every change; component/e2e CI jobs; scheduled load and fault runs.

Gate: reproducible release candidate, documented operating limits, bounded memory under load, no unexplained loss or premature commit in fault runs.

## Out of scope

Payload decoding or transformation, Schema Registry remapping, topic auto-creation, coordinator or done-marker service, segmented files, viewer, general SQL engine, throughput targets before a baseline.
