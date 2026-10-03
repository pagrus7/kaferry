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
- Progress stores persist confirmed next positions plus identity metadata, including the acknowledged output extent for file outputs: Kafka commits and atomic file checkpoints sit behind one boundary.
- Filtered and skipped records count as resolved; written records resolve on delivery success or fsync. Never commit past an unresolved consumed position.
- Caps cover all uncommitted work, including confirmed records awaiting commit; client queues are bounded separately.
- Outcomes are fenced by assignment generation so late callbacks cannot advance progress. Mutable progress state lives in one event loop.

## 0. Client compatibility spike

A short spike against a disposable broker, before Stage 2, with pinned `confluent-kafka-go`/librdkafka and broker versions. It answers only the client assumptions the design rests on:

- Whether delivery reports attribute a batch rejection to one record.
- Incremental assign/unassign under cooperative-sticky.
- Fetching committed offsets for partitions the worker does not own.
- Key, value, header, and timestamp fidelity through the async producer.
- `auto.offset.reset=error` behavior on out-of-range positions.
- A lower-bound lookup with no match while a transaction is open.
- Partition 0 initialization racing a cooperative revocation, including an in-flight commit: an old owner never overwrites a new window.

Gate: concise recorded outcomes and supported versions in `decisions.md`. A negative or uncertain result reopens the affected rule before Stage 2. Not a prototype and not a substitute for later tests.

## 1. Command planning

- Recursive-descent parser for the full grammar; TOML loader; `ENV` resolution; alias resolution including `default`.
- Planner rules from `grammar.md`; window normalization to `[lo, hi)`; normalized `WHERE` form for the fingerprint (cluster IDs are added at startup).

Gate: table-driven tests from every `grammar.md` example, boundary-time tests, parser fuzzing. No broker.

## 2. Delivery and progress core

Runner against fake sources, sinks, clocks, and progress stores.

- Distinct submission, confirmation, and durable-progress states; bounded batches; per-record results; filtering; skipping; caps with pause/resume.
- Coalesced per-worker commits (one in flight; 1 s, 50% of a worker cap, 50% of any partition cap, or intake paused at a cap with confirmed progress) with per-partition release.
- Per-partition cutoff stop with stop-reason logging, tip completion through a sampled H with `tip_wait`, and trigger-record commit position.
- Shutdown drains (10 s, whole worker) and revocation drains (10 s, outcomes of the revoked partition only), revocation timeout as worker failure, idle exit in bounded runs with its timer started only after an assignment, rebalance-empty assignment not treated as completion.
- Record-error taxonomy: skippable classes per policy; ambiguous batch rejections and fatal errors always fail.

Gate: deterministic state-machine tests for out-of-order acknowledgments, partial batch failure, offset gaps, failed and partially failed commits, every cap, shutdown, revocation, and late outcomes. Durable progress never passes unresolved work. A single-partition worker never waits on the timer only because the worker threshold exceeds the partition cap; waits for real delivery or commit latency are allowed.

## 3. File adapters and file→file

- Fixed magic plus framed header (length, versioned CBOR map, CRC32C) and frame codec with bounded allocation; full record-map fidelity.
- Source snapshot at open, partial-frame end, change detection, and `ON_SOURCE_FILE_CHANGED`; a `CONTINUE` cursor off a frame boundary fails immediately.
- Output initialization: temporary header, no-clobber publish, directory fsync, then stored destination ID before records. Header-only orphan adoption; missing output with stored identity fails.
- Output append in submission order, fsync per confirmed batch, directory fsyncs, header identity checks on resume.
- Acknowledged extent E stored with progress; restart fails below E and truncates only invalid or partial frames from E on.
- Locks: exclusive writer lock on the output data file; per-group exclusive lock on a stable checkpoint lock file; no lock on source data.
- Checkpoints: safe names, atomic replacement, stored identities, `ONE_OFF` removal on success. Reject self-copies including path aliases.
- Crash ordering between output and checkpoint may replay but never lose acknowledged records.

Gate: component tests on real temporary files with injected write/fsync/rename failures; subprocess crash/resume, including a crash at every initialization step; torn and zero-filled tails past E truncated, damage before E and files shorter than E failing; codec fuzzing; lock conflicts across checkpoint renames, a source read while another group writes it, changed sources, corruption, oversized frames.

## 4. Kafka adapters

- One poll loop per worker, read-committed isolation, explicit offsets, generation-aware assignment handling; classic protocol with protected `cooperative-sticky` and incremental assign/unassign.
- Startup metadata: cluster IDs (fingerprint, self-copy), destination existence, timestamp type, timestamp acceptance bounds, cleanup policy, retention.
- Commit metadata: fingerprint checks, foreign-commit rejection, partition-0 window initialization and barrier, missing-anchor failure; partition-0 destination file ID and extent for Kafka→file; partition-0 commits serialized against local ownership.
- Start positions via commits, timestamp lookup, or `START`; protected `auto.offset.reset=error`; expired commits resume at the log start; commits above the log end and in-run out-of-range positions fail; no-match lookups start at the LSO; possible-incompleteness warning; tip completion by consuming through a sampled high watermark, verified around open and aborted transactions.
- One idempotent producer per worker; `murmur2_random` default; protected settings; queue-full handling that preserves order; delivery timeout; timestamp-bound and retention-risk checks before produce.
- Map timestamp, transient, and fatal failures to the error policies; deterministic authentication and authorization failures fail without retry.

Gate: mocked-client adapter tests plus disposable-broker component tests: null/empty fidelity, timestamp types, delivery errors, Java-compatible routing, partition-count changes, read-committed behavior, ownership fencing of the partition-0 initialization commit. Timestamp lookup with non-monotonic and equal timestamps, segment boundaries, and aborted or open transactions. A mixed batch with one broker-rejected timestamp: no innocent record reported skipped, and the idempotent producer's following behavior recorded. Whether delivery reports attribute rejections to single records. Cooperative rebalances while workers join, finish, and idle-exit. Expired commits with `START = LATEST` resume at the log start.

## 5. End-to-end verification

- All four directions with byte-level comparison, allowing only documented timestamp exceptions.
- Multiple workers: reassignment, crash, completion, more workers than partitions, concurrent `NOW()` startup reaching one window, a sole-worker rerun finishing an orphaned partition.
- Kill between durable output and commit: duplicates only, no loss; per-partition order outside the documented replay exceptions.
- Out-of-order, boundary, future, and missing timestamps; retention risk; cutoff-trigger replay; new source partitions; absent, expired, and foreign commits; commits above the log end; retention overtaking a running partition; Kafka→file with concurrent first workers sharing one destination ID; revocation drains succeeding at a documented sink rate and failing with bounded replay when the sink cannot deliver in time; empty topics; stalled transactions; outages; commit failures at full caps; fingerprint mismatch; every exit code.

Gate: scenarios prove the recorded semantics using controlled fixtures rather than timing sleeps.

## 6. Packaging and measurement

- Example-based user documentation under 10,000 characters, covering duplicates, ordering limits, retention risk, routing, schema compatibility, file recovery, errors, confirming a bounded group with a sole-worker rerun, single-machine file endpoints, a single future-dated record ending a partition, copying only retained data, expired-commit gaps, and the sink rate below which rebalances can fail busy workers.
- Redacted logs: group, resolved window, stop reasons and triggers, skip positions and counts, retention-risk counts, producer-stamped timestamp counts, retries, commits, partition changes, terminal causes. Basic counters and gauges, no observability framework.
- Reproducible builds with documented Go/CGO/librdkafka requirements for verified targets.
- Race and static checks, dependency review, malformed-input fuzzing.
- Load tests over key/header/payload mixes, partition counts, and slow sinks: throughput, CPU, peak memory, commit lag, duplicate exposure, shutdown time. Set cap defaults and protected producer queue limits together from these results.
- Fast unit/mock tests on every change; component/e2e CI jobs; scheduled load and fault runs.

Gate: reproducible release candidate, documented operating limits, bounded memory under load, no unexplained loss or premature commit in fault runs.

## Out of scope

Payload decoding or transformation, Schema Registry remapping, topic auto-creation, coordinator or done-marker service, segmented files, viewer, general SQL engine, throughput targets before a baseline.
