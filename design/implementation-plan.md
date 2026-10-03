# Implementation plan

Baseline: `design-v1` (`bd0d461`). This plan proposes implementation stages; it does not authorize coding or change the agreed semantics.

## Structure

Use single-purpose structs, narrow interfaces at replaceable boundaries, and constructor injection. Fx modules wire each subsystem/adapter at the composition root; the core has no Fx dependency. Avoid interfaces for passive data types and a general plugin framework.

| Component | Responsibility | Boundary |
| --- | --- | --- |
| SQL | Tokenize and parse the restricted command language; report input positions | Parser returning an AST |
| Configuration | Load TOML, merge inline properties, resolve environment values, redact secrets | Loader and environment lookup |
| Planner | Validate semantics and produce an immutable execution plan | Planner and injected clock |
| Runner | Consume, filter, submit bounded batches, and handle lifecycle events | Source, sink, progress store |
| Progress | Track ordered consumed positions, per-record outcomes, ownership, and uncommitted limits | Progress tracker, independent of Kafka types |
| Kafka adapters | Poll/assign/pause/commit; asynchronously produce and report outcomes | Separate source, sink, and commit adapters |
| File adapters | Sequential input, ordered durable output, framing, resume, and locks | Separate source, sink, codec, and checkpoint components |
| CLI/composition | Parse flags, select adapters, wire Fx, handle signals and exit status | Start app, run query, stop app |

Suggested packages: `cmd/kafka-fwd2`, `internal/sql`, `internal/config`, `internal/plan`, `internal/copy`, `internal/kafka`, `internal/file`, and `internal/app`. Split packages further only when responsibility or dependencies justify it.

## Core contracts

- A record owns raw key/value bytes, ordered headers, timestamp/type, and optional original Kafka provenance. Preserve null versus empty explicitly through all adapters.
- Keep the current source cursor separate from provenance: Kafka partition/offset or file ID/frame position. File replay progresses by frame position even when the record contains an original Kafka offset.
- A source supplies records and assignment/revocation events. Pause/resume must leave the Kafka poll loop servicing client events. File sources traverse sequentially to EOF.
- A sink accepts bounded batches and reports one result per submitted record. Define byte ownership and lifetime so buffers cannot be reused while writes remain outstanding.
- A progress store persists confirmed next positions. Kafka commits and atomic file checkpoints implement different adapters behind this boundary.
- Filtered records and explicitly skipped failures count as processed positions. Written records become confirmed only after delivery success or file fsync.
- Track the ordered list of consumed positions, not numeric offset adjacency: compaction and committed isolation can leave gaps. Never commit past an unresolved consumed record.
- Enforce both partition and worker limits across all uncommitted work, including confirmed records awaiting a commit. Bound client queues separately.
- Fence outcomes with assignment ownership/generation so late callbacks cannot advance progress after revocation. Keep the runner's mutable progress state in one event loop where practical.

## 1. Command planning

Build the CLI input path, TOML loader, small parser, AST, and planner before client integration. Prefer a small recursive-descent parser over a general SQL engine.

- Support topic/file endpoints, prefixed connections, `SELECT *`, `RECORD_TIMESTAMP` comparisons, `BETWEEN`, `NOW()`, interval arithmetic, and flat options.
- Freeze `NOW()` once per execution for a group that has no stored window; normalize times and validate interval units and range bounds. Normalize all comparisons to a half-open millisecond range `[lo, hi)`.
- Compute a query fingerprint from the normalized query, excluding resolved values.
- Implement start rules: usable commit with a matching fingerprint wins; otherwise lower time bound, earliest for upper-only filters, or explicit earliest/latest without a time filter. Reject `START` with time filters.
- Validate required group, one-off restrictions, option applicability/conflicts (including `CUTOFF` on file sources), connection aliases, same alias/topic self-copies, and safe file paths/identity before copying.
- Merge SQL properties over TOML by alias/key; resolve environment values without exposing secrets.

Gate: table-driven valid/invalid query tests, examples for all four endpoint combinations, boundary-time tests, and parser fuzzing. No broker needed.

## 2. Delivery and progress core

Implement the runner against small fake sources, sinks, clocks, and progress stores.

- Keep output submission, confirmation, and durable source progress as distinct states.
- Implement bounded batching, per-record results, retry deadlines, filtering, explicit skipping, and pause/resume at uncommitted caps.
- Commit all of a worker's partitions in one request with at most one in flight, every second or at 50% of a worker cap; retain uncommitted accounting until commit success. Admit one oversized record alone.
- Implement the agreed per-partition cutoff rule, including strict `> end + cutoff`, wall-clock/tip completion, and trigger-record commit position.
- Stop intake immediately on shutdown; allow up to 10 seconds for outstanding writes and confirmed progress commits. Apply the same drain bound to revoked partitions.
- Track initial assignment and revocation explicitly: a worker temporarily assigned no partitions during a rebalance must not mistake that for completing its query. In bounded runs, exit after `worker.idle_exit` with no partitions; any assignment resets the timer.

Gate: deterministic state-machine tests for out-of-order acknowledgments, partial batch failure, offset gaps, failed commits, all caps, retries, shutdown, revocation, and late outcomes. Assert that durable progress never passes unresolved work.

## 3. File adapters and first complete copy

Deliver file→file first to exercise the complete runner without a broker.

- Implement a header carrying version, file ID, `GROUP`, and query fingerprint, and length/CBOR/CRC32C frames with bounded allocation. Preserve all named fields, duplicate headers, and null/empty distinctions.
- Append in submission order and fsync once per confirmed batch. File→file retains input traversal order; there is no global ordering guarantee across Kafka partitions.
- Implement safe group-derived checkpoint names, exclusive locks, query/source identity checks, atomic checkpoint replacement, and checkpoint-directory override.
- Identify a source by its header file ID, not path alone. Read only up to the size at open and treat a partial final frame there as end of input. Detect a changed file ID, truncation below the checkpoint, or a last-frame CRC mismatch and apply the changed-source policy; appends are not changes.
- Guard against identical source/destination files, including aliases. Lock file output for single-process use and permit append only when the output header's `GROUP` and fingerprint match.
- Recover an incomplete final output frame; fail on complete corruption. Coordinate output recovery and source checkpoint order so crashes may replay confirmed writes but cannot lose acknowledged records.

Gate: component tests with real temporary files and injected write/fsync/rename failures; subprocess crash/resume tests; framing/decoder fuzzing. Check lock conflicts, source changes, sources still being appended, corruption, oversized frames, EOF, and restart duplicates.

## 4. Kafka adapters

Integrate `confluent-kafka-go`; keep Kafka types inside the adapter packages.

- Use one polling loop per worker, committed isolation, explicit offset management, and ownership-aware assignment/revocation handling.
- Resolve committed positions and retention validity per partition; apply the agreed missing/expired fallback. Persist initial positions before copying.
- Read commit metadata on assignment: fail on a fingerprint mismatch, adopt a stored window, or resolve the window and store it with the initial position. Partitions without commits adopt a committed sibling's window.
- Resolve timestamp starts through the client timestamp lookup. If no qualifying record exists, establish the current readable end and follow/finish according to the time rule.
- Determine the visible tip consistently with committed isolation. Verify client EOF/position behavior around open transactions before relying on it for termination.
- Create one producer per assigned source partition with queue limits that sum within worker caps; on revocation drain, then purge and close it.
- Produce asynchronously with original key bytes and partition unset; retain headers and timestamps. Default to `murmur2_random` unless the connection overrides it. Use delivery reports and librdkafka batching; enable idempotence and protect settings required by our guarantees.
- Read destination timestamp type, cleanup policy, and retention; apply the retention-horizon check at planning and per record, warning when configuration access is denied.
- Validate destination existence and prevent automatic topic creation. Monitor destination partition-count changes and log them without assigning source partitions.
- Map record-specific timestamp failures and transient/fatal failures to the agreed policies. Keep retry duration bounded and expose the terminal cause.

Gate: adapter tests with a thin mocked client boundary plus component tests against disposable brokers. Verify nil/empty fidelity, timestamp types, delivery errors, Java-compatible key routing, different partition counts, and committed versus uncommitted records with real clients.

## 5. Distributed and restart verification

Exercise complete commands against real files and Kafka clusters.

- Cover all four copy directions and byte-level record comparison, with only the documented destination timestamp exceptions.
- Verify same-key source order at the destination across retries, crashes, and revocations whose drain times out.
- Run two workers in one group; force reassignment, stop a completed worker, crash a worker, run more workers than partitions, and verify independent local completion without done markers or a coordinator.
- Demonstrate duplicate windows by killing the process after durable output but before source progress commit; verify no committed record is lost.
- Test out-of-order timestamps, boundary equality, future timestamps, missing timestamps, timestamp rejection, records past destination retention, cutoff-trigger replay, and new source partitions.
- Test absent/expired commits, empty topics, timestamp lookups with no match, stalled transactions, group rebalances, destination outages, and commit failures under full limits.
- Verify resume identity when expressions use `NOW()`: restarts and later workers reuse the stored window, a changed query fails on fingerprint mismatch, and the resolved window is logged.
- Test across different partition counts and opaque schema-encoded bytes. Document that Schema Registry compatibility is the user's responsibility.

Gate: end-to-end scenarios prove the recorded semantics. Retention/time/transaction tests use controlled fixtures rather than timing-dependent sleeps wherever possible.

## 6. Production packaging and measurement

- Provide concise example-based user documentation below 10,000 characters: configuration, four copy directions, time/group semantics, restart duplicates, routing/schema compatibility, file recovery, and errors.
- Emit useful redacted logs: group ID, resolved window, source positions on skips, retries, committed progress, partition changes, and terminal failures. Maintain basic counts and queue/progress gauges without introducing an observability framework initially.
- Provide reproducible builds and documented Go/CGO/librdkafka requirements for supported targets. Verify actual supported build targets before promising portable binaries.
- Run race/static checks, dependency review, and malformed-input fuzzing. Review LOC and cyclomatic complexity as signals; avoid arbitrary gates or abstractions added solely to improve metrics.
- Load-test keyed/unkeyed records, headers, small/large payloads, partition counts, and delayed sinks. Measure throughput, CPU, peak memory, broker connections per worker, commit lag, and shutdown duration; prove queues remain bounded before tuning throughput.
- Use fast unit/mock tests on every change, focused component/e2e CI jobs, and scheduled/on-demand load and extended fault runs. Keep meaningful correctness coverage before optimizing CI duration or binary size.

Gate: reproducible release candidate, documented operating limits, bounded behavior under load, and no unexplained losses or premature commits in fault scenarios.

## Scope discipline

No payload decoding/transformation, Schema Registry remapping, automatic topic creation, coordinator/done-marker service, segmented files, viewer, or general SQL engine. No throughput target is invented before a baseline is measured. Review each stage before expanding abstractions or dependencies.
