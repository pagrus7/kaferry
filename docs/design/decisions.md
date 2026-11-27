# Kaferry copy behavior

Completion, delivery and restart contracts. See the [usage guide](../usage.md) for operating instructions, [syntax](grammar.md), [components and tests](components.md), and [client limits](client-compatibility.md).

## Scope

- Copies topic→topic, topic→file, file→topic and file→file with one SQL-like statement; no transformations or Schema Registry access.
- Preserves raw key/value bytes, null versus empty, and ordered headers including duplicates and NUL/non-UTF-8 names, subject to [client limits](client-compatibility.md). Files retain timestamp values and types; Kafka output follows the destination's timestamp rules.
- Payloads are opaque. Users must ensure byte formats and Schema Registry IDs are compatible with the destination.
- Delivery is at least once for records selected before completion: output acknowledgment precedes source progress. Crashes and rebalances can duplicate output. Filters, SKIP, completion rules and retention can omit records.

## Time window

- `WHERE` filters Kafka record timestamps, not payload event time. Bounds normalize to a logged half-open millisecond range `[lo, hi)`; `BETWEEN` includes both endpoints by adding 1 ms to its upper bound.
- `NOW()` resolves once and is stored with progress: per partition for Kafka, per checkpoint for files. Restart and reassignment reuse the saved window, even when its offset falls outside retention.
- A partition without progress adopts the lowest-numbered partition's valid saved window. If none exists, its worker resolves `NOW()`. Concurrent workers may resolve different windows; use literal bounds for a consistent group-wide range.
- Changed filters require a new `GROUP`. If all group offsets expire or a checkpoint is lost, the job starts fresh and resolves `NOW()` again. Kafka startup logs how many partitions have saved progress.

## Source partitions

- `WHERE PARTITION IN (0, 2)` selects fixed source IDs. `NOT IN` selects all others. Kafka discovers additions while workers run. Omission selects all partitions. Each listed ID must exist at startup in Kafka metadata or completed archive headers; lists are nonempty, sorted and deduplicated.
- `PARTITION BETWEEN 0 AND 2` selects the same IDs as `IN (0, 1, 2)`; both bounds are included. `NOT BETWEEN` excludes that set. Equivalent ranges, lists and offset comparisons keep the same saved query identity.
- For archives, `PARTITION` is the original Kafka partition, across all topics/clusters represented in the directory. Validate every completed header, then replay only matching files; excluded payloads are not hashed or decoded. Pin the filtered selection before output. Resume validates only that saved selection, never discovering additions or requiring excluded files to remain present. Both operators use the existing file checkpoint and one virtual progress cursor.
- Kafka assigns only selected partitions through cooperative sticky balancing. Excluded partitions are never fetched or committed and do not count toward completion or destination layout checks. Saved metadata on all source partitions is still checked for query compatibility.
- All workers in a group use the same predicate and query identity. Selected Kafka copies advertise a query-specific group protocol, without a fallback for older or differently configured members. Changing the predicate requires a new group.

## Kafka start positions

Kafka's [source settings](../reference.md#kafka-source) define the first offset. The positioner disables automatic offset reset and commits explicit initial positions before copying. It uses the last stable offset for a readable end. Resume checks the saved position and end against current retention and truncation bounds. A saved end is never recaptured. [Out-of-range behavior](../reference.md#record-error-policies).

A nonzero log start whose first retained timestamp exceeds the lower time bound produces a retention warning. The broker can identify a retained match but cannot establish whether earlier matching records expired.

## Completion

See [how a copy ends](../reference.md#how-a-copy-ends) for the source limits. Kaferry saves a timestamp stop at the triggering record's offset after confirming earlier writes. A partition that first reaches its offset end saves an end stop.

Saved timestamp and end stops remain complete across restart and reassignment. Worker record stops receive a fresh allowance. A crash before saving an end can lead to a later capture.

### Offset ends

Kafka reads committed records. A numeric or saved end above the current readable end fails assignment. Offset gaps, aborted records and control records count toward reaching the fixed end. An empty range completes immediately after confirming and saving progress.

### Added assignments and lookups

Eligible added partitions use the startup assignment path. Kaferry validates identity and layout, loads saved progress, resolves missing positions and saves initial state before exposing input. Exited workers do not discover additions.

All explicit broker position queries use the injected `OffsetLookup`: earliest retained, readable end, replicated end, or earliest retained match at/after a requested timestamp. The timestamp request includes partition and Unix milliseconds. No match is distinct from an unknown partition, failed request or missing response. Timestamp lower bounds can use this lookup; an upper timestamp lookup alone is not a safe end because later offsets can have earlier timestamps.

Stop logs identify reason, partition and position. Timestamp stops identify the trigger and flag it if more than one minute ahead of the local clock. Inspection previews unsaved offset ends and reports saved ends; it never saves a boundary or validates source-order and clock assumptions.

## Worker record stop

The [record cap](../reference.md#final-options) counts accepted writes across assignments in one invocation. Revocation discards late outcomes and leaves those writes eligible for replay. It does not restore the worker's allowance.

At the cap the worker stops reading, drains still-owned writes for up to ten seconds and saves each owned partition's confirmed position with `worker_record_cap`, preserving existing source stops. A signal at the cap succeeds if draining and saving succeed. On write or drain failure it saves only confirmed progress without a new cap stop. A save or shutdown failure fails the run.

## Groups and workers

### Group names

The [source group](../reference.md#kafka-source) owns Kafka offsets or a file checkpoint. A successful file-source `ONE_OFF` run deletes its checkpoint but retains its lock, unless a worker record cap was configured. With a cap it keeps the checkpoint even at EOF. Kafka progress expires under broker offset retention.

Kafka workers share partitions through classic cooperative-sticky assignment. The group has no shared completion marker.

### When a worker exits

A worker confirms outstanding writes and saves progress for partitions it still owns. A released partition may remain unfinished if all workers exit before its new owner receives it. Resume the same group to finish.

For a bounded or capped Kafka copy, an empty assignment notification starts the idle timer when the worker holds no partitions. A repeated empty notification restarts it. Initial waiting, revocation alone and an assigned partition containing no records do not start the timer. The [exit-code table](../reference.md#exit-codes) distinguishes this worker's idle exit from a completed copy.

### Rebalances

- When Kafka revokes partitions, stop reading them, save already-confirmed progress and release them without waiting for outstanding deliveries. Discard their late outcomes; the new owner replays uncommitted records. A worker that has lost ownership does not commit.
- Recoverable rebalance/coordinator/timeout/transport errors during revocation commit are logged and released for replay. Authorization, fencing, fatal/unknown errors, oversized metadata and missing results fail. Initial and periodic commits fail on any error.
- Rebalancing can discover added source partitions; additions after workers exit need another run. Groups cannot seed from another group's offsets.

## Identity

Resume requires matching query and endpoint identity:

| Included in query fingerprint | Excluded |
| --- | --- |
| Endpoint kinds and archive formats; Kafka cluster IDs and topics; destination topic UUID; normalized timestamp and partition predicates; effective timestamp allowances and offset criteria; Kafka output partitioning mode | Connection aliases/properties, file paths/IDs, `GROUP`, `START`, `WORKER_STOP_AFTER_RECORDS`, error policies and tuning |

Policies can therefore change to SKIP when resuming a failed record. Changing partitioning mode requires a new group. `NOW()` bounds are fingerprinted as expressions; resolved windows are stored separately.

- Kafka saves each next offset with [commit metadata](#commit-metadata).
- Startup and assignment reject mismatched fingerprints/topic IDs, foreign metadata and missing saved `NOW()` windows. Topic IDs must be nonzero. The destination UUID in the fingerprint is read at startup and is not refreshed while a worker runs. Adding partitions with the same UUID keeps that identity.
- File-source checkpoints save the query fingerprint, resolved window, fixed file selection, confirmed cursors and optional stop. [Checkpoint contract](archive.md#checkpoints).

Before subscribing, group inspection also rejects active members whose client IDs do not start with `kaferry-` or are assigned to another topic. An absent group passes; an unavailable check fails. The topic check sees assigned partitions, so it cannot check subscriptions of members with no partitions.

### Commit metadata

Kafka stores `kaferry:4;` followed by a UTF-8 JSON object with each committed offset. The complete string is at most 4,096 bytes; a broker may enforce a smaller limit.

| Field | Meaning |
| --- | --- |
| `fp`, `tid` | Query SHA256 hex fingerprint; source topic UUID in unpadded standard Base64 |
| `win` | Optional `{"lower": integer-or-null, "upper": integer-or-null}`; signed 64-bit Unix milliseconds, lower inclusive/upper exclusive. Required for saved `NOW()` bounds |
| `stop` | Optional `{"reason": "timestamp", "tip" or "worker_record_cap", "at": integer}`; nonnegative source offset where the stopping rule was reached, at or below the committed offset. `at` is not a timestamp |

Absent `stop` means no saved stop. Zero is a valid offset or time bound. Unknown/duplicate fields, null fields other than window bounds, invalid values, foreign prefixes and unsupported versions are rejected. JSON whitespace and field order do not matter. The fixed exclusive offset `end` is required for a `tip` stop, where `at == end`, and for a `timestamp` stop, where `at < end`. A `worker_record_cap` stop can omit `end` on a FOLLOW query. When present, `end` bounds both the committed position and the stop.

The initial assignment commit preserves saved timestamp and end stops and clears worker record stops. A later commit includes it only after every earlier required write succeeds (Kafka acknowledgment or file fsync), including stops at unchanged offsets. Lost assignments never commit. A crash before saving can replay writes.

## Kafka output

- One asynchronous idempotent producer per worker, with delivery reports and client batching.
- With preserved routing, validate all selected original partition IDs before output, including partitions whose records the time filter excludes. Kafka rechecks on assignment. Replay checks the fixed selection's original IDs. Log destination partition-count changes. [Routing, keys and ordering](../reference.md#kafka-sink).

- Layout checks are not atomic with publishing. Topic recreation or partition changes during a copy can fail after partial output; missing partitions never fall back to key routing. The producer retains the first topic UUID it learns and refuses a replacement; that UUID can differ from the one in the startup fingerprint. Brokers using topic-name Produce requests can accept replacement-topic writes before detection; see [franz-go's topic-recreation limits](https://github.com/twmb/franz-go/blob/v1.22.2/README.md#topic-recreation).
- Client delivery retries use `worker.delivery_timeout` ([worker settings](../reference.md#worker-settings)). Connection `message.timeout.ms` / `delivery.timeout.ms` overrides it; values must be positive and conflicting aliases fail. Deadlines start at submission, independently of record timestamps. Exhausted delivery fails the run.
- Queue-full refusal pauses reading and retries the same record first, up to `worker.delivery_timeout`. Accepted records are not submitted again within that assignment.
- Optional destination configuration supports size/timestamp preflight; denial logs a warning. [Checks and fallbacks](client-compatibility.md#optional-destination-configuration). Preflight cannot guarantee broker acceptance: clocks, settings, framing and compression may differ.
- The destination determines timestamp type. Known LogAppendTime restamps records. Otherwise timestamps outside ±9,223,372,036,854 Unix milliseconds cannot be published; apply the timestamp policy. Zero and negative timestamps are supported; Kafka uses −1 for a missing timestamp. [Source decoding limit](client-compatibility.md#kafka-input-timestamps). A missing timestamp without timestamp bounds gets producer time, counted and warned about.
- Destination retention is not checked; old CreateTime records may be deleted soon after copying.
- Async delivery errors fail under every policy. A rejected batch does not identify the offending record, so SKIP cannot isolate it. No batch splitting or error-string parsing is attempted. Outcomes after revocation are discarded.

## Delivery and progress

- Kafka sources read committed records. A Kafka delivery report or file fsync acknowledges output before source progress is saved.
- Batches are bounded and non-atomic. Save a partition's next position only after all earlier consumed records are delivered, filtered or explicitly skipped. Consumed positions account for Kafka offset gaps.
- A later failure leaves earlier output intact. Restart can duplicate output acknowledged after the last saved position, and revisits the failing record unless conditions or policy change.
- [Worker settings](../reference.md#worker-settings) bound uncommitted records and bytes per partition and per worker. Pause reading at a cap until commit succeeds; a single oversized record may proceed alone. These bound tracked work, not total process memory, which also includes client buffers and overhead.
- Commit all worker partitions in one request, with at most one request in flight: every second, when a worker or partition cap is reached or the next input cannot fit, while paused at a cap with confirmed progress, and on shutdown/revocation. Check results per partition.
- Shutdown stops reading, allows ten seconds for outstanding writes, then commits confirmed progress. Synchronous file flush/fsync can block that save until the whole-process deadline. The CLI enforces `worker.shutdown_timeout` across draining, saving and cleanup, starting when a signal arrives or stopping begins. Expiry exits 1 immediately; unsaved work may replay. A short budget can end before the final save.

## Errors

The [record-error policies](../reference.md#record-error-policies) apply while the caller can identify and reject an individual record before delivery acceptance. Once the sink accepts a write, its asynchronous failure stops the run.

- Consumer polling retries classified transient connection/coordinator errors without a total deadline.
- Required startup inspection and initial/periodic commits fail on error. Optional destination-config inspection warns. Kafka inspection and commit requests use context deadlines; the CLI shutdown budget also covers client cleanup and blocked filesystem or stdio calls.
- Incomplete file replay rechecks every selected file; missing, changed or corrupt inputs fail. [Archive replay](archive.md#replay-and-progress).

## Files

`ARCHIVE 'directory'` captures independently recoverable compressed files. Multiple workers can capture to local or shared storage. Replay uses one process per group/checkpoint directory, saves a fixed completed-file selection, and preserves original Kafka identity. Storage needs durable sync, atomic rename and exclusive locks across its users. [Archive contract](archive.md).

Kafka consumers are created and validated before opening archive output. Kaferry saves source progress only after the archive has synced its data and recovery state. The runner saves progress before closing adapters; closing a Kafka source does not commit offsets. [Capture and recovery](archive.md#capture-and-recovery).
