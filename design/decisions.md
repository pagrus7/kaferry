# Kaferry copy behavior

Completion, delivery and restart contracts. See the [usage guide](../docs/usage.md) for operating instructions, [syntax](grammar.md), [components and tests](components.md), and [client limits](client-compatibility.md).

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
- For archives, `PARTITION` is the original Kafka partition, across all topics/clusters represented in the directory. Validate every completed header, then replay only matching files; excluded payloads are not hashed or decoded. Pin the filtered selection before output. Resume validates only that saved selection, never discovering additions or requiring excluded files to remain present. Both operators use the existing file checkpoint and one virtual progress cursor.
- Kafka assigns only selected partitions through cooperative sticky balancing. Excluded partitions are never fetched or committed and do not count toward completion or destination layout checks. Saved metadata on all source partitions is still checked for query compatibility.
- All workers in a group use the same predicate and query identity. Selected Kafka copies advertise a query-specific group protocol, without a fallback for older or differently configured members. Changing the predicate requires a new group.
- Kafka partition-only `WHERE` keeps the default EARLIEST/follow behavior; explicit `START = LATEST` is allowed without timestamp bounds. Empty assignments follow the existing worker idle rules; they do not establish group completion. Workers that exited do not discover additions.

## Kafka start positions

Saved commits take precedence. Without a commit:

| Query / group state | Starting position |
| --- | --- |
| `WHERE` with positive lower bound | First offset returned by timestamp lookup for `lo` |
| Non-positive lower bound or upper-only filter | Earliest retained offset |
| No timestamp bounds, group has no valid progress | Earliest retained offset by default; `START = LATEST` uses the stable end |
| No timestamp bounds, group already has valid progress | Earliest retained offset for the uncommitted partition |

`LATEST` and a timestamp lookup with no match use the read-committed end (last stable offset, LSO), so records in open transactions remain eligible. Initial positions are committed before copying. Every partition is positioned explicitly; automatic offset reset is disabled. Out-of-range positions follow the rules below.

- Commit below log start: resume at log start; log old position, log start and offset gap, which is not a lost-record count.
- Commit above log end: fail as truncation. An out-of-range position during copying fails; restart reevaluates it.
- With a saved captured end H, resume fails if the current log end is below H or retention has moved log start beyond H. Kaferry does not capture a replacement end.
- Only retained data is available. A nonzero log start whose first retained timestamp exceeds `lo` produces a retention warning.

## Completion

Copies consume immediately. [Endpoint settings](../docs/reference.md#endpoint-with-properties) define the defaults. None of the timestamp allowances changes the timestamp filter or limits total run time.

For a timestamp upper bound `hi`, each Kafka partition can stop on either enabled rule:

1. A consumed record timestamp strictly exceeds `hi + MAX_OUT_OF_ORDER`.
2. Its consumed position reaches a saved exclusive end **H**. After wall time strictly passes `hi + MAX_STALENESS`, capture the current replicated end H, including open transactions, without waiting to catch up. Save H separately from confirmed progress before processing more input. Timestamp stopping remains active during the drain.

Timestamp stopping assumes later offsets never fall farther behind an earlier timestamp than the configured disorder allowance. A future-dated record can stop before matching records already present. Wall-clock capture assumes arrival delay and the consumer clock's lead fit its allowance. Configuring either value verifies neither assumption. Raising one value does not correct an invalid bound used by the other. No finite allowance covers arbitrary replay or clock errors; retained data cannot recover records removed by retention or compaction.

`OFF` disables a rule. With both rules disabled, Kafka follows filtered records until another stop criterion or a signal. File replay defaults to examining the whole pinned selection. Explicit `MAX_OUT_OF_ORDER` applies the same timestamp rule to each original source identity and partition across its selected files; it cannot stop another original partition. Cursor changes that skip the rest of a stream are saved only through confirmed input progress.

### Offset ends

`RECORD_OFFSET < LATEST` captures the readable committed end (last stable offset) on a partition's first assignment. It excludes unresolved transactions and later appends. Numeric offset bounds supply positions directly; explicit Kafka bounds outside retained positions fail at initialization. An explicit offset end determines completion; timestamps only filter records, and timestamp completion settings are rejected. For files, offsets refer to original Kafka offsets and LATEST means EOF of the pinned file selection.

Ends stay fixed across restart and reassignment. Saved timestamp and end stops remain complete; worker record stops receive a fresh allowance. Appends at or beyond a saved end are excluded. A crash before saving an end can lead to a later capture. Each partition captures independently; neither end mode is an atomic topic snapshot. With saved H, resume fails if log end is below H or retention has moved log start beyond H. Do not capture a replacement end.

Kafka reads only committed records. An open transaction below a replicated H can delay the drain until it commits or aborts. `worker.tip_wait` bounds a continuous wait at the readable tip below H; ordinary backlog progress resets it. Timeout fails the run. Offset gaps, aborted records and control records count toward reaching H. H = 0 completes immediately. Output confirmation precedes saving completion.

### Added assignments and lookups

Eligible added Kafka partitions use the startup assignment path: validate identity and destination layout, load saved progress, reuse resolved windows and ends, resolve missing positions, then save initial state before exposing input. A partition without progress adopts a saved relative window when available. `START = LATEST` bootstraps a fresh group; an uncommitted partition in an established group starts at earliest retained data. A passed timestamp deadline captures its end immediately. A new LATEST range captures its readable end at initialization. Exited workers do not discover additions.

All explicit broker position queries use the injected `OffsetLookup`: earliest retained, readable end, replicated end, or earliest retained match at/after a requested timestamp. The timestamp request includes partition and Unix milliseconds. No match is distinct from an unknown partition, failed request or missing response. A timestamp older than retention can still return a retained match; the broker cannot establish whether earlier matching records expired. Timestamp lower bounds can use this lookup; an upper timestamp lookup alone is not a safe end because later offsets can have earlier timestamps.

Stop logs identify reason, partition and position. Timestamp stops identify the trigger and flag it if more than one minute ahead of the local clock. Inspection previews unsaved offset ends and reports saved ends; it never saves a boundary or validates source-order and clock assumptions.

## Worker record stop

- `WORKER_STOP_AFTER_RECORDS = n` in `OPTIONS` caps records accepted for writing in this process invocation. Filtered records, synchronous SKIP rejections and queue-full retries do not count. Writes remain asynchronous and batched.
- At the threshold, stop reading, allow up to 10 seconds for writes from still-owned partitions, save confirmed progress and exit `0`. Earlier file EOF or Kafka time-window completion also exits `0`. A quiet Kafka follow query can wait indefinitely for enough records. A signal before the threshold exits `1`; at the threshold it succeeds if writes and saving succeed.
- Accepted writes still count after revocation; their late outcomes are discarded and the next owner may replay them. Rebalances do not reset the count. Only currently owned partitions save progress.
- Each invocation gets a fresh allowance across all its assigned partitions. Three workers with `WORKER_STOP_AFTER_RECORDS = 100` can accept 300 writes total, across any number of source partitions. Reruns add another allowance; unsaved writes can repeat.
- A successful drain saves `worker_record_cap` at each owned partition's confirmed position, preserving existing source stops. Write/drain failures save confirmed progress without a new worker stop; save and shutdown failures fail the run. Stop reasons clear when processing resumes. They describe saved partition progress, not worker history.
- With no active Kafka members and a saved stop on every current partition, `status` is `stopped`, even if more input remains. Mixed reasons are listed. `explain` shows the allowance in text and optional JSON `worker_stop_after_records`, without estimating records.

## Groups and workers

### Group names

- `GROUP` identifies a Kafka consumer group or a file-source checkpoint. Omitting it means `GROUP = ONE_OFF`: generate and log `kaferry-<hex>` for a single worker. Repeated or concurrent unnamed runs independently copy the source. Resume an unfinished one-off run with its logged group name; use a shared named group for cooperating Kafka workers. Successful file-source ONE_OFF runs without `WORKER_STOP_AFTER_RECORDS` delete the checkpoint but retain its lock. Runs with a worker threshold keep the checkpoint, even at EOF. Kafka progress expires under broker offset retention.
- Topic→topic workers share a named group and progress independently using classic `cooperative-sticky` assignment. There is no group-wide done marker or external coordinator.

### When a worker exits

- A worker exits `0` after every partition it still holds stops or its record limit is reached, outstanding writes are acknowledged and progress is committed. Partitions released during a rebalance do not count toward source completion. Kafka copies without an enabled completion rule or worker record threshold keep waiting for records or assignments until signalled.
- For a Kafka copy with an enabled completion rule or worker record threshold, the wait starts when Kafka reports an empty assignment while the worker holds no partitions. If it still has none after `worker.idle_exit` (30 seconds by default), it exits `3`. Another empty-assignment notification restarts the timer. Waiting for the first assignment notification or losing all partitions during a rebalance does not start it. The exit logs `local idle exit`, the timeout and the code. A worker assigned an empty partition does not exit this way.
- More workers than source partitions can leave some workers with no partitions to read. This can also happen while Kafka moves partitions between workers. If the last workers exit before a partition reaches its new owner, that partition can remain unfinished. Rerunning the same query and group resumes saved positions and may copy late arrivals.
- If your supervisor restarts workers on nonzero exits, set a policy for code `3`. Restarting rejoins the group and can give the worker partitions to copy.

### Rebalances

- When Kafka revokes partitions, stop reading them, save already-confirmed progress and release them without waiting for outstanding deliveries. Discard their late outcomes; the new owner replays uncommitted records. A worker that has lost ownership does not commit.
- Recoverable rebalance/coordinator/timeout/transport errors during revocation commit are logged and released for replay. Authorization, fencing, fatal/unknown errors, oversized metadata and missing results fail. Initial and periodic commits fail on any error.
- Rebalancing can discover added source partitions; additions after workers exit need another run. Groups cannot seed from another group's offsets.

## Identity

Resume requires matching query and endpoint identity:

| Included in query fingerprint | Excluded |
| --- | --- |
| Endpoint kinds and archive formats; Kafka cluster IDs and topics; normalized timestamp and partition predicates; effective timestamp allowances and offset criteria; Kafka output partitioning mode | Connection aliases/properties, file paths/IDs, `GROUP`, `START`, `WORKER_STOP_AFTER_RECORDS`, error policies and tuning |

Policies can therefore change to SKIP when resuming a failed record. Changing partitioning mode requires a new group. `NOW()` bounds are fingerprinted as expressions; resolved windows are stored separately.

- Kafka saves each next offset with [commit metadata](#commit-metadata).
- Startup and assignment reject mismatched fingerprints/topic IDs, foreign metadata and missing saved `NOW()` windows. Topic IDs must be nonzero. Source identity is checked at startup and assignment, without continuous monitoring. Destination identity is not monitored.
- File-source checkpoints save the query fingerprint, resolved window, fixed file selection, confirmed cursors and optional stop. [Checkpoint contract](archive.md#checkpoints).

Before subscribing, group inspection also rejects active members whose client IDs do not start with `kaferry-` or are assigned to another topic. An absent group passes; an unavailable check fails. The topic check sees assigned partitions, so it cannot check subscriptions of members with no partitions.

### Commit metadata

Kafka stores `kaferry:2;` or `kaferry:3;` followed by a UTF-8 JSON object with each committed offset. The complete string is at most 4,096 bytes; a broker may enforce a smaller limit.

| Field | Meaning |
| --- | --- |
| `fp`, `tid` | Query SHA256 hex fingerprint; source topic UUID in unpadded standard Base64 |
| `win` | Optional `{"lower": integer-or-null, "upper": integer-or-null}`; signed 64-bit Unix milliseconds, lower inclusive/upper exclusive. Required for saved `NOW()` bounds |
| `stop` | Optional `{"reason": "timestamp", "tip" or "worker_record_cap", "at": integer}`; nonnegative source offset where the stopping rule was reached, at or below the committed offset. `at` is not a timestamp |

Absent `stop` means no saved stop. Zero is a valid offset or time bound. Unknown/duplicate fields, null fields other than window bounds, invalid values, foreign prefixes and unsupported versions are rejected. JSON whitespace and field order do not matter. The prefix selects the decoder. Version `2` uses the fields above. Version `3` additionally requires `end`, a nonnegative exclusive captured end offset. Version `2` rejects `end`; version `3` rejects missing or null `end`. Progress without a captured end uses version `2`; progress with one uses version `3`. A committed position or stop cannot exceed its captured end.

The initial assignment commit preserves saved timestamp and end stops and clears worker record stops. A later commit includes it only after every earlier required write succeeds (Kafka acknowledgment or file fsync), including stops at unchanged offsets. Lost assignments never commit. A crash before saving can replay writes.

## Kafka output

- One asynchronous idempotent producer per worker, with delivery reports and client batching.
- Default `PARTITIONING = PRESERVE`: write original partition N to destination partition N, keeping original key bytes, including nulls. Check all selected input partition IDs before output; Kafka assignments recheck the destination. Avro uses the fixed selection's original IDs, never its replay cursor. This includes selected partitions whose records the timestamp filter excludes. Destination counts may be larger; missing IDs fail under every policy. Topics must exist.
- `WITH (PARTITIONING = KEY)` on the destination selects Java-compatible Murmur2 key routing; null keys use sticky routing. Equal non-null keys stay together with a stable partitioner/count. Preserved placement can differ from other destination producers' key routing. Destination partition-count changes are logged. Source offsets are never copied; destination offsets are new.
- Layout checks are not atomic with publishing. Topic recreation or partition changes during a copy can fail after partial output; missing partitions never fall back to key routing.
- Submission order is preserved for records from one source partition routed to one destination partition during an uninterrupted producer session with stable routing. With `KEY`, null keys can spread. Replay and rebalances can duplicate and reorder output; there is no cross-partition order guarantee.
- Client delivery retries default to `worker.delivery_timeout` (60 s). Connection `message.timeout.ms` / `delivery.timeout.ms` overrides it; values must be positive and conflicting aliases fail. Deadlines start at submission, independently of record timestamps. Exhausted delivery fails the run.
- Queue-full refusal pauses reading and retries the same record first, up to `worker.delivery_timeout`. Accepted records are not submitted again within that assignment.
- Optional destination configuration supports size/timestamp preflight; denial logs a warning. [Checks and fallbacks](client-compatibility.md#optional-destination-configuration). Preflight cannot guarantee broker acceptance: clocks, settings, framing and compression may differ.
- The destination determines timestamp type. Known LogAppendTime restamps records. Otherwise timestamps outside ±9,223,372,036,854 Unix milliseconds cannot be published; apply the timestamp policy. Zero and negative timestamps are supported; Kafka uses −1 for a missing timestamp. [Source decoding limit](client-compatibility.md#known-issues). A missing timestamp without timestamp bounds gets producer time, counted and warned about.
- Destination retention is not checked; old CreateTime records may be deleted soon after copying.
- Async delivery errors fail under every policy. A rejected batch does not identify the offending record, so SKIP cannot isolate it. No batch splitting or error-string parsing is attempted. Outcomes after revocation are discarded.

## Delivery and progress

- Kafka sources read committed records. A Kafka delivery report or file fsync acknowledges output before source progress is saved.
- Batches are bounded and non-atomic. Save a partition's next position only after all earlier consumed records are delivered, filtered or explicitly skipped. Consumed positions account for Kafka offset gaps.
- A later failure leaves earlier output intact. Restart can duplicate output acknowledged after the last saved position, and revisits the failing record unless conditions or policy change.
- Default uncommitted caps: 5,000 records / 16 MiB per partition; 20,000 records / 128 MiB per worker. Pause reading at a cap until commit succeeds; a single oversized record may proceed alone. These bound tracked work, not total process memory, which also includes client buffers and overhead.
- Commit all worker partitions in one request, with at most one request in flight: every second, when a worker or partition cap is reached or the next input cannot fit, while paused at a cap with confirmed progress, and on shutdown/revocation. Check results per partition.
- Shutdown stops reading, allows ten seconds for outstanding writes, then commits confirmed progress. Synchronous file flush/fsync can block that save until the whole-process deadline. The CLI enforces `worker.shutdown_timeout` (default 30 s) across draining, saving and cleanup, starting when a signal arrives or stopping begins. Expiry exits 1 immediately; unsaved work may replay. A short budget can end before the final save.

## Errors

Default FAIL. Choose `ON_ERROR` or the specific timestamp policies; they are mutually exclusive. SKIP drops and logs an identified record rejected synchronously before delivery acceptance.

| Skippable failure | Policy |
| --- | --- |
| Missing timestamp required by timestamp bounds in `WHERE` | `ON_MISSING_TIMESTAMP` or `ON_ERROR` |
| Timestamp outside known destination bounds or client range | `ON_REJECTED_TIMESTAMP` or `ON_ERROR` |
| Synchronous size refusal or supported record validation failure | `ON_ERROR` |

- Async delivery errors, fatal client errors, authentication/authorization, out-of-range positions, archive corruption and changed source files cannot be skipped.
- Consumer polling retries classified transient connection/coordinator errors without a total deadline. `worker.tip_wait` bounds only waiting through sampled H, not general outages.
- Required startup inspection and initial/periodic commits fail on error. Optional destination-config inspection warns. Kafka inspection and commit requests use context deadlines; the CLI shutdown budget also covers client cleanup and blocked filesystem or stdio calls.
- Incomplete file replay rechecks every selected file; missing, changed or corrupt inputs fail. [Archive replay](archive.md#replay-and-progress).

## Files

`FILE avro('directory')` captures independently recoverable compressed files. Multiple workers can capture to local or shared storage. Replay uses one process per group/checkpoint directory, saves a fixed completed-file selection, and preserves original Kafka identity. Storage needs durable sync, atomic rename and exclusive locks across its users. [Avro contract](archive.md).

Kafka consumers are created and validated before opening archive output. Kaferry saves source progress only after the archive has synced its data and recovery state. The runner saves progress before closing adapters; closing a Kafka source does not commit offsets. [Capture and recovery](archive.md#capture-and-recovery).

## Settings and results

See the [user reference](../docs/reference.md) for inspection output, saved status, configuration, exit codes and metrics.
