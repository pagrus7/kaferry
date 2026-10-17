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
- Changed filters require a new `GROUP`. If all group offsets expire or a checkpoint is lost, the job starts fresh and resolves `NOW()` again. Startup logs whether progress exists.

## Kafka start positions

Saved commits take precedence. Without a commit:

| Query / group state | Starting position |
| --- | --- |
| `WHERE` with positive lower bound | First offset returned by timestamp lookup for `lo` |
| Non-positive lower bound or upper-only filter | Earliest retained offset |
| No `WHERE`, group has no valid progress | `START = EARLIEST` or `LATEST` |
| No `WHERE`, group already has valid progress | Earliest retained offset for the uncommitted partition |

`LATEST` and a timestamp lookup with no match use the read-committed end (last stable offset, LSO), so records in open transactions remain eligible. Initial positions are committed before copying. Every partition is positioned explicitly; automatic offset reset is disabled. Out-of-range positions follow the rules below.

- Commit below log start: resume at log start; log old position, log start and offset gap, which is not a lost-record count.
- Commit above log end: fail as truncation. An out-of-range position during copying fails; restart reevaluates it.
- Only retained data is available. A nonzero log start whose first retained timestamp exceeds `lo` produces a retention warning.

## Completion

A Kafka partition with an upper bound stops when either:

1. It encounters a timestamp strictly greater than `hi + CUTOFF`.
2. Wall time passes `hi + CUTOFF` and it catches up as described below.

`CUTOFF` defaults to one minute. This is a heuristic: later offsets can have earlier timestamps. For an end of 10:00 and one-minute cutoff, a 10:02 record stops the partition; a subsequent 09:59 record is missed, even if already present. Each stop logs its reason, partition and position. Timestamp stops also log the trigger timestamp and flag it if more than one minute ahead of the local clock; tip stops log H.

### Catching up

1. After wall time passes `hi + CUTOFF` and the partition reaches its read-committed tip, Kaferry records the log end **H**, including open transactions. H stays fixed for this assignment.
2. Kaferry keeps reading only committed records. The partition stops when its consumed position reaches H, accounting for aborted/control records. H = 0 completes immediately. The worker waits until earlier records are delivered, filtered or skipped before exiting.
3. An open transaction delays completion until it commits or aborts. If H is not reached within `worker.tip_wait` (default 15 min) after sampling, Kaferry fails the run.

Sampling and timeout logs include H and the stable/consumed positions. H is local to this worker and is not saved for restart.

A timestamp stop commits at the trigger record, so rerunning stops there again. A tip stop commits the next position, so rerunning may copy later arrivals. Without an upper bound, Kafka sources follow indefinitely.

File sources read from their checkpoint or first frame to the size observed at open. A partial final frame ends input; later appends wait for another run. Files do not use `CUTOFF`.

## Groups and workers

### Group names

- `GROUP` identifies a Kafka consumer group or a file-source checkpoint. `GROUP = ONE_OFF` generates and logs `kaferry-<hex>` for a single worker. Successful file-source ONE_OFF runs delete the checkpoint but retain its stable lock; Kafka progress expires under broker offset retention.
- Topic→topic workers share a named group and progress independently using classic `cooperative-sticky` assignment. There is no group-wide done marker or external coordinator.

### When a worker exits

- A worker exits `0` after every partition it still holds stops, its outstanding writes are acknowledged and progress is committed. Partitions released during a rebalance do not count. Copies from Kafka without an end time keep waiting for new records or assignments until signalled.
- For a copy from Kafka with an end time, the wait starts when Kafka reports an empty assignment while the worker holds no partitions. If it still has none after `worker.idle_exit` (30 seconds by default), it exits `3`. Another empty-assignment notification restarts the timer. Waiting for the first assignment notification or losing all partitions during a rebalance does not start it. The exit logs `local idle exit`, the timeout and the code. A worker assigned an empty partition does not exit this way.
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
| Endpoint kinds; Kafka cluster IDs and topics; normalized `WHERE` expressions; `CUTOFF` | Connection aliases/properties, file paths/IDs, `GROUP`, `START`, error policies and tuning |

Policies can therefore change to SKIP when resuming a failed record. `NOW()` bounds are fingerprinted as expressions; resolved windows are stored separately.

- Kafka saves each next offset with [commit metadata](#commit-metadata).
- Startup and assignment reject mismatched fingerprints/topic IDs, foreign metadata and missing saved `NOW()` windows. Topic IDs must be nonzero. Source identity is checked at startup and assignment, without continuous monitoring; destination recreation is not detected.
- File-source checkpoints (version `2`) store fingerprint, resolved window, source file ID, last-frame CRC, next byte position, opening size and optional EOF stop; file output adds destination ID and acknowledged extent. Output headers store group, file ID and fingerprint.

Before subscribing, group inspection also rejects active members whose client IDs do not start with `kaferry-` or are assigned to another topic. An absent group passes; an unavailable check fails. The topic check sees assigned partitions, so it cannot check subscriptions of members with no partitions.

### Commit metadata

Kafka stores `kaferry:1;` followed by a UTF-8 JSON object with each committed offset. The complete string is at most 4,096 bytes; a broker may enforce a smaller limit.

| Field | Meaning |
| --- | --- |
| `fp`, `tid` | Query SHA256 hex fingerprint; source topic UUID in unpadded standard Base64 |
| `win` | Optional `{"lower": integer-or-null, "upper": integer-or-null}`; signed 64-bit Unix milliseconds, lower inclusive/upper exclusive. Required for saved `NOW()` bounds |
| `fid`, `extent` | Optional destination archive ID and positive acknowledged byte extent; both present or both absent |
| `stop` | Optional `{"reason": "timestamp" or "tip", "at": integer}`; nonnegative source offset where the stopping rule was reached, at or below the committed offset. `at` is not a timestamp |

Absent `stop` means no saved stop. Zero is a valid offset or time bound. Unknown/duplicate fields, null fields other than window bounds, invalid values, foreign prefixes and unsupported versions are rejected. JSON whitespace and field order do not matter. A format version defines encoding, fields and meaning; any contract change needs a new version. Only version `1` JSON is supported; other encodings require another version.

The initial assignment commit clears the previous stop. A later commit includes it only after every earlier required write succeeds (Kafka acknowledgment or file fsync), including stops at unchanged offsets. Lost assignments never commit. A crash before saving can replay writes. File checkpoints save the same stop shape with reason `eof`; opening size includes any partial final frame.

## Kafka output

- One asynchronous idempotent producer per worker, with delivery reports and client batching.
- Send original key bytes with partition unset. Java-compatible Murmur2 routing is fixed; `partitioner=murmur2_random` is the only accepted setting. Equal non-null keys stay together with a stable partitioner/count. Source partitions/offsets are never routing instructions. Topics must exist; destination partition-count changes are logged.
- Submission order is preserved for records from one source partition routed to one destination partition during an uninterrupted producer session with stable routing. Null keys can spread. Replay and rebalances can duplicate and reorder output; there is no cross-partition order guarantee.
- Client delivery retries default to `worker.delivery_timeout` (60 s). Connection `message.timeout.ms` / `delivery.timeout.ms` overrides it; values must be positive and conflicting aliases fail. Deadlines start at submission, independently of record timestamps. Exhausted delivery fails the run.
- Queue-full refusal pauses reading and retries the same record first, up to `worker.delivery_timeout`. Accepted records are not submitted again within that assignment.
- Optional destination configuration supports size/timestamp preflight; denial logs a warning. [Checks and fallbacks](client-compatibility.md#optional-destination-configuration). Preflight cannot guarantee broker acceptance: clocks, settings, framing and compression may differ.
- The destination determines timestamp type. Known LogAppendTime restamps records. Otherwise timestamps outside ±9,223,372,036,854 Unix milliseconds cannot be published; apply the timestamp policy. Zero and negative timestamps are supported; Kafka uses −1 for a missing timestamp. [Source decoding limit](client-compatibility.md#known-issues). A missing timestamp without `WHERE` gets producer time, counted and warned about.
- Destination retention is not checked; old CreateTime records may be deleted soon after copying.
- Async delivery errors fail under every policy. A rejected batch does not identify the offending record, so SKIP cannot isolate it. No batch splitting or error-string parsing is attempted. Outcomes after revocation are discarded.

## Delivery and progress

- Kafka sources read committed records. A Kafka delivery report or file fsync acknowledges output before source progress is saved.
- Batches are bounded and non-atomic. Save a partition's next position only after all earlier consumed records are delivered, filtered or explicitly skipped. Consumed positions account for Kafka offset gaps.
- A later failure leaves earlier output intact. Restart can duplicate output acknowledged after the last saved position, and revisits the failing record unless conditions or policy change.
- Default uncommitted caps: 5,000 records / 16 MiB per partition; 20,000 records / 128 MiB per worker. Pause reading at a cap until commit succeeds; a single oversized record may proceed alone. These bound tracked work, not total process memory, which also includes client buffers and overhead.
- Commit all worker partitions in one request, with at most one request in flight: every second, at 50% of a worker/partition cap, while paused at a cap with confirmed progress, and on shutdown/revocation. Check results per partition.
- Shutdown stops reading, drains writes for up to ten seconds, then commits confirmed progress. The CLI enforces `worker.shutdown_timeout` (default 30 s) across draining, saving and cleanup, starting when a signal arrives or stopping begins. Expiry exits 1 immediately; unsaved work may replay. A short budget can end before the final save.

## Errors

Default FAIL. Choose `ON_ERROR` or the specific timestamp policies; they are mutually exclusive. SKIP drops and logs an identified record rejected synchronously before delivery acceptance.

| Skippable failure | Policy |
| --- | --- |
| Missing timestamp required by `WHERE` | `ON_MISSING_TIMESTAMP` or `ON_ERROR` |
| Timestamp outside known destination bounds or client range | `ON_REJECTED_TIMESTAMP` or `ON_ERROR` |
| Synchronous size refusal or supported record validation failure | `ON_ERROR` |

- Async delivery errors, fatal client errors, authentication/authorization, out-of-range positions, framing/CRC corruption and changed source files cannot be skipped.
- Consumer polling retries classified transient connection/coordinator errors without a total deadline. `worker.tip_wait` bounds only waiting through sampled H, not general outages.
- Required startup inspection and initial/periodic commits fail on error. Optional destination-config inspection warns. Kafka inspection and commit requests use context deadlines; the CLI shutdown budget also covers client cleanup and blocked filesystem or stdio calls.
- File input fails if its ID changes, it is shorter than the checkpoint, or the last checkpointed frame's CRC changes. Appends are allowed; earlier frames are not all revalidated on resume. Use a new group for changed input.

## Files

Any FILE endpoint requires one process per query on one machine, using a local filesystem with locks and hard links. Cross-machine use or copied files cannot be coordinated by these locks and is unsupported.

Output holds an exclusive inode lock. A file-source group holds a stable checkpoint lock that atomic checkpoint replacement does not change. Source data is not locked; other groups can read it.

### Format

[CBOR archive v1](../docs/file-format.md) defines frames and record maps. Invalid/incomplete published headers fail. Reads have configured size limits; there is no compression or segmentation.

### Creation

For Kafka-to-file copies, Kaferry creates the Kafka consumer and checks its settings before opening the output file. Before reading any messages, it sets up progress tracking so saved Kafka offsets include the file ID and the last safely written byte position. The runner saves confirmed progress before adapters close; closing a Kafka source does not save progress.

Before copying records:

1. Write/fsync a complete header to a locked temporary file.
2. Publish without replacing an existing path; fsync the directory. A losing creator opens and locks the published file.
3. Save destination ID and header extent with initial source progress, checking every partition result or the file checkpoint.
4. Begin copying. Cleanup removes only temporary files whose locks it acquires.

An existing output without saved ownership fails. Startup failure, interruption or a crash before the first successful progress save can leave a header-only file. Delete the file manually only after verifying it is header-only and saved progress does not name it, then rerun. Missing output named by progress is never recreated automatically.

### Output recovery

Output is fsynced before acknowledgment. Write/flush/short-write/fsync failure poisons the output for the run: pending work is not acknowledged and its saved extent does not advance.

Progress stores acknowledged extent E, initially the header end. Kafka→file uses the largest committed E across partitions. Restart:

1. Validate header group/fingerprint and stored destination ID; partitions without their own ID adopt the group's. Never regenerate IDs.
2. Fail if output is missing or shorter than E. Scan and CRC-check the whole output; corruption before E or E inside a frame fails.
3. Frames whose encoded record exceeds `worker.max_frame_bytes` fail without truncation; raise the limit.
4. Keep valid complete frames at/after E; replay may duplicate them. Truncate the first invalid/partial tail beyond E, fsync and log removed bytes.

Without valid ownership, never infer E or delete data. Full-output validation makes restart time grow with file size.

### Checkpoints

File-source checkpoints are atomically replaced in `~/.kaferry/checkpoints`, or `worker.checkpoint_dir`, with filenames derived safely from `GROUP`. Output/checkpoint creation and checkpoint renames fsync the parent directory.

The checkpoint directory must exist; Kaferry does not search or migrate it. Another directory starts fresh for Kafka output and fails for existing file output. Locks belong to that directory, so different directories can duplicate a copy. [Resume and recovery instructions](../docs/usage.md#resume-or-start-over).

## Inspection

`explain` previews the same statement, configuration, identity and start-position rules as `run`. It never joins a group, consumes records, commits, creates/truncates files or takes locks. Observations can change during inspection; this is not an atomic snapshot or a guarantee that a later run succeeds. Write access is not checked. Records, output bytes and duration are not estimated.

Text sections show endpoints, progress, time windows, behavior and any warnings. They summarize saved/fresh partitions, resolved bounds, start/stop rules, destination routing and record-error policies. Identical bounds and origins share a window summary. At most eight window groups, eight partition IDs per group and eight warnings are printed; omitted counts point to `--json`. Cluster IDs are shown separately from connection aliases; fingerprints and topic IDs stay in JSON.

Kafka JSON rows show the selected start and retained/log-end offsets; log end includes open transactions. Offset differences are not record counts or completion evidence. Fresh `NOW()` values are preview-only.

File inspection reads the archive header, checkpoint and last checkpointed frame identity without scanning records or recovering a tail. Text shows encoded input bytes after the selected position before filtering, and whether output would be created or resumed. A run reads existing output frames during recovery; inspection does not verify full archive integrity. `ONE_OFF` never generates a group or reads an existing checkpoint; the preview says a fresh group would be used.

`--json` emits version 1 with all partitions and warnings. Time bounds use signed Unix milliseconds; byte and offset fields are counts. Connection properties and SQL text are excluded.

| Fields | Meaning |
| --- | --- |
| `version`, `fingerprint`, `group`, `one_off`, `cutoff_ms` | Report format and query identity; one-off group is `ONE_OFF` |
| `start_rule` | Kafka fallback for partitions without progress: `WINDOW`, `EARLIEST` or `LATEST`; absent for file sources |
| `record_errors` | Effective `FAIL`/`SKIP` policies: `missing_timestamp`, `rejected_timestamp`, `other_record`; SKIP applies before writing, not to delivery failures |
| `source`, `destination` | `kind`, `name`, optional connection alias, cluster/topic IDs, partition count, destination timestamp type |
| Endpoint `file` | `exists`, `id`, `size_bytes`; source `start_bytes`, `window`, `window_origin`, `checkpoint`, `checkpoint_exists`; destination `acknowledged_extent_bytes` when saved |
| `partitions` | Sorted `partition`, `start`, `log_start`, `log_end`, `saved`, `window`, `window_origin` |
| `window` | Inclusive `lower`, exclusive `upper`; null means unbounded. `window_origin`: `literal`, `saved`, `adopted` or `resolved` for preview-only NOW |
| `warnings` | Detected retention gaps and unavailable/changed destination timestamp behavior; may be empty |

### Saved status

`kaferry status` accepts the same query and configuration as `run`; a named `GROUP` is required. It reads saved progress without joining, consuming, producing, committing, creating files or taking locks. It needs no permissions beyond existing source/group DESCRIBE and endpoint metadata access. `ONE_OFF` requires the actual logged group name; a deleted file checkpoint has no saved status.

Verdicts: incompatible/unreadable progress → `unknown`; incompatible active members → `unknown`; active members, unsettled group state or changed membership → `running`; no saved progress → `unknown`; any current partition without a saved stop → `incomplete`; otherwise → `stopped`. Added partitions count. Kafka membership is read before and after offsets; observations are not atomic and cannot detect every concurrent start. File activity is not checked. File growth since opening makes a saved EOF stop `incomplete`.

`stopped` means a stop was saved for every current partition; the cutoff rule can still miss records within the time range. Workers remain independent: they never wait for other workers' stops.

Text lists up to eight partitions; `--json` includes all. JSON version `1` contains `verdict`, optional human-readable `reason`, `meaning`, `group`, `fingerprint`, source/destination endpoints, `activity`, and `partitions` (`id`, optional saved `position`, `stop`, `window`). File status adds `opening_size_bytes` and current source `file.size_bytes`. Kafka positions are offsets; file positions are bytes. Status does not scan records or verify destination writes.

## Configuration and CLI

Connections use TOML profiles or `WITH CONNECTION`; SQL keys override TOML. `ENV('NAME')` resolves at startup, fails if unset and is redacted. Worker settings are in TOML.

Profiles can be shared by sources and destinations. Supported connection keys are mapped to franz-go options; other keys fail before endpoints are opened, without displaying property values. A property name does not imply full librdkafka compatibility.

### Worker settings

Optional keys in TOML's `[worker]` table; byte values are integers:

| Key | Default | Meaning |
| --- | --- | --- |
| `partition_record_cap`, `partition_byte_cap` | 5000, 16777216 (16 MiB) | Unsaved work per partition |
| `record_cap`, `byte_cap` | 20000, 134217728 (128 MiB) | Unsaved work per worker; also sizes Kafka queues |
| `max_frame_bytes` | 67108864 (64 MiB) | Maximum encoded archive record, including key, value, headers and metadata; excludes frame length and CRC |
| `delivery_timeout` | `'60s'` | Delivery deadline and queue-full wait |
| `shutdown_timeout` | `'30s'` | Whole-process stop deadline |
| `idle_exit` | `'30s'` | Wait after an empty assignment before exit 3 |
| `tip_wait` | `'15m'` | Wait to reach sampled H |
| `checkpoint_dir` | `~/.kaferry/checkpoints` | File-source checkpoint directory |

Caps must be positive; `max_frame_bytes` is at most 4,294,967,295. Durations are positive Go duration strings; `delivery_timeout` is 1 ms–2,147,483,647 ms. `checkpoint_dir` must not be empty. Unknown keys fail.

Caps include delivered records whose source progress is not yet saved. [Delivery and progress](#delivery-and-progress).

### Connection properties

Values are strings in TOML and SQL. Unknown keys, unsupported values and contradictory aliases fail; they are never silently ignored. Authentication and TLS paths are validated before output opens.

| Keys | Meaning / limits |
| --- | --- |
| `bootstrap.servers` | Required comma-separated host[:port] entries, no protocol prefix |
| `security.protocol` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. `ssl.*` keys require SSL or SASL_SSL; `sasl.*` keys require SASL_PLAINTEXT or SASL_SSL |
| `sasl.mechanism`, `sasl.username`, `sasl.password` | PLAIN (default), SCRAM-SHA-256 or SCRAM-SHA-512; credentials required for SASL |
| `ssl.ca.location` | PEM CA file; otherwise system roots |
| `ssl.certificate.location`, `ssl.key.location` | PEM client certificate and unencrypted private key; supply both |
| `ssl.endpoint.identification.algorithm` | `https` (default) or `none`; `none` disables hostname checks but retains CA-chain checks |
| `client.id` | Optional suffix; source ID is `kaferry-<group>-<suffix>`, other clients `kaferry-<suffix>` |
| `compression.type` | `none` (default), gzip, snappy, lz4, zstd |
| `partitioner` | Only `murmur2_random`; Java-compatible Murmur2 for non-null keys, sticky routing for null keys |
| `message.max.bytes` | Client batch ceiling including 4–5 bytes of outer framing, 1,000–1,000,000,000 bytes; default 1,000,000, adjusted by readable destination config |
| `message.timeout.ms`, `delivery.timeout.ms` | Submission deadline; positive milliseconds, aliases must agree; default worker delivery timeout |
| `queue.buffering.max.messages`, `queue.buffering.max.kbytes` | Sink admission bounds: at most worker records / the larger of worker bytes or the message limit (rounded up to KiB) |
| `queued.max.messages.kbytes`, `fetch.max.bytes`, `max.partition.fetch.bytes` | Soft per-fetch ceilings, capped by worker bytes; defaults worker bytes / 50 MiB / 1 MiB |
| `session.timeout.ms`, `heartbeat.interval.ms` | Defaults 45,000 / 3,000; heartbeat must be smaller than session |
| `max.poll.interval.ms` | Group rebalance timeout, default 300,000; at least session timeout and 30,000. Not an application poll watchdog |
| `request.timeout.ms` | Produce request timeout, default 30,000; not the record-delivery deadline |
| `socket.timeout.ms` | Request timeout overhead, default 2,000; 100–900,000; not a total-operation deadline |
| `linger.ms` | Batch wait, default 5; 0–60,000 |
| `metadata.max.age.ms`, `topic.metadata.refresh.interval.ms` | Metadata refresh age, default 300,000; aliases must agree; 10–3,600,000 |

Queue/fetch overrides must be positive. Byte queue settings use KiB. Large or compressed batches can exceed fetch ceilings; these do not cap total client memory. Only one fetch request runs at a time. Kaferry's uncommitted-work caps remain independent.

Auto-commit/reset, group ID/protocol, read isolation, cooperative assignment, auto topic creation, idempotence, acknowledgements and delivery handling are owned by Kaferry. Kaferry sets these; the corresponding property names are rejected as unsupported. Sources use the SQL group, classic protocol, read-committed and no automatic offset reset. Producers use idempotence and all-ISR acknowledgements. Topic auto-creation is disabled.

### CLI and exit codes

`run`, `explain` and `status` accept exactly one input: `--file/-f path`, `--sql/-e statement`, or `--file -`. `--config/-c path` loads TOML. `explain` and `status` write text or `--json` to stdout; `run` keeps stdout empty. Events are JSON on stderr; errors also print a plain-text message after cleanup. Status verdicts use stdout, including nonzero verdicts. Shutdown timeout diagnostics name the operation still running.

| Normal exit | Meaning |
| --- | --- |
| 0 | Status is `stopped`, or the preview was printed, or this worker finished its file input or remaining assigned Kafka partitions, or a signal cleanly stopped a copy from Kafka with no end time. SKIP counts do not change the code |
| 1 | Copy, inspection, output, cleanup or the shutdown deadline failed, or a signal interrupted startup, a file copy, or a Kafka copy with an end time. Conflicts after the first consumed record also exit 1 |
| 2 | Usage/parse/plan/configuration error, or identity/ownership/self-copy conflict before the first consumed record |
| 3 | For a copy from Kafka with an end time, Kafka assigned this worker no partitions, and none arrived within `worker.idle_exit` (default 30 seconds). This does not say whether other workers finished |
| 4 | Status is `incomplete`: input lacks a saved stop, or the source file grew |
| 5 | Status is `unknown`: progress is absent, incompatible or unreadable, or the group has an incompatible active member |
| 6 | Status is `running`: group activity or a change was observed |

Busy locks and broker/record failures exit 1. Conflicts include foreign progress/members, changed input and unowned/short/corrupt acknowledged output. The first failure determines the code; cleanup cannot replace it. A second signal or external forced termination can use system signal status.

A cleanup error can occur after all records are copied and progress is saved; the error names the close operation. Resume using the same named group to avoid starting over. `ONE_OFF` always creates a new group, so rerunning it can copy records again.
