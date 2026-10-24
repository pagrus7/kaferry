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

A timestamp stop commits at the trigger record, so rerunning stops there again. A tip stop commits the next position, so rerunning may copy later arrivals. Without an upper bound, Kafka sources follow until stopped or the worker record threshold is reached.

File sources resume a fixed selection of completed Avro files; a new group selects current files. File sources do not use `CUTOFF`.

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

- A worker exits `0` after every partition it still holds stops or its record limit is reached, outstanding writes are acknowledged and progress is committed. Partitions released during a rebalance do not count toward source completion. Kafka copies without an end time or worker record threshold keep waiting for records or assignments until signalled.
- For a Kafka copy with an end time or worker record threshold, the wait starts when Kafka reports an empty assignment while the worker holds no partitions. If it still has none after `worker.idle_exit` (30 seconds by default), it exits `3`. Another empty-assignment notification restarts the timer. Waiting for the first assignment notification or losing all partitions during a rebalance does not start it. The exit logs `local idle exit`, the timeout and the code. A worker assigned an empty partition does not exit this way.
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
| Endpoint kinds and archive formats; Kafka cluster IDs and topics; normalized timestamp and partition predicates; `CUTOFF`; Kafka output partitioning mode | Connection aliases/properties, file paths/IDs, `GROUP`, `START`, `WORKER_STOP_AFTER_RECORDS`, error policies and tuning |

Policies can therefore change to SKIP when resuming a failed record. Changing partitioning mode requires a new group. `NOW()` bounds are fingerprinted as expressions; resolved windows are stored separately.

- Kafka saves each next offset with [commit metadata](#commit-metadata).
- Startup and assignment reject mismatched fingerprints/topic IDs, foreign metadata and missing saved `NOW()` windows. Topic IDs must be nonzero. Source identity is checked at startup and assignment, without continuous monitoring. Destination identity is not monitored.
- File-source checkpoints save the query fingerprint, resolved window, fixed file selection, confirmed cursors and optional stop. [Checkpoint contract](archive.md#checkpoints).

Before subscribing, group inspection also rejects active members whose client IDs do not start with `kaferry-` or are assigned to another topic. An absent group passes; an unavailable check fails. The topic check sees assigned partitions, so it cannot check subscriptions of members with no partitions.

### Commit metadata

Kafka stores `kaferry:2;` followed by a UTF-8 JSON object with each committed offset. The complete string is at most 4,096 bytes; a broker may enforce a smaller limit.

| Field | Meaning |
| --- | --- |
| `fp`, `tid` | Query SHA256 hex fingerprint; source topic UUID in unpadded standard Base64 |
| `win` | Optional `{"lower": integer-or-null, "upper": integer-or-null}`; signed 64-bit Unix milliseconds, lower inclusive/upper exclusive. Required for saved `NOW()` bounds |
| `stop` | Optional `{"reason": "timestamp", "tip" or "worker_record_cap", "at": integer}`; nonnegative source offset where the stopping rule was reached, at or below the committed offset. `at` is not a timestamp |

Absent `stop` means no saved stop. Zero is a valid offset or time bound. Unknown/duplicate fields, null fields other than window bounds, invalid values, foreign prefixes and unsupported versions are rejected. JSON whitespace and field order do not matter. The prefix selects the decoder. Only version `2` JSON with the fields above is supported.

The initial assignment commit clears the previous stop. A later commit includes it only after every earlier required write succeeds (Kafka acknowledgment or file fsync), including stops at unchanged offsets. Lost assignments never commit. A crash before saving can replay writes.

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

## Inspection

`explain` previews the same statement, configuration, identity and start-position rules as `run`. It never joins a group, consumes records, commits, creates/truncates files or takes locks. Observations can change during inspection; this is not an atomic snapshot or a guarantee that a later run succeeds. Write access is not checked. Records, output bytes and duration are not estimated.

Text sections show endpoints, progress, time windows, behavior and any warnings. They summarize saved/fresh partitions, resolved bounds, start/stop rules, destination routing and record-error policies. Identical bounds and origins share a window summary. At most eight window groups, eight partition IDs per group and eight warnings are printed; omitted counts point to `--json`. Cluster IDs are shown separately from connection aliases; fingerprints and topic IDs stay in JSON.

Kafka JSON rows show the selected start and retained/log-end offsets; log end includes open transactions. Offset differences are not record counts or completion evidence. Fresh `NOW()` values are preview-only.

Archive inspection reads completed-file headers, sizes and saved checkpoint data without scanning record payloads, taking locks or recovering private files. Source positions count confirmed input records. `ONE_OFF` never generates a group or reads an existing checkpoint; the preview says a fresh group would be used.

`--json` emits version 1 with all partitions and warnings. Time bounds use signed Unix milliseconds; byte and offset fields are counts. Connection properties and SQL text are excluded.

| Fields | Meaning |
| --- | --- |
| `version`, `fingerprint`, `group`, `one_off`, `cutoff_ms` | Report format and query identity; one-off group is `ONE_OFF` |
| `start_rule` | Kafka fallback for partitions without progress: `WINDOW`, `EARLIEST` or `LATEST`; absent for file sources |
| `record_errors` | Effective `FAIL`/`SKIP` policies: `missing_timestamp`, `rejected_timestamp`, `other_record`; SKIP applies before writing, not to delivery failures |
| `source`, `destination` | `kind`, `name`, optional connection alias, cluster/topic IDs, partition count, destination timestamp type and `partitioning` (`PRESERVE` or `KEY` for a Kafka destination) |
| Endpoint `file` | `exists`; source adds `size_bytes`, `file_count`, `position_records`, `window`, `window_origin`, `checkpoint`, `checkpoint_exists`. Destination `size_bytes` is zero; destination size and file count are not inspected |
| `source.partition_filter` | Optional `{exclude, ids}` predicate; `exclude=true` means NOT IN. Kafka discovers eligible added partitions while workers run; archives retain their saved file selection |
| `partitions` | Sorted `partition`, `start`, `log_start`, `log_end`, `saved`, `window`, `window_origin` |
| `window` | Inclusive `lower`, exclusive `upper`; null means unbounded. `window_origin`: `literal`, `saved`, `adopted` or `resolved` for preview-only NOW |
| `warnings` | Detected retention gaps and unavailable/changed destination timestamp behavior; may be empty |

### Saved status

`kaferry status` accepts the same query and configuration as `run`; a named `GROUP` is required. It reads saved progress without joining, consuming, producing, committing, creating files or taking locks. It needs no permissions beyond existing source/group DESCRIBE and endpoint metadata access. `ONE_OFF` requires the actual logged group name; a deleted file checkpoint has no saved status.

Verdicts: invalid/incompatible progress → `unknown`; incompatible active members → `unknown`; active members, unsettled group state or changed membership → `running`; no saved progress → `unknown`; any current partition without a saved stop → `incomplete`; otherwise → `stopped`. Added eligible partitions count. Kafka membership is read before and after offsets; observations are not atomic and cannot detect every concurrent start. File activity is not checked. New archive files do not expand a saved replay selection. Status reports `unknown` (5) when a checkpoint or saved selection cannot be validated; other operational read failures exit 1 without a verdict.

`stopped` means every currently selected partition has a saved stop reason. More source records may remain; the time cutoff can miss records within the range. Workers remain independent: they never wait for other workers' stops.

Kafka text lists up to eight partitions; file text reports replay progress. `--json` includes all. JSON version `1` contains `verdict`, optional human-readable `reason`, `meaning`, `group`, `fingerprint`, source/destination endpoints, `activity`, and `partitions` (`id`, optional saved `position`, `stop`, `window`). Kafka positions are offsets; file positions count confirmed input records. File `size_bytes` and `file_count` describe the saved selection, or current completed files before the first run. Inspection checks selected headers and sizes for incomplete groups, not payload checksums. Status does not scan records or verify destination writes.

## Configuration and CLI

Connections use TOML profiles or `WITH CONNECTION`; SQL keys override TOML. Endpoint `WITH` configures source progress/start rules or destination rotation/partitioning; optional final `OPTIONS` holds worker stops and all record-error policies. `ENV('NAME')` resolves at startup, fails if unset and is redacted. Worker settings are in TOML.

Profiles can be shared by sources and destinations. Supported connection keys are mapped to franz-go options; other keys fail before endpoints are opened, without displaying property values. A property name does not imply full librdkafka compatibility.

### Worker settings

Optional keys in TOML's `[worker]` table; byte values are integers:

| Key | Default | Meaning |
| --- | --- | --- |
| `partition_record_cap`, `partition_byte_cap` | 5000, 16777216 (16 MiB) | Unsaved work per partition |
| `record_cap`, `byte_cap` | 20000, 134217728 (128 MiB) | Unsaved work per worker; also sizes Kafka queues |
| `max_frame_bytes` | 67108864 (64 MiB) | Maximum decoded Avro block |
| `archive_capture_open_files` | 1024 | Open capture files; reaching this budget rotates the least recently used file |
| `archive_replay_open_files` | 32 | Open replay handles |
| `archive_replay_blocks` | 8 | Cached replay blocks; overlapping files beyond this budget repeat decoding |
| `delivery_timeout` | `'60s'` | Delivery deadline and queue-full wait |
| `shutdown_timeout` | `'30s'` | Whole-process stop deadline |
| `idle_exit` | `'30s'` | Wait after an empty assignment before exit 3 |
| `tip_wait` | `'15m'` | Wait to reach sampled H |
| `progress_interval` | '30s' | Progress logs; '0' disables |
| `checkpoint_dir` | `~/.kaferry/checkpoints` | File-source checkpoint directory |

Caps must be positive; `max_frame_bytes` is at most 4,294,967,295. Durations are positive Go duration strings, except `progress_interval` permits zero; `delivery_timeout` is 1 ms–2,147,483,647 ms. `checkpoint_dir` must not be empty. Unknown keys fail.

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

Queue/fetch overrides must be positive. Byte queue settings use KiB. Large or compressed batches can exceed fetch ceilings; these do not cap total client memory. The ordinary consumer runs one fetch request at a time; separate requests checking offset gaps can overlap it. Kaferry's uncommitted-work caps remain independent.

`partitioner` is unsupported; select the copy's placement with destination `WITH (PARTITIONING = PRESERVE|KEY)`. Auto-commit/reset, group ID/protocol, read isolation, cooperative assignment, auto topic creation, idempotence, acknowledgements and delivery handling are owned by Kaferry. Kaferry sets these; the corresponding property names are rejected as unsupported. Sources use the SQL group, classic protocol, read-committed and no automatic offset reset. Producers use idempotence and all-ISR acknowledgements. Topic auto-creation is disabled.

### CLI and exit codes

`run`, `explain` and `status` accept exactly one input: `--file/-f path`, `--sql/-e statement`, or `--file -`. `--config/-c path` loads TOML. `explain` and `status` write text or `--json` to stdout; `run` keeps stdout empty. Events are JSON on stderr; errors also print a plain-text message after cleanup. Status verdicts use stdout, including nonzero verdicts. Shutdown timeout diagnostics name the operation still running.

| Normal exit | Meaning |
| --- | --- |
| 0 | Status is `stopped`, or the preview printed, or this worker finished its file input or remaining Kafka partitions, or reached its worker record threshold. A signal cleanly stopped a Kafka copy with no end time or worker record threshold. SKIP counts do not change the code |
| 1 | Copy, inspection, output, cleanup or the shutdown deadline failed, or a signal arrived before completion of startup, a file copy, or a Kafka copy with an end time or worker record threshold. Conflicts after the first consumed record also exit 1 |
| 2 | Usage/parse/plan/configuration error, or a validation conflict before the first consumed record (Kafka topic validation, query/source identity, replay checkpoint/lock or self-copy) |
| 3 | For a Kafka copy with an end time or worker record threshold, Kafka assigned this worker no partitions, and none arrived within `worker.idle_exit` (default 30 seconds). This does not say whether other workers finished |
| 4 | Status is `incomplete`: a partition lacks a saved stop |
| 5 | Status is `unknown`: progress is absent, invalid or incompatible, or the group has an incompatible active member |
| 6 | Status is `running`: group activity or a change was observed |

A busy replay-group lock exits 2. Errors while writing, publishing or recovering archive data exit 1. Startup Kafka topic-validation conflicts (for example, topic authorization denied) exit 2. The first failure determines the code; cleanup cannot replace it. A second signal or external forced termination can use system signal status.

A cleanup error can occur after all records are copied and progress is saved; the error names the close operation. Resume using the same named group to avoid starting over. `ONE_OFF` always creates a new group, so rerunning it can copy records again.

## Observability

JSON `progress` logs on stderr report local counters and buffer usage every `worker.progress_interval` (default 30 s; `0` disables). The final `copy summary` is independent of this setting.

Metric export is off by default. [Setup](../docs/usage.md#monitoring).

A Collector or compatible backend receives metrics; no scrape listener or Kafka requests are added. `explain` and `status` start no telemetry.

Supported exporter environment suffixes: `ENDPOINT`, `PROTOCOL` (`http/protobuf` only), `HEADERS`, `TIMEOUT`, `INSECURE`, `CERTIFICATE` (CA PEM), `CLIENT_CERTIFICATE` and `CLIENT_KEY` (PEM pair). Prefix them with `OTEL_EXPORTER_OTLP_` or `OTEL_EXPORTER_OTLP_METRICS_`; metric-specific settings take precedence. `INSECURE`, if set, must match the endpoint scheme: `true` for `http`, `false` for `https`. Certificate settings require an HTTPS endpoint. Client certificate/key settings must form a pair under the same prefix. A generic endpoint is a base URL with `/v1/metrics` appended; a metric-specific endpoint is the complete URL. Endpoint credentials, queries and fragments are rejected. Headers use comma-separated `name=value` entries with percent-encoded values.

`OTEL_METRIC_EXPORT_INTERVAL` defaults to 60000 ms; `OTEL_METRIC_EXPORT_TIMEOUT` to 30000 ms; exporter `TIMEOUT` to 10000 ms. Durations use positive integer milliseconds. `[metrics].enabled` is the only enable switch; `OTEL_METRICS_EXPORTER` does not control it. Counters use cumulative totals without exporter retries.

Resources include `service.name=kaferry`, version, a generated `service.instance.id` and `messaging.consumer.group.name`. `OTEL_RESOURCE_ATTRIBUTES` adds deployment/query attributes; built-in identity takes precedence. Automatic attributes exclude SQL, record contents, file paths and credentials. Attributes increase the number of series; use stable deployment values.

Metric names are experimental. All describe this worker's invocation; restarts and rebalances can replay records. Source partitions and topics are not metric attributes.

| Metric (`kaferry.` prefix) | Meaning |
| --- | --- |
| `records.submitted`, `records.confirmed` | Separate cumulative counts accepted and acknowledged by the destination; not unique records |
| `records.filtered`, `records.skipped` | Separate cumulative counts excluded by time filters or error policy |
| `buffer.entries`, `buffer.entry_limit` | Unsaved tracked progress entries and their worker cap; entries can include cursor gaps and exclude a record deferred before tracking |
| `buffer.estimated_size`, `buffer.estimated_size_limit` | Tracked cap-accounting bytes and their cap; not payload/wire bytes or total memory |
| `partitions.held`, `partitions.stopped` | Current held partitions and held partitions at their stop rule, split by reason: `timestamp`, `tip`, `eof`, or `worker_record_cap`; not group completion |
| `progress.saves` | Nonempty save operations, including startup and metadata-only writes; `outcome=success|partial|failure` reflects requested partitions |
| `progress.save.duration` | Save duration in seconds |
| `progress.last_success` | Unix seconds of the last nonempty save where every requested partition succeeded; absent until then |
| `loop.iterations` | Completed loop steps, including quiet polls; not successful copying or group health |

Snapshots refresh at most once per second and once at exit. Gauges may lag current state; metric delivery is best effort. Invalid transport settings fail before output opens. Receiver failures produce a redacted, rate-limited warning and copying continues. Cleanup attempts final export for at most one second within the remaining process deadline. Lost exports, short runs, crashes and receiver outages can leave missing metrics; saved source progress is unaffected. Lag sampling and an HTTP health endpoint are not provided.
