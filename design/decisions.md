# Kaferry copy behavior

Completion, delivery and restart contracts. See the [usage guide](../docs/usage.md) for operating instructions, [syntax](grammar.md), [components and tests](components.md), and [client limits](client-compatibility.md).

## Scope

- Copies topic→topic, topic→file, file→topic and file→file with one SQL-like statement; no transformations or Schema Registry access.
- Preserves raw key/value bytes, null versus empty, and ordered headers including duplicates, subject to [client limits](client-compatibility.md#binding-limits). Files retain timestamp values and types; Kafka output follows the destination's timestamp rules.
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

`LATEST` and a timestamp lookup with no match use the read-committed end (last stable offset, LSO), so records in open transactions remain eligible. Initial positions are committed before copying. Every partition is positioned explicitly; `auto.offset.reset=error` is protected.

- Commit below log start: resume at log start; log old position, log start and offset gap, which is not a lost-record count.
- Commit above log end: fail as truncation. An out-of-range position during copying fails; restart reevaluates it.
- Only retained data is available. A nonzero log start whose first retained timestamp exceeds `lo` produces a retention warning.

## Completion

A Kafka partition with an upper bound stops when either:

1. It encounters a timestamp strictly greater than `hi + CUTOFF`.
2. Wall time passes `hi + CUTOFF` and it catches up as described below.

`CUTOFF` defaults to one minute. This is a heuristic: later offsets can have earlier timestamps. For an end of 10:00 and one-minute cutoff, a 10:02 record stops the partition; a subsequent 09:59 record is missed, even if already present. Each stop logs its reason, partition, cursor and trigger time; unusually future-dated triggers are flagged.

### Catching up

After the time threshold passes and the partition reaches its read-committed tip, sample the read-uncommitted log end H once for that attempt. H includes records in open transactions; the consumer continues to read only committed records. Complete when the consumed cursor reaches H, including aborted/control records, and all prior work resolves. H = 0 completes immediately. An open transaction delays completion until it commits or aborts; `worker.tip_wait` (default 15 min) bounds waiting after H is sampled. Timeout fails with H and the stable/consumed positions. Sampling logs H and the current positions. H is neither shared nor persisted.

A timestamp stop commits at the trigger record, so rerunning stops there again. A tip stop commits the next position, so rerunning may copy later arrivals. Without an upper bound, Kafka sources follow indefinitely.

File sources read from their checkpoint or first frame to the size observed at open. A partial final frame ends input; later appends wait for another run. Files do not use `CUTOFF`.

## Groups and workers

- `GROUP` identifies a Kafka consumer group or a file-source checkpoint. `GROUP = ONE_OFF` generates and logs `kaferry-<hex>` for a single worker. Successful file-source ONE_OFF runs delete the checkpoint but retain its stable lock; Kafka progress expires under broker offset retention.
- Topic→topic workers share a named group and progress independently using classic `cooperative-sticky` assignment. There is no group-wide done marker or external coordinator.
- Before subscription, group inspection rejects foreign/invalid progress and active members without the protected client-ID prefix or assigned to another topic. An absent group passes; an unavailable check fails. The topic check sees assigned partitions, so it cannot check the subscriptions of members with no partitions. Positioning validates progress again after assignment.
- A worker exits `0` after every partition it still holds stops, its outstanding writes are acknowledged and progress is committed. Partitions released during a rebalance do not count. Copies from Kafka without an end time keep waiting for new records or assignments until signalled.
- For a copy from Kafka with an end time, the wait starts when Kafka reports an empty assignment while the worker holds no partitions. If it still has none after `worker.idle_exit` (30 seconds by default), it exits `3`. Another empty-assignment notification restarts the timer. Waiting for the first assignment notification or losing all partitions during a rebalance does not start it. The exit logs `local idle exit`, the timeout and the code. A worker assigned an empty partition does not exit this way.
- More workers than source partitions can leave some workers with no partitions to read. This can also happen while Kafka moves partitions between workers. Code `3` does not tell you whether other workers finished. If the last workers exit before a partition reaches its new owner, that partition can remain unfinished. Rerunning the same query and group resumes saved positions and may copy late arrivals.
- If your supervisor restarts workers on nonzero exits, set a policy for code `3`. Restarting rejoins the group and can give the worker partitions to copy.
- On revocation, stop reading the revoked partitions, commit the confirmed prefix and release without waiting for outstanding deliveries. Discard their late outcomes; the new owner replays uncommitted records. Ownership loss never commits.
- Recoverable rebalance/coordinator/timeout/transport errors during revocation commit are logged and released for replay. Authorization, fencing, fatal/unknown errors, oversized metadata and missing results fail. Initial and periodic commits fail on any error.
- Rebalancing can discover added source partitions; additions after workers exit need another run. Groups cannot seed from another group's offsets.

## Identity

Resume requires matching query and endpoint identity:

| Included in query fingerprint | Excluded |
| --- | --- |
| Endpoint kinds; Kafka cluster IDs and topics; normalized `WHERE` expressions; `CUTOFF` | Connection aliases/properties, file paths/IDs, `GROUP`, `START`, error policies and tuning |

Policies can therefore change to SKIP when resuming a failed record. `NOW()` bounds are fingerprinted as expressions; resolved windows are stored separately.

- Kafka commit metadata is `kfwd2:1;` plus compact JSON: SHA256 `fp`, source topic ID `tid`, optional `win` bounds, and file-output `fid`/`extent`. The prefix is a fixed wire identifier.
- Startup and assignment reject mismatched fingerprints/topic IDs, foreign metadata and missing saved `NOW()` windows. Topic IDs must be nonzero. Source identity is checked at startup and assignment, without continuous monitoring; destination recreation is not detected.
- File-source checkpoints store fingerprint, resolved window, source file ID, last-frame CRC and next position; file output also stores destination ID and acknowledged extent. Output headers store group, file ID and fingerprint.

## Kafka output

- One asynchronous idempotent producer per worker, with delivery reports and native batching. The Go batch producer is disabled because it drops headers and timestamps.
- Send original key bytes with partition unset. Default `partitioner=murmur2_random` matches Java routing; connection properties may override it. Equal non-null keys stay together with a stable partitioner/count. Source partitions/offsets are never routing instructions. Topics must exist; destination partition-count changes are logged.
- Submission order is preserved for records from one source partition routed to one destination partition during an uninterrupted producer session with stable routing. Null keys can spread. Replay and rebalances can duplicate and reorder output; there is no cross-partition order guarantee.
- Client delivery retries default to `worker.delivery_timeout` (60 s). Connection `message.timeout.ms` / `delivery.timeout.ms` overrides it; 0 is unbounded, conflicting aliases fail. Exhausted delivery fails the run.
- Queue-full refusal pauses reading and retries the same record first, up to `worker.delivery_timeout`. Accepted records are not submitted again within that assignment.
- Optional destination configuration supports size/timestamp preflight; denial logs a warning. [Checks and fallbacks](client-compatibility.md#optional-destination-configuration). Preflight cannot guarantee broker acceptance: clocks, settings, framing and compression may differ.
- The destination determines timestamp type. Known LogAppendTime restamps records. Otherwise the Go client cannot preserve timestamps ≤ 0 or above 9,223,372,036,854 Unix milliseconds; apply the timestamp policy. A missing timestamp without `WHERE` gets producer time, counted and warned about.
- Destination retention is not checked; old CreateTime records may be deleted soon after copying.
- Async delivery errors fail under every policy. A rejected batch does not identify the offending record, so SKIP cannot isolate it. No batch splitting or error-string parsing is attempted. Outcomes after revocation are discarded.

## Delivery and progress

- Kafka sources read committed records. A Kafka delivery report or file fsync acknowledges output before source progress is saved.
- Batches are bounded and non-atomic. Commit only each partition's contiguous resolved prefix: filtered, delivered or explicitly skipped. Consumed positions account for Kafka offset gaps.
- A later failure leaves earlier output intact. Restart can duplicate output acknowledged after the last saved position, and revisits the failing record unless conditions or policy change.
- Default uncommitted caps: 5,000 records / 16 MiB per partition; 20,000 records / 128 MiB per worker. Pause reading at a cap until commit succeeds; a single oversized record may proceed alone. These bound tracked work, not total process memory, which also includes native buffers and overhead.
- Commit all worker partitions in one request, with at most one request in flight: every second, at 50% of a worker/partition cap, while paused at a cap with confirmed progress, and on shutdown/revocation. Check results per partition.
- Shutdown stops reading, drains writes for up to ten seconds, then commits confirmed progress. Native commits/close and filesystem calls can extend shutdown; `socket.timeout.ms` is not a total commit deadline.

## Errors

Default FAIL. Choose `ON_ERROR` or the specific timestamp policies; they are mutually exclusive. SKIP drops and logs an identified record rejected synchronously before delivery acceptance.

| Skippable failure | Policy |
| --- | --- |
| Missing timestamp required by `WHERE` | `ON_MISSING_TIMESTAMP` or `ON_ERROR` |
| Timestamp outside known destination bounds or client range | `ON_REJECTED_TIMESTAMP` or `ON_ERROR` |
| NUL in a header name sent to Kafka, synchronous size refusal or supported record validation failure | `ON_ERROR` |

- Async delivery errors, fatal client errors, authentication/authorization, out-of-range positions, framing/CRC corruption and changed source files cannot be skipped.
- Consumer polling retries classified transient connection/coordinator errors without a total deadline. `worker.tip_wait` bounds only waiting through sampled H, not general outages.
- Required startup inspection and initial/periodic commits fail on error. Optional destination-config inspection warns. Go context deadlines do not cancel underlying native admin work or bound cleanup.
- File input fails if its ID changes, it is shorter than the checkpoint, or the last checkpointed frame's CRC changes. Appends are allowed; earlier frames are not all revalidated on resume. Use a new group for changed input.

## Files

Any FILE endpoint requires one process per query on one machine, using a local filesystem with locks and hard links. Cross-machine use or copied files cannot be coordinated by these locks and is unsupported.

Output holds an exclusive inode lock. A file-source group holds a stable checkpoint lock that atomic checkpoint replacement does not change. Source data is not locked; other groups can read it.

### Format

[CBOR archive v1](../docs/file-format.md) defines frames and record maps. Invalid/incomplete published headers fail. Reads have configured size limits; there is no compression or segmentation.

### Creation

For Kafka-to-file copies, Kaferry creates the Kafka consumer and checks its settings before opening the output file. Before reading any messages, it sets up progress tracking so saved Kafka offsets include the file ID and the last safely written byte position. On shutdown, Kaferry keeps the file open and locked until the consumer finishes closing, because its shutdown callbacks can still save offsets.

Before copying records (before subscription for Kafka→file):

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
3. Complete frames above `worker.max_frame_bytes` fail without truncation; raise the limit.
4. Keep valid complete frames at/after E; replay may duplicate them. Truncate the first invalid/partial tail beyond E, fsync and log removed bytes.

Without valid ownership, never infer E or delete data. Full-output validation makes restart time grow with file size.

### Checkpoints

File-source checkpoints are atomically replaced in `~/.kaferry/checkpoints`, or `worker.checkpoint_dir`, with filenames derived safely from `GROUP`. Output/checkpoint creation and checkpoint renames fsync the parent directory.

The checkpoint directory must exist; Kaferry does not search or migrate it. Another directory starts fresh for Kafka output and fails for existing file output. Locks belong to that directory, so different directories can duplicate a copy. [Resume and recovery instructions](../docs/usage.md#resume-or-start-over).

## Configuration and CLI

Connections use TOML profiles or `WITH CONNECTION`; SQL keys override TOML. `ENV('NAME')` resolves at startup, fails if unset and is redacted. Worker settings are in TOML.

Known Go binding options are passed only to clients that support them; profiles can be shared by producers and consumers. Admin clients exclude their producer/consumer Go options. Booleans and positive integer capacities are converted from configuration strings. Unknown Go or native property names fail, without displaying property values.

### Worker settings

Optional defaults in TOML's `[worker]` table:

```toml
[worker]
partition_record_cap = 5000
partition_byte_cap = 16777216
record_cap = 20000
byte_cap = 134217728
max_frame_bytes = 67108864
delivery_timeout = '60s'
idle_exit = '30s'
tip_wait = '15m'
# checkpoint_dir = '/var/lib/kaferry/checkpoints'
```

Limits include delivered records whose source progress is not yet saved. [Delivery and progress](#delivery-and-progress).

### Protected client properties

Conflicting values fail configuration; matching values are allowed.

| Source properties | Required value |
| --- | --- |
| `group.id`, `client.id` | SQL group; `kaferry-<group>` |
| `enable.auto.commit`, `enable.auto.offset.store` | `false` |
| `auto.offset.reset`, `isolation.level` | `error`, `read_committed` |
| `partition.assignment.strategy`, `group.protocol` | `cooperative-sticky`, `classic` |
| `enable.partition.eof` | `true` |
| `allow.auto.create.topics`, `go.events.channel.enable`, `go.application.rebalance.enable` | `false` |

Producer requires `enable.idempotence=true`, `go.delivery.reports=true`, `go.batch.producer=false`, `delivery.report.only.error=false` and `allow.auto.create.topics=false`; nonempty `transactional.id` is rejected. All clients require `go.logs.channel.enable=false`; the CLI does not consume Go log channels. Unknown librdkafka keys also fail.

Consumer `queued.max.messages.kbytes` is capped by worker bytes; producer `queue.buffering.max.messages` and `go.produce.channel.size` by worker records, and `go.events.channel.size` by worker records + 1. Producer `queue.buffering.max.kbytes` allows the larger of worker bytes or the effective message-size limit. Byte limits round up to KiB; overrides must be positive and no larger than these bounds.

### CLI and exit codes

Exactly one input: `run --file/-f path`, `run --sql/-e statement`, or `run --file -`. `--config/-c path` loads TOML. Events are JSON on stderr; nonzero exits also print a plain-text message after cleanup, so hung cleanup can delay it indefinitely.

| Normal exit | Meaning |
| --- | --- |
| 0 | This worker finished its file input or remaining assigned Kafka partitions, or a signal cleanly stopped a copy from Kafka with no end time. SKIP counts do not change the code |
| 1 | The copy failed, or a signal interrupted startup, any copy from a file, or a copy from Kafka with an end time. Conflicts after the first consumed record also exit 1 |
| 2 | Usage/parse/plan/configuration error, or identity/ownership/self-copy conflict before the first consumed record |
| 3 | For a copy from Kafka with an end time, Kafka assigned this worker no partitions, and none arrived within `worker.idle_exit` (default 30 seconds). This does not say whether other workers finished |

Busy locks and broker/record failures exit 1. Conflicts include foreign progress/members, changed input and unowned/short/corrupt acknowledged output. The first failure determines the code; cleanup cannot replace it. Native aborts and forced termination use system signal status.
