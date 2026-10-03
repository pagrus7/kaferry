# kafka-fwd2 design

## Scope

- Lightweight Go CLI that copies Kafka records with one SQL-like statement (`grammar.md`); no transformations. Directions: topic→topic, topic→file, file→topic, file→file.
- Raw key/value bytes, null versus empty, ordered headers (duplicates included), and timestamp/type are preserved where the destination allows. Payloads are opaque; format and Schema Registry compatibility are the user's responsibility.
- Delivery is at least once: crashes can duplicate records but never lose acknowledged ones.

## Time window

- `WHERE` bounds `RECORD_TIMESTAMP` (Kafka record time). Every filter normalizes to a logged half-open millisecond range `[lo, hi)`.
- `NOW()` is resolved once and stored with progress (per partition for Kafka sources, in the checkpoint for file sources); restarts and reassignment reuse it. A Kafka partition without progress adopts the window of the lowest-numbered partition with valid progress; if none has any, its worker resolves `NOW()`. Workers that start a group together can resolve different windows, with no bound on the difference; literal bounds give every partition one window. Each partition filters by its own window and logs it, adopted or resolved.
- A new window needs a new group. A group whose Kafka offsets have all expired (`offsets.retention.minutes`) or whose checkpoint is gone is new: a rerun resolves a fresh window, and a literal window recopies retained records. Startup logs whether progress exists.

## Kafka start positions

- A partition resumes at its commit. Without one it starts at the earliest offset with timestamp `>= lo` (timestamp lookup), at the earliest retained offset for an upper-only filter, or, without `WHERE`, at `START`. `START` applies only while the group has no valid progress on the source topic; after that, a partition without a commit starts at its earliest retained offset, logged. Initial positions are committed before copying.
- A lookup with no match starts at the read-committed end (the LSO, never the high watermark), so a matching record in an open transaction is not skipped.
- `auto.offset.reset=error` is protected; every partition is positioned explicitly.
- A commit below the log start resumes at the log start, since every retained record after it is unread, and logs the old position, the log start, and the offset gap (not a lost-record count).
- A commit above the log end fails as truncation. An out-of-range position during a run fails naming the positions; a restart applies these rules.
- Only retained records are copied. When the log start is above 0 and the first retained record is newer than `lo`, the run warns that the window may begin before retained data.

## Completion

- With an upper bound, a Kafka partition stops at the first timestamp `> hi + cutoff` (timestamp stop), or at the read-committed tip once wall time passes `hi + cutoff` (tip stop); `CUTOFF` defaults to one minute. Records past the stop are omitted, including everything after a single future-dated record. Why: Kafka has no end-of-window marker, and a per-partition rule needs no coordination.
- Every stop logs and counts its reason, partition, cursor, trigger timestamp, and distance past `hi`; conspicuously future-dated triggers are flagged.
- A tip stop samples the high watermark H once per attempt, after wall time passes `hi + cutoff` and the partition reaches its read-committed tip. It completes when the consumed cursor reaches H (past aborted and control records; H = 0 completes at once) and all prior work is resolved; it never waits for a later visible record. Exceeding `worker.tip_wait` (default 15 min) fails the worker naming the partition, H, and the stable and consumed positions. H is never shared or persisted. Why: an open transaction holds the LSO back while hiding records already written.
- A timestamp stop commits at the trigger record, so a rerun stops there again. A tip stop commits the next offset, so a rerun can copy later arrivals. Without an upper bound, Kafka sources follow.
- File sources start at their checkpoint or first frame and read to EOF, the size at open; a partial frame there ends input, and later appends wait for another run. Files interleave source partitions, so they take no `CUTOFF`.

## Groups and workers

- `GROUP` is a Kafka consumer group for Kafka sources and a local checkpoint identity for file sources; workers share a named group. `GROUP = ONE_OFF` generates and logs an ID, runs one worker, and resumes through the named form; a successful file-source `ONE_OFF` run deletes its checkpoint, and Kafka `ONE_OFF` groups expire through broker offset retention.
- Kafka workers share a group with no coordination and progress independently per assigned partition, using the classic group protocol with protected `cooperative-sticky` assignment. Why: nothing extra to run, and eager rebalancing would revoke every busy worker's partitions on each exit.
- Before subscribing, a worker checks the group: invalid or foreign progress on the source topic fails, as do active members lacking the protected kafka-fwd2 `client.id` prefix or subscribed to another topic. An absent group passes; a check that cannot run fails. It guards against typos, not concurrent starts; positioning validates again.
- A worker exits when all its assigned partitions have stopped; an empty assignment during a rebalance is not completion. In bounded runs a worker holding no partitions for `worker.idle_exit` (default 30 s) exits 0, logged as an idle exit; the timer starts only after a successful assignment (empty included). In follow mode it stays as a standby.
- There is no group-wide done signal; all workers exiting 0 does not prove coverage, since an idle worker can leave while a partition is being handed to it. To confirm a bounded group, rerun the same statement and `GROUP` with one worker after previous members have left: as sole member it finishes orphaned work, and its completion (never an idle exit) is the confirmation. It is a real copy run, so tip-stopped partitions may copy late arrivals.
- On revocation, a worker stops intake for the partition, commits its confirmed prefix, and releases it without waiting for in-flight deliveries. Their late outcomes are discarded and the new owner replays them, so a rebalance can duplicate up to a partition's uncommitted work.
- Rebalances may pick up source partitions added during a run; bounded workers can exit first, and later additions need another run. A group never seeds from another group's offsets.

## Identity

- The query fingerprint hashes endpoint kinds, cluster ID + topic of each Kafka endpoint, the normalized `WHERE` (per bound: side, literal ms or `NOW()`, and the millisecond adjustment including the 1 ms normalization, with checked arithmetic), and `CUTOFF`. It excludes aliases, paths, file IDs, connection properties, `GROUP`, `START`, error policies (so a failed run can resume with `SKIP`), and tuning.
- Kafka commit metadata is versioned `kfwd2:1` and on every partition carries `fp=<sha256>`, the source topic ID `tid`, `win=<lo>,<hi>` for `NOW()` queries (either side may be empty), and, for file outputs, the destination file ID and acknowledged output extent.
- Startup and positioning fail on a fingerprint mismatch, a commit without kafka-fwd2 metadata, `NOW()` progress without its window, and a `tid` that differs from the topic's current ID, naming both IDs. Topic IDs come from `AdminClient.DescribeTopics`; a missing or zero ID never bypasses the check. This detects recreation between runs, not deletion during one; destination recreation is not detected. A stored window stays authoritative after its offset leaves retention.
- File identity is validated outside the fingerprint. File-source checkpoints store fingerprint, window, source file ID, last-frame CRC, next frame position, and, for file outputs, the destination file ID and acknowledged extent. Output headers store their own file ID and the fingerprint.

## Kafka output

- One idempotent producer per worker, asynchronous with delivery reports; librdkafka batches internally (the Go batch producer drops headers and timestamps).
- The original key is sent with partition unset, using `partitioner=murmur2_random` (Java-compatible) unless the connection sets one. Source partition and offset are not copied; destination partition-count changes are logged. Destination topics must exist.
- Delivery retries belong to the client (`message.timeout.ms`, default 60 s), then the run fails. A queue-full refusal pauses intake and retries the same record first. Terminal record failures follow FAIL/SKIP and are never re-produced behind later records.
- Preflight reads the destination's timestamp type, timestamp acceptance bounds, and `max.message.bytes`; unreadable configuration warns and skips the dependent check.
- CreateTime records are checked at wall time against the acceptance bounds (`message.timestamp.before.max.ms` / `after.max.ms`, or legacy `difference.max.ms`) before produce; records outside them go through `ON_REJECTED_TIMESTAMP` and are never sent. The check can race broker clocks and configuration.
- The producer's `message.max.bytes` is capped at the destination's `max.message.bytes` and logged, so oversized records normally fail synchronously, attributed to one record. librdkafka's limit is not strict (framing, compression), so broker size rejections remain possible.
- Destination retention is not checked: records older than a CreateTime destination's `retention.ms` are copied and may be deleted by the broker soon after.
- A broker rejection is skippable only when its delivery report attributes it to one record. Other batch rejections and fatal producer errors fail the worker under every policy; no innocent record is marked skipped, error strings are not parsed, and batches are not split by re-producing. Why: the broker rejects the whole batch.
- Ordering: records from one source partition that reach the same destination partition keep submission order within one uninterrupted producer session with stable routing (unchanged partitioner and destination partition count). Nothing is ordered across destination or source partitions, and null-key records spread across partitions. Crash replay repeats runs (100, 101, 100, 101), and records submitted before a revocation can land after the new owner's replays. `LogAppendTime` destinations restamp records.

## Delivery and progress

- Source records are read committed. Output is acknowledged (delivery report or fsync) before source progress is committed.
- Batches are bounded and non-atomic. Each partition commits only its contiguous resolved prefix (filtered, delivered, or skipped), tracked by consumed positions rather than offset arithmetic.
- No copy is all-or-nothing: a later failure leaves earlier acknowledged output in place, and a restart revisits the failing record until the policy or conditions change.
- Uncommitted work is capped at 5,000 records / 16 MiB per partition and 20,000 records / 128 MiB per worker; one oversized record may proceed alone; intake pauses at a cap until a commit succeeds. Record caps bound crash duplicates and byte caps bound memory; load tests set the defaults together with protected producer queue limits.
- Each worker commits all its partitions in one request, at most one in flight: every second, at 50% of a worker cap or of any partition's cap, when intake is paused at a cap with confirmed progress, and on shutdown and revocation. Per-partition results decide which work is released.
- Shutdown stops intake, drains for up to 10 s, and commits confirmed progress.

## Errors

- Default is FAIL. A query uses either `ON_ERROR = FAIL|SKIP` or the specific policies `ON_MISSING_TIMESTAMP` and `ON_REJECTED_TIMESTAMP`, never both.
- SKIP drops a failed record with an identifiable source position and logs position and reason. Skippable classes: missing required timestamp, timestamp outside the destination's acceptance bounds, synchronous per-record size rejection, and other supported validation failures attributed to one record. Specific policies cover their named classes; `ON_ERROR = SKIP` covers all.
- Always FAIL, whatever the policy: unattributed batch rejections, fatal client errors, connectivity and authentication failures, out-of-range positions, framing/CRC corruption, changed source files. Only classified transient client and network failures retry; framing/CRC and deterministic authentication or authorization failures fail immediately.
- A source file has changed when its file ID differs, it is shorter than the checkpoint, or the last checkpointed frame's CRC differs; appends never count. The failure names old and new identity; the remedy is a new `GROUP`.
- A source record without a timestamp, when no filter needs one, gets the producer's timestamp at a topic destination; this is counted and warned about in a summary.

## Files

- Endpoints are `FILE cbor('path')`. Any `FILE` endpoint means one process per query on one machine, so Kafka→file runs one worker; locks cannot enforce this across machines or copied files, and such use is unsupported.
- An output has one writer, holding an exclusive lock on its inode from creation. A file-source group holds an exclusive lock on a stable checkpoint lock file that checkpoint replacement never swaps out; source data is not locked, so other groups can read a growing file.
- Format: a fixed magic, a header frame, then record frames. A frame is a big-endian u32 payload length, a CBOR payload, and CRC32C over both. The header map holds version, file ID, writing `GROUP`, and fingerprint, and is read with its own bound; an invalid or incomplete published header is fatal. Read size is limited by configuration; no compression or segmentation. The format may change until the first release, then is frozen and specified exactly; later readers accept every released version.
- Record map: `key`, `value`, ordered `headers` (`name`, `value`), `timestamp_ms`, `timestamp_type` (`create_time`, `log_append_time`, `unknown`), and optional `source` (`topic`, `partition`, `offset`) for diagnostics only. Byte fields keep null versus empty.
- Output is fsynced before records are acknowledged. Progress stores the acknowledged extent E: the file length after the fsync that confirmed the committed records, initially the header end; Kafka→file uses the largest committed E across partitions. On restart, progress is read and validated first; an output shorter than E fails, and so does any framing or CRC error in a frame starting before E. From E on, valid complete frames are kept; the first invalid or partial frame and everything after it are truncated, fsynced, and the removed byte count logged. Missing or invalid identity never licenses guessing E or deleting data. Why: only bytes past E are known to be unacknowledged.
- An existing output is appended to only when its header fingerprint matches and its file ID, read from the header and never regenerated, equals the stored destination ID. If the group stores no destination identity, an existing output fails, including a header-only file left by a crash during creation; deleting it is the remedy. A partition missing its own ID adopts the group's. An output missing while progress names it fails and is never recreated.
- The output is locked and created or validated before any source is read (for Kafka→file, before subscribing). Creation: generate an ID, write and fsync a complete header to a locked temporary file, publish it without replacing an existing path (a creator that loses opens and locks the published file), fsync the directory, persist initial source progress (every partition's initial position, checking each result, or the initial checkpoint) with the destination ID and initial E, then write records. Cleanup deletes only temporary files whose lock it acquires.
- Checkpoints live in `~/.kafka-fwd2/checkpoints` (`worker.checkpoint_dir`), named safely from `GROUP`, and replaced atomically. The parent directory is fsynced after creating any output or checkpoint file and after every checkpoint rename.

## Configuration and CLI

- Aliases come from TOML profiles or `WITH CONNECTION`; inline properties override TOML keys. `ENV('NAME')` resolves at startup, fails if unset, and is redacted. Unknown or guarantee-breaking librdkafka settings are rejected.
- TOML holds connection profiles and worker tuning: caps, delivery timeout, `idle_exit`, `tip_wait`, `checkpoint_dir`.
- CLI: exactly one of `run --file/-f path`, `run --sql/-e statement`, or `run --file -`. `--config/-c path` loads TOML.
- Exit codes: 0 for completion or a follow run stopped cleanly by a signal. 1 for runtime failures (including busy locks, transient and broker failures, and record-policy failures) and a bounded run interrupted before completion. 2 for usage, parse, plan, or configuration errors, and for conflicts found before copying that need operator action (fingerprint, topic ID, or window mismatch; foreign commit or member; self-copy; an output that is unowned, shorter than E, or damaged before it; a changed source file). Skips do not change the code; logs carry the count.

## Engineering

- Single-purpose structs behind narrow interfaces. Fx wires replaceable components at the composition root; core logic is Fx-free.
- `confluent-kafka-go` for Kafka I/O. Supported broker and client versions are a recorded Stage 0 result; known feature floors are Kafka 3.6 for timestamp acceptance bounds, Kafka 2.8 for topic IDs, and client 2.3.0 for `DescribeTopics`.
- Tests at the cheapest useful layer: unit, mock, component, end-to-end, load, and targeted edge cases.
- Keep code simple; lines of code and cyclomatic complexity are signals, not targets.
- Priorities: correctness, simplicity, throughput, CI duration, binary size.
- User documentation is example-based and under 10,000 characters.
