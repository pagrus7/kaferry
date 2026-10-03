# kafka-fwd2 design

## Scope

- Lightweight Go CLI that copies Kafka records with one SQL-like statement; no transformations. Directions: topic→topic, topic→file, file→topic, file→file. Syntax: `grammar.md`.
- Raw key/value bytes, null versus empty, ordered headers (duplicates included), and timestamp/type are preserved where the destination allows. Payloads are opaque; format and Schema Registry compatibility are the user's responsibility.
- Delivery is at least once: crashes can duplicate records but never lose acknowledged ones.

## Time window

- `WHERE` filters `RECORD_TIMESTAMP` (Kafka record time) with `>=`, `>`, `<`, `<=`, or inclusive `BETWEEN`: at most one lower and one upper bound, joined by `AND`. Every filter normalizes to a logged half-open millisecond range `[lo, hi)`; sub-millisecond literals and empty ranges are rejected.
- Literals are `TIMESTAMP '2026-10-01T00:00:00Z'` (RFC 3339 with `Z` or an explicit offset). Intervals use `SECOND`, `MINUTE`, `HOUR`, or `DAY` (86,400 s).
- `NOW()` resolves once per group, not per run; every worker and restart reuses the stored window, so a new window needs a new group. A group whose Kafka offsets have all expired (`offsets.retention.minutes`) or whose checkpoint is gone is new: a rerun resolves a fresh window, and a literal window recopies retained records. Startup logs whether progress exists and the resolved window.

## Kafka start positions

- A partition starts at its commit. With no commit it starts at the lower bound by timestamp lookup (the earliest offset with timestamp `>= lo`, so no in-window record precedes it), at the earliest retained offset for an upper-only filter, or, without `WHERE`, at `START = EARLIEST|LATEST`, which is then required. `START` with `WHERE` is rejected. Initial positions are committed before copying.
- A lookup with no match starts at the read-committed end (the LSO, never the high watermark). Why: a matching record in an open transaction must not be skipped.
- `auto.offset.reset=error` is protected; the tool positions every partition explicitly.
- A commit below the log start resumes at the log start whatever `START` says, then filters as usual; the log records the old position, the log start, and the offset gap (not a lost-record count). Why: every retained record after an expired commit is unread, and `LATEST` would skip them.
- A commit above the log end fails as truncation. An out-of-range position during a run fails naming the positions; a restart applies the startup rules.
- Only retained records are copied. When the log start is above 0 and the first retained record is newer than `lo`, the run warns that the window may begin before retained data; this never fails the run.

## Completion

- With an upper bound, a Kafka partition stops at the first timestamp `> hi + cutoff` (timestamp stop), or at the read-committed tip once wall time passes `hi + cutoff` (tip stop); `CUTOFF` defaults to `INTERVAL '1' MINUTE`. Records past the stop are omitted, including everything after a single future-dated record. Why: Kafka has no end-of-window marker, and a per-partition rule needs no coordination.
- Every stop logs and counts its reason, partition, cursor, trigger timestamp, and distance past `hi`; conspicuously future-dated triggers are flagged.
- A tip stop samples the high watermark H once per attempt, after wall time passes `hi + cutoff` and the partition reaches its read-committed tip. It completes when the consumed cursor reaches H (past aborted and control records; H = 0 completes at once) and all prior work is resolved; it never waits for a later visible record. Exceeding `worker.tip_wait` (default 15 min) fails the worker naming the partition, H, and the stable and consumed positions. H is never shared or persisted; a new owner samples its own. Why: an open transaction holds the LSO back while hiding records already written.
- A timestamp stop commits at the trigger record, so a rerun stops there again. A tip stop commits the next offset, so a rerun can copy records that arrived later. Without an upper bound, Kafka sources follow.
- File sources start at their checkpoint or first frame and read to EOF, the size at open; a partial frame there ends input, and later appends wait for another run. `START` and `CUTOFF` are rejected for file sources. Why: files interleave source partitions, so a cutoff would drop slower partitions.

## Groups and workers

- `GROUP` is required: a Kafka consumer group for Kafka sources, a local checkpoint identity for file sources. `GROUP = 'name'` is shared by workers. `GROUP = ONE_OFF` generates and logs an ID, runs one worker, and resumes through the named form; a successful file-source `ONE_OFF` run deletes its checkpoint, and Kafka `ONE_OFF` groups expire through broker offset retention.
- Kafka workers share a group with no coordinator and progress independently per assigned partition, using the classic group protocol with protected `cooperative-sticky` assignment. Why: nothing extra to run, and eager rebalancing would drain every busy worker on each exit.
- A worker exits when all its assigned partitions have stopped; an empty assignment during a rebalance is not completion. In bounded runs a worker holding no partitions for `worker.idle_exit` (default 30 s) exits 0, logged as an idle exit; the timer starts only after a successful assignment (empty included). In follow mode it stays as a standby.
- There is no group-wide done signal, and every worker exiting 0 does not prove coverage: an idle worker can leave while a partition is being handed to it. To confirm a bounded group, rerun the same statement and `GROUP` with one worker after previous members have left. As sole member it receives every partition and finishes orphaned work; its completion, never an idle exit, is the confirmation. It is a real copy run: tip-stopped partitions may copy late arrivals.
- On revocation, a worker stops intake for the partition, waits up to 10 s for delivery outcomes of that partition's submitted records (not a producer-wide flush), commits its confirmed prefix while ownership holds, and releases it. A drain timeout fails the worker; unresolved work replays. With a sink too slow to drain in time, membership changes can fail busy workers in cascade.
- Rebalances pick up source partitions added during a run. A group never seeds from another group's offsets.

## Identity

- The query fingerprint hashes endpoint kinds, cluster ID + topic of each Kafka endpoint, the normalized `WHERE` (each bound as side, literal ms or `NOW()`, and time adjustment in milliseconds, including the 1 ms normalization, with checked arithmetic), and `CUTOFF`. It excludes aliases, paths, file IDs, connection properties, `GROUP`, `START`, error policies (so a failed run can resume with `SKIP`), and tuning.
- Kafka commit metadata is versioned `kfwd2:1`: `fp=<sha256>` and the source topic ID `tid` on every partition; the destination file ID and acknowledged output extent for file outputs; `win=<lo>,<hi>` (either side may be empty) on partition 0 for `NOW()` queries.
- Startup fails on a fingerprint mismatch, a commit without kafka-fwd2 metadata, `NOW()` progress without its partition 0 window, and a `tid` that differs from the topic's current ID, naming both IDs. Topic IDs come from `AdminClient.DescribeTopics`; a missing or zero ID never bypasses the check. This detects recreation between runs, not deletion during one. Destination recreation is not detected. A stored window stays authoritative after its offset leaves retention.
- Partition 0 initializes shared metadata: the `NOW()` window and, for Kafka→file, the destination file ID and initial extent. Only its current owner resolves them, commits them with partition 0's initial position, then copies; a replacement owner reads stored metadata first. Broker generation checks do not prove ownership, so the worker fences itself: partition 0 commits are serialized against local ownership, initialization retries stop at revocation, nothing for partition 0 is submitted after unassignment, and revocation waits for an in-flight initialization commit, failing and closing the worker if it cannot resolve.
- Other partitions stay paused and write nothing until that metadata is readable, up to `worker.tip_wait` from a fixed deadline that retries and rebalances do not extend; then the worker fails naming the group, partition 0, elapsed time, and the missing metadata. They carry the established file ID; conflicting IDs fail. Literal windows with topic outputs need no barrier.
- File identity is validated outside the fingerprint. File-source checkpoints store fingerprint, window, source file ID, last-frame CRC, next frame position, and, for file outputs, the destination file ID and acknowledged extent. Output headers store their own file ID and the fingerprint.

## Kafka output

- One idempotent producer per worker, asynchronous with delivery reports; librdkafka batches internally (the Go batch producer drops headers and timestamps).
- The original key is sent with partition unset, using `partitioner=murmur2_random` (Java-compatible) unless the connection sets one. Source partition and offset are not copied; destination partition-count changes are logged.
- Destination topics must exist. A destination with the source's cluster ID and topic is rejected.
- Delivery retries belong to the client (`message.timeout.ms`, default 60 s), then the run fails. A queue-full refusal pauses intake and retries the same record first. Terminal record failures follow FAIL/SKIP and are never re-produced behind later records.
- Before produce, CreateTime records are checked at wall time against the destination's timestamp acceptance bounds (`message.timestamp.before.max.ms` / `after.max.ms`, or legacy `difference.max.ms`); records outside them go through `ON_REJECTED_TIMESTAMP` and are never sent. The check can race broker clocks and configuration.
- Retention risk applies to CreateTime destinations with a delete policy and finite `retention.ms`. The horizon is wall time minus `retention.ms`, checked at preflight and per batch, never from `NOW()`. A lower bound older than the horizon fails before copying unless SKIP applies; without a lower bound there is no preflight. Older records go through `ON_REJECTED_TIMESTAMP`, counted apart from broker rejections. Unreadable topic configuration warns and skips the check.
- A broker rejection is skippable only when its delivery report attributes it to one record. Other batch rejections and fatal producer errors fail the worker under every policy; no innocent record is marked skipped, error strings are not parsed, and batches are not split by re-producing. Why: the broker rejects the whole batch.
- Ordering: within one uninterrupted producer session with stable routing, records from one source partition submitted to the same destination partition keep submission order. A stable non-null key routes consistently while the partitioner and destination partition count are unchanged. There is no order across destination partitions, null-key records included, or across source partitions. Crash replay repeats runs (100, 101, 100, 101), and a request sent before a revocation can land late. `LogAppendTime` destinations restamp records.

## Delivery and progress

- Source records are read committed. Output is acknowledged (delivery report or fsync) before source progress is committed.
- Batches are bounded and non-atomic. Each partition commits only its contiguous resolved prefix (filtered, delivered, or skipped), tracked by consumed positions rather than offset arithmetic.
- No copy is all-or-nothing: a later failure leaves earlier acknowledged output in place, and a restart revisits the failing record until the policy or conditions change.
- Uncommitted work is capped at 5,000 records / 16 MiB per partition and 20,000 records / 128 MiB per worker; one oversized record may proceed alone; intake pauses at a cap until a commit succeeds. Record caps bound crash duplicates and byte caps bound memory; load tests set the defaults together with protected producer queue limits.
- Each worker commits all its partitions in one request, at most one in flight: every second, at 50% of a worker cap or of any partition's cap, when intake is paused at a cap with confirmed progress, and on shutdown and revocation. Per-partition results decide which work is released.
- Shutdown stops intake, drains for up to 10 s, and commits confirmed progress.

## Errors

- Default is FAIL. A query uses either `ON_ERROR = FAIL|SKIP` or the specific policies `ON_MISSING_TIMESTAMP = FAIL|SKIP`, `ON_REJECTED_TIMESTAMP = FAIL|SKIP`, and `ON_SOURCE_FILE_CHANGED = FAIL|CONTINUE`, never both. `ON_ERROR` governs only skippable record failures, never source-file changes, cursor gaps, framing, or infrastructure; with it, a changed source fails.
- SKIP drops a failed record with an identifiable source position and logs position and reason. Skippable classes: missing required timestamp, timestamp outside the destination's acceptance bounds or at retention risk, synchronous per-record size rejection, and other supported validation failures attributed to one record. Specific policies cover their named classes; `ON_ERROR = SKIP` covers all.
- Always FAIL: unattributed batch rejections, fatal client errors, connectivity and authentication failures, framing/CRC corruption. Only classified transient client and network failures retry; framing/CRC and deterministic authentication or authorization failures fail immediately.
- A source record without a timestamp, when no filter needs one, gets the producer's timestamp at a topic destination; this is counted and warned about in a summary.
- A source file has changed when its file ID differs, it is shorter than the checkpoint, or the last checkpointed frame's CRC differs; appends never count. CONTINUE resumes at the saved frame position and logs old and new identity; a position that is not a frame boundary fails.

## Files

- Endpoints are `FILE cbor('path')`. An output has one writer, holding an exclusive lock on the data file. A file-source group holds an exclusive lock on a stable checkpoint lock file that checkpoint replacement never swaps out; source data is not locked, so other groups can read a growing file. Any `FILE` endpoint means one process per query on one machine; locks cannot enforce this across machines or copied files, and such use is unsupported.
- Format: a fixed magic, a header frame, then record frames. A frame is a big-endian u32 payload length, a CBOR payload, and CRC32C over both. The header map holds version, file ID, writing `GROUP`, and fingerprint, and is read with its own bound; an invalid or incomplete published header is fatal. Read size is limited by configuration; no compression or segmentation. The format is provisional.
- Record map: `key`, `value`, ordered `headers` (`name`, `value`), `timestamp_ms`, `timestamp_type` (`create_time`, `log_append_time`, `unknown`), and optional `source` (`topic`, `partition`, `offset`) for diagnostics only. Byte fields keep null versus empty.
- Output is fsynced before records are acknowledged. Progress stores the acknowledged extent E: the file length after the fsync that confirmed the committed records, initially the header end; Kafka→file uses the largest committed E across partitions. On restart, an output shorter than E fails, and so does any framing or CRC error in a frame starting before E. From E on, valid complete frames are kept; the first invalid or partial frame and everything after it are truncated, fsynced, and the removed byte count logged. Missing or invalid identity never licenses guessing E or deleting data. Why: only bytes past E are known to be unacknowledged.
- An existing output is appended to only when its header fingerprint matches and its file ID, read from the header and never regenerated, equals the stored destination ID. With no stored destination identity anywhere in the group, it is adopted only if its header (format, version, fingerprint, writing `GROUP`) is valid and nothing follows it. A partition missing its own ID adopts the group's. An output missing while progress names it fails and is never recreated.
- A new output is created under the writer lock: generate an ID, write and fsync a complete header to a temporary file, publish it without replacing an existing path, fsync the directory, persist initial source progress (and the initial checkpoint for file sources) with the destination ID and initial E, then write records. Temporary leftovers are cleaned under the lock.
- Checkpoints live in `~/.kafka-fwd2/checkpoints` (`worker.checkpoint_dir`), named safely from `GROUP`, and replaced atomically. The parent directory is fsynced after creating any output or checkpoint file and after every checkpoint rename.
- A file destination that is its source, including path aliases, is rejected.

## SQL, configuration, CLI

- Exactly one statement; case-insensitive keywords, case-sensitive connection aliases. Unknown, repeated, conflicting, or inapplicable options are rejected.
- `TOPIC 'topic@alias'`; without `@alias` the connection `default` is used and must exist. Aliases come from TOML profiles or `WITH CONNECTION alias AS (...)`, comma-separated; inline properties override TOML keys. `ENV('NAME')` resolves at startup, fails if unset, and is redacted. Unknown or guarantee-breaking librdkafka settings are rejected.
- TOML holds connection profiles and worker tuning: caps, delivery timeout, `idle_exit`, `tip_wait`, `checkpoint_dir`.
- CLI: exactly one of `run --file/-f path`, `run --sql/-e statement`, or `run --file -`. `--config/-c path` loads TOML.
- Exit codes: 0 for completion or a follow run stopped cleanly by a signal; 1 for runtime failure or a bounded run interrupted before completion; 2 for usage, parse, plan, or configuration errors. Skips do not change the code; logs carry the count.

## Engineering

- Single-purpose structs behind narrow interfaces. Fx wires replaceable components at the composition root; core logic is Fx-free.
- `confluent-kafka-go` for Kafka I/O. Supported broker and client versions are a recorded Stage 0 result; known feature floors are Kafka 3.6 for timestamp acceptance bounds, Kafka 2.8 for topic IDs, and client 2.3.0 for `DescribeTopics`.
- Tests at the cheapest useful layer: unit, mock, component, end-to-end, load, and targeted edge cases.
- Keep code simple; lines of code and cyclomatic complexity are signals, not targets.
- Priorities: correctness, simplicity, throughput, CI duration, binary size.
- User documentation is example-based and under 10,000 characters.
