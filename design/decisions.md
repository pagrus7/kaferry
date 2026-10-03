# kafka-fwd2 design

## Scope

- Lightweight Go CLI that copies Kafka records with one SQL-like statement; no transformations.
- Directions: topic→topic, topic→file, file→topic, file→file. Syntax and examples: `grammar.md`.
- Preserve raw key/value bytes, null versus empty, ordered headers (duplicates included), and timestamp/type where the destination allows. Payloads are opaque; format and Schema Registry compatibility are the user's responsibility.

## Time window

- `WHERE` filters `RECORD_TIMESTAMP` (Kafka record time) with `>=`, `>`, `<`, `<=`, or inclusive `BETWEEN`: at most one lower and one upper bound, joined by `AND`. Every filter normalizes to a logged half-open millisecond range `[lo, hi)`; sub-millisecond literals and empty ranges are rejected.
- Literals are `TIMESTAMP '2026-10-01T00:00:00Z'` (RFC 3339 with `Z` or an explicit offset). Intervals use `SECOND`, `MINUTE`, `HOUR`, or `DAY` (86,400 s).
- `NOW()` resolves once per group, not per run. The stored window is reused by every worker and restart; a new window needs a new group.

## Start and completion

- A Kafka partition starts at its commit when the commit's fingerprint matches; otherwise at the lower bound's timestamp, or at the earliest retained offset for an upper-only filter. Without `WHERE`, `START = EARLIEST|LATEST` is required when no commit exists; `START` with `WHERE` is rejected. Initial positions are committed before copying.
- With an upper bound, each Kafka partition stops at the first timestamp `> hi + cutoff`, or at the read-committed tip once wall time passes `hi + cutoff` (`CUTOFF`, default `INTERVAL '1' MINUTE`). Out-of-order records past the stop are omitted, including everything after a single future-dated record. Why: Kafka has no end-of-window marker, and a per-partition rule needs no coordination.
- Every stop logs and counts its reason, partition, cursor, trigger timestamp, and distance past `hi`; conspicuously future-dated triggers are flagged.
- A tip stop samples the high watermark H once per attempt, after wall time passes `hi + cutoff` and the partition reaches its read-committed tip. It completes only when consumption has passed H (across aborted and control records) and all prior work is resolved. If that takes longer than `worker.tip_wait` (default 15 min), the worker fails naming the partition, H, and the stable and consumed positions. H is never chased, shared, or persisted; a new owner samples its own. Why: an open transaction holds the stable offset back while hiding records already written. The wait is a tool budget, not a transaction-timeout guarantee.
- A timestamp stop commits at the trigger record so a new owner re-evaluates it; a tip stop commits the next offset. A rerun re-triggers a timestamp stop but may extend a tip stop with records that arrived later. Without an upper bound, Kafka follows.
- Timestamp lookup returns the earliest offset whose timestamp is `>= lo`, so no in-window record precedes the start position.
- File sources start at their checkpoint or first frame and read to EOF, defined as the size at open; a partial frame there ends input, and later appends wait for another run. `START` and `CUTOFF` are rejected for file sources. Why: files interleave source partitions, so a cutoff would drop slower partitions.

## Groups and workers

- `GROUP` is required: a Kafka consumer group for Kafka sources, a local checkpoint identity for file sources. `GROUP = 'name'` is shared by workers; `GROUP = ONE_OFF` generates and logs an ID, runs one worker, and resumes through the named form.
- Kafka workers share a group with no coordinator and progress independently per assigned partition. Why: nothing extra to run or fail; group membership already divides partitions.
- Kafka sources use the classic group protocol with `cooperative-sticky` assignment, set explicitly and protected. Why: with eager rebalancing every worker exit revokes all partitions and drains every busy worker.
- A worker exits when all its assigned partitions have stopped; an empty assignment during a rebalance is not completion. In bounded runs, a worker holding no partitions for `worker.idle_exit` (default 30 s) exits 0, logged as a local idle exit; the timer starts only after a successful assignment (empty included), never while joining or failing to connect. In follow mode it stays as a standby.
- There is no group-wide done signal, and every worker exiting 0 does not prove coverage: an idle worker can leave while a partition is being handed to it. To confirm a bounded group, rerun the same statement and `GROUP` with one worker after previous members have left or expired. As the sole member it receives every discovered partition and finishes any orphaned work; its completion, never an idle exit, is the confirmation.
- On revocation, stop intake for the partition, drain for up to 10 s, commit its confirmed prefix while ownership holds, and release it. A drain timeout fails the worker; unresolved work replays.
- Rebalances pick up source partitions added while workers run. Never seed from another group's offsets.
- A successful `ONE_OFF` file-source run deletes its checkpoint. Kafka `ONE_OFF` groups expire through broker offset retention.

## Identity

- The query fingerprint hashes endpoint kinds, cluster ID + topic of each Kafka endpoint, the normalized `WHERE` (each bound as side, literal ms or `NOW()`, and offset in seconds), and `CUTOFF`. It excludes aliases, paths, file IDs, credentials and other connection properties, `GROUP`, `START`, error policies (so a failed run can resume with `SKIP`), and tuning.
- Kafka commit metadata is versioned: `kfwd2:1;fp=<sha256>` on every partition, the destination file ID for file outputs, and `win=<lo>,<hi>` (either side may be empty) on partition 0 for `NOW()` queries.
- For `NOW()` queries, only the current owner of source partition 0 resolves the window. It commits the window with partition 0's initial position, fenced by its assignment generation and never retried after losing ownership, and only then copies. Other workers keep polling with partitions paused until that window is readable and write nothing before it. A replacement owner reads stored metadata before resolving. Literal windows need no barrier.
- Fail on a fingerprint mismatch, on commits without kafka-fwd2 metadata, and on `NOW()` progress whose partition 0 window is missing. A stored window stays authoritative after its offset leaves source retention. When all of a group's offsets expire (`offsets.retention.minutes`), the group starts fresh.
- File instances are validated outside the fingerprint. File-source checkpoints store fingerprint, window, source file ID, last-frame CRC, next frame position, and the destination file ID for file outputs. Output headers store their own file ID and the fingerprint.

## Kafka output

- One idempotent producer per worker, with asynchronous calls and delivery reports; librdkafka batches internally (the Go batch producer drops headers/timestamps).
- Send the original key with partition unset, using `partitioner=murmur2_random` (Java-compatible) unless the connection sets one. Do not copy source partition or offset; key routing depends on destination partition count, and changes to it are logged.
- Destination topics must exist; no auto-creation. Reject a destination with the same cluster ID and topic as the source.
- Delivery retries belong to the client (`message.timeout.ms`, 60 s default), then the run fails. A queue-full refusal pauses intake and retries the same record first. Terminal record failures follow FAIL/SKIP and are never re-produced behind later records.
- Before produce, CreateTime records are checked against the destination's timestamp acceptance bounds (`message.timestamp.before.max.ms` / `after.max.ms`, or the legacy `difference.max.ms`) at current wall time. Records outside them go through `ON_REJECTED_TIMESTAMP` and are never sent. The check can race broker clocks and configuration; it does not prove acceptance.
- A broker rejection is skippable only when it is attributed to one record. A batch-level rejection without trustworthy per-record attribution, and any fatal producer error, fails the worker under every policy; innocent records of that batch are never marked skipped. No error-string parsing and no application re-produce to split a batch. Post-send attribution is used only if the real-client gate proves the Go delivery reports expose it. Why: the broker rejects the whole batch, so a batch error cannot be pinned on one record.
- Ordering: within one uninterrupted producer session with stable routing, a source partition's records arrive in submission order. Crash replay repeats ordered runs (100, 101, 100, 101), and a request already sent before a revocation can land late. Nothing is promised across source partitions. `LogAppendTime` destinations restamp records.
- Retention risk applies to CreateTime destinations with a delete policy and finite `retention.ms`. The horizon is current wall time minus `retention.ms`, evaluated at preflight and per batch, never from the filter's `NOW()`. A lower bound older than the horizon fails before copying unless SKIP applies; older records go through `ON_REJECTED_TIMESTAMP`. Retention risks are logged and counted apart from broker rejections. Unreadable topic configuration produces a warning and no check. This is a conservative check, not a retention guarantee.

## Delivery and progress

- Read committed source records. Output is acknowledged (delivery report or fsync) before source progress is committed, so crashes can duplicate but not lose records.
- Submit bounded, non-atomic batches. Commit only each partition's contiguous resolved prefix (filtered, delivered, or skipped), tracked by consumed positions rather than offset arithmetic.
- Uncommitted work is capped at 5,000 records / 16 MiB per partition and 20,000 records / 128 MiB per worker; one oversized record may proceed alone; intake pauses at a cap until a commit succeeds. Record caps bound crash duplicates and byte caps bound memory; the defaults are provisional until measured for both throughput and duplicate exposure.
- Each worker commits all its partitions in one request, at most one in flight: every second, at 50% of a worker cap, at 50% of any partition's record or byte cap, as soon as intake is paused at a cap with confirmed progress available, and on shutdown and revocation. Per-partition results decide which work is released. Why: a partition cap below half the worker cap would otherwise leave a single hot partition waiting on the timer.
- Shutdown stops intake, drains for up to 10 s, and commits confirmed progress.

## Errors

- Default is FAIL. A query uses either `ON_ERROR = FAIL|SKIP` or the specific policies `ON_MISSING_TIMESTAMP = FAIL|SKIP`, `ON_REJECTED_TIMESTAMP = FAIL|SKIP`, and `ON_SOURCE_FILE_CHANGED = FAIL|CONTINUE`; mixing them is a validation error.
- SKIP skips a failed record with an identifiable source position and logs position and reason. Skippable record classes: missing required timestamp, timestamp outside the destination's acceptance bounds or retention risk (both checked before produce), synchronous per-record producer size rejection, and other explicitly supported record validation failures attributed to one record. Specific policies cover only their named classes; `ON_ERROR = SKIP` also covers the other supported classes.
- Always FAIL: ambiguous batch rejections, fatal client errors, connectivity and authentication failures, framing/CRC corruption. Only classified transient client and network failures retry; framing/CRC and deterministic authentication or authorization failures fail immediately.
- A source record without a timestamp, when no filter needs one, gets the producer's timestamp at a topic destination. It is counted and warned about in a summary, not per record.
- A source file has changed when its file ID differs, it is shorter than the checkpoint, or the last checkpointed frame's CRC differs; appends never count. CONTINUE resumes at the saved frame position and logs old and new identity; a position that is not a valid frame boundary fails immediately.

## Files

- Endpoints are `FILE cbor('path')`. An output has one writer, holding an exclusive lock on the data file. A file-source group holds an exclusive lock on a stable checkpoint lock file that atomic checkpoint replacement never swaps out; the source data is not locked, so independent groups can read a file while it grows. Local locks give no cross-machine file groups; those are unsupported.
- Header: format marker, version, file ID, writing `GROUP`, fingerprint. Frames: big-endian u32 payload length, CBOR payload, CRC32C over length and payload. Configurable read-size limit; no compression or segmentation.
- Record map: `key`, `value`, ordered `headers` (`name`, `value`), `timestamp_ms`, `timestamp_type` (`create_time`, `log_append_time`, `unknown`), and optional `source` (`topic`, `partition`, `offset`) for diagnostics only, never routing. Byte fields keep null versus empty. The format is provisional.
- Output is fsynced before records are acknowledged. On restart, truncate an incomplete final frame; fail on corruption. An existing output is appended to only when its header fingerprint matches and its file ID equals the stored destination file ID; the ID is read from the header, never regenerated.
- A new output is initialized under the writer lock: generate an ID, write and fsync a complete header to a temporary file, publish it without replacing an existing path, fsync the directory, persist initial source progress (and, for file sources, the initial checkpoint) with the destination ID, then write records. Temporary leftovers are cleaned under the lock.
- An existing output with no stored destination identity anywhere in the group is adopted only if its header (format, version, fingerprint, writing `GROUP`) is valid and no bytes follow it; otherwise fail. A partition missing its own ID adopts the group's established ID. An output that is missing while progress names it fails; it is never recreated.
- Checkpoints live in `~/.kafka-fwd2/checkpoints` (TOML `worker.checkpoint_dir`), named safely from `GROUP`, and replaced atomically. Fsync the parent directory after creating any output or checkpoint file and after every checkpoint rename.
- Reject a file destination that is its source, including path aliases.

## SQL, configuration, CLI

- Exactly one statement; case-insensitive keywords, case-sensitive connection aliases. Reject unknown, repeated, conflicting, or inapplicable options.
- `TOPIC 'topic@alias'`; without `@alias` the connection `default` is used and must exist. Aliases come from TOML profiles or `WITH CONNECTION alias AS (...)`, comma-separated; inline properties override TOML keys. `ENV('NAME')` resolves at startup, fails if unset, and is redacted. Reject unknown or guarantee-breaking librdkafka settings.
- TOML holds connection profiles and worker tuning: caps, delivery timeout, `idle_exit`, `tip_wait`, `checkpoint_dir`.
- CLI: `run --file/-f path`, `run --sql/-e statement`, or `run --file -`; exactly one input. `--config/-c path` loads TOML.
- Exit codes: 0 for completion or a follow run stopped cleanly by a signal; 1 for runtime failure or a bounded run interrupted before completion; 2 for usage, parse, plan, or configuration errors. Skips do not change the code; logs carry the count.

## Engineering

- Single-purpose structs behind narrow interfaces. Wire replaceable components as Fx modules at the composition root; core logic is Fx-free.
- `confluent-kafka-go` for Kafka I/O.
- Test with unit, mock, component, end-to-end, load, and targeted edge-case coverage at the cheapest useful layer.
- Keep code simple; lines of code and cyclomatic complexity are signals, not targets.
- Priorities: correctness, simplicity, throughput, CI duration, binary size.
- User documentation is example-based and under 10,000 characters.
