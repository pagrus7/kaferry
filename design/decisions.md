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
- With an upper bound, each Kafka partition stops at the first timestamp `> hi + cutoff`, or at the read-committed tip once wall time passes `hi + cutoff` (`CUTOFF`, default `INTERVAL '1' MINUTE`). Out-of-order records past the stop are omitted. Why: Kafka has no end-of-window marker, and a per-partition rule needs no coordination.
- A timestamp stop commits at the trigger record so a new owner re-evaluates it; a tip stop commits the next offset. Without an upper bound, Kafka follows.
- File sources start at their checkpoint or first frame and read to EOF, defined as the size at open; a partial frame there ends input, and later appends wait for another run. `START` and `CUTOFF` are rejected for file sources. Why: files interleave source partitions, so a cutoff would drop slower partitions.

## Groups and workers

- `GROUP` is required: a Kafka consumer group for Kafka sources, a local checkpoint identity for file sources. `GROUP = 'name'` is shared by workers; `GROUP = ONE_OFF` generates and logs an ID, runs one worker, and resumes through the named form.
- Kafka workers share a group with no coordinator and progress independently per assigned partition. Why: nothing extra to run or fail; group membership already divides partitions.
- A worker exits when all its assigned partitions have stopped; an empty assignment during a rebalance is not completion. In bounded runs, a worker holding no partitions for `worker.idle_exit` (default 30 s) exits; in follow mode it stays as a standby.
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
- Ordering: within one uninterrupted producer session with stable routing, a source partition's records arrive in submission order. Crash replay repeats ordered runs (100, 101, 100, 101), and a request already sent before a revocation can land late. Nothing is promised across source partitions. `LogAppendTime` destinations restamp records.
- Retention risk applies to CreateTime destinations with a delete policy and finite `retention.ms`. The horizon is current wall time minus `retention.ms`, evaluated at preflight and per batch, never from the filter's `NOW()`. A lower bound older than the horizon fails before copying unless SKIP applies; older records go through `ON_REJECTED_TIMESTAMP`. Retention risks are logged and counted apart from broker rejections. Unreadable topic configuration produces a warning and no check. This is a conservative check, not a retention guarantee.

## Delivery and progress

- Read committed source records. Output is acknowledged (delivery report or fsync) before source progress is committed, so crashes can duplicate but not lose records.
- Submit bounded, non-atomic batches. Commit only each partition's contiguous resolved prefix (filtered, delivered, or skipped), tracked by consumed positions rather than offset arithmetic.
- Uncommitted work is capped at 5,000 records / 16 MiB per partition and 20,000 records / 128 MiB per worker; one oversized record may proceed alone; intake pauses at a cap until a commit succeeds. Record caps bound crash duplicates and byte caps bound memory; the defaults are provisional until measured for both throughput and duplicate exposure.
- Each worker commits all its partitions in one request, at most one in flight: every second, at 50% of a worker cap, and on shutdown and revocation. Per-partition results decide which work is released.
- Shutdown stops intake, drains for up to 10 s, and commits confirmed progress.

## Errors

- Default is FAIL. A query uses either `ON_ERROR = FAIL|SKIP` or the specific policies `ON_MISSING_TIMESTAMP = FAIL|SKIP`, `ON_REJECTED_TIMESTAMP = FAIL|SKIP`, and `ON_SOURCE_FILE_CHANGED = FAIL|CONTINUE`; mixing them is a validation error.
- SKIP skips a failed record with an identifiable source position and logs position and reason. Authentication failures, outages, and unreadable framing still retry and fail.
- A source file has changed when its file ID differs, it is shorter than the checkpoint, or the last checkpointed frame's CRC differs; appends never count. CONTINUE resumes at the saved frame position and logs old and new identity.

## Files

- Endpoints are `FILE cbor('path')`. One process per file source or output, each locked exclusively.
- Header: format marker, version, file ID, writing `GROUP`, fingerprint. Frames: big-endian u32 payload length, CBOR payload, CRC32C over length and payload. Configurable read-size limit; no compression or segmentation.
- Record map: `key`, `value`, ordered `headers` (`name`, `value`), `timestamp_ms`, `timestamp_type` (`create_time`, `log_append_time`, `unknown`), and optional `source` (`topic`, `partition`, `offset`) for diagnostics only, never routing. Byte fields keep null versus empty. The format is provisional.
- Output is fsynced before records are acknowledged. On restart, truncate an incomplete final frame; fail on corruption. An existing output is appended to only when its header fingerprint matches and its file ID equals the stored destination file ID; the ID is read from the header, never regenerated.
- Checkpoints live in `~/.kafka-fwd2/checkpoints` (TOML `worker.checkpoint_dir`), named safely from `GROUP`, and replaced atomically. Fsync the parent directory after creating any output or checkpoint file and after every checkpoint rename.
- Reject a file destination that is its source, including path aliases.

## SQL, configuration, CLI

- Exactly one statement; case-insensitive keywords, case-sensitive connection aliases. Reject unknown, repeated, conflicting, or inapplicable options.
- `TOPIC 'topic@alias'`; without `@alias` the connection `default` is used and must exist. Aliases come from TOML profiles or `WITH CONNECTION alias AS (...)`, comma-separated; inline properties override TOML keys. `ENV('NAME')` resolves at startup, fails if unset, and is redacted. Reject unknown or guarantee-breaking librdkafka settings.
- TOML holds connection profiles and worker tuning: caps, delivery timeout, `idle_exit`, `checkpoint_dir`.
- CLI: `run --file/-f path`, `run --sql/-e statement`, or `run --file -`; exactly one input. `--config/-c path` loads TOML.
- Exit codes: 0 for completion or a follow run stopped cleanly by a signal; 1 for runtime failure or a bounded run interrupted before completion; 2 for usage, parse, plan, or configuration errors. Skips do not change the code; logs carry the count.

## Engineering

- Single-purpose structs behind narrow interfaces. Wire replaceable components as Fx modules at the composition root; core logic is Fx-free.
- `confluent-kafka-go` for Kafka I/O.
- Test with unit, mock, component, end-to-end, load, and targeted edge-case coverage at the cheapest useful layer.
- Keep code simple; lines of code and cyclomatic complexity are signals, not targets.
- Priorities: correctness, simplicity, throughput, CI duration, binary size.
- User documentation is example-based and under 10,000 characters.
