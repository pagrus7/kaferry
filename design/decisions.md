# kafka-fwd2 design

## Scope

- Lightweight Go CLI; SQL-like copy command, no transformations.
- Support topic→topic, topic→file, file→topic, and file→file.

## Records

- Preserve raw key/value bytes, nulls, ordered headers (including duplicates), and Kafka record timestamp/type where possible.
- Keep payloads opaque; users ensure format and Schema Registry compatibility.

## Time

- Kafka record time governs filters through the column `RECORD_TIMESTAMP`. Allow `>=`, `>`, `<`, `<=`, and inclusive `BETWEEN`, with at most one lower and one upper bound joined by `AND`; no `OR` or `NOT`.
- Normalize every filter to a half-open millisecond range `[lo, hi)` and log it; reject sub-millisecond literals and empty ranges.
- Support `NOW()` and basic timestamp/interval arithmetic in `SECOND`, `MINUTE`, `HOUR`, and `DAY` (fixed 86,400 s; no months or years); evaluate `NOW()` once, when a group first stores its window. Later workers and restarts of that group reuse the stored window; a new window needs a new group.
- Spell time literals as `TIMESTAMP '2026-10-01T00:00:00Z'`; require RFC 3339 with `Z` or an explicit UTC offset.
- A Kafka group's usable commit with a matching query fingerprint wins. Otherwise a time filter starts at its lower-bound timestamp, or at the earliest retained offset if it has only an upper bound; commit the initial position before copying.
- Without a time filter, require `START = EARLIEST|LATEST` if no usable commit exists. Reject `START` with time filters; timestamp starts are expressed through `WHERE`. Allow follow mode.
- Bounded partitions stop at the first timestamp `> end + cutoff`, or at the visible tip after wall time passes `end + cutoff` (default cutoff: one minute).
- Out-of-order or late records may be omitted by this stop rule.
- Spell the cutoff as `CUTOFF = INTERVAL '1' MINUTE`. For Kafka sources an upper filter bound enables cutoff completion; without one, Kafka follows.
- File sources always read to EOF; upper bounds only filter, and `CUTOFF` on a file source is a validation error.

## Kafka output

- Send the original key with partition unset. Default destination producers to `partitioner=murmur2_random` (Java-compatible keyed routing) unless the connection sets a partitioner; log the choice.
- Do not copy source partition or offset; log destination partition-count changes and continue.
- Same-key routing depends on the destination partitioner and partition count; `LogAppendTime` destinations may restamp records.
- Destination topics must already exist; do not support auto-creation.
- For a destination that keeps CreateTime and deletes by time (`retention.ms` ≠ -1), compute the horizon `NOW() - retention.ms` from the stored window's `NOW()`. Fail at planning if the window's lower bound is older; during the copy, treat older records as destination-rejected timestamps. If topic configuration cannot be read, warn and skip the check.
- Read committed source records; acknowledge output before committing source offsets.
- Crashes between output and commit can duplicate records.
- Records with the same key from one source partition reach the destination in source order. Crashes and rebalances may repeat an ordered run but never place an older record after a newer one, except a produce request already sent when a revocation purge runs.

## Workers

- Require `GROUP` in SQL for every query: a broker consumer group for Kafka sources, a local checkpoint identity for file sources.
- `GROUP = 'name'` is stable and shared by Kafka workers; `GROUP = ONE_OFF` generates/logs a fresh ID, runs with one worker, and can resume via the named form. No `AUTO` mode.
- Kafka workers share a group without a coordinator and progress independently per assigned partition.
- Store a query fingerprint and the resolved `[lo, hi)` window in every Kafka commit's metadata and every file-source checkpoint. The fingerprint hashes the normalized query (endpoints, `WHERE` expression text, semantic options), not resolved values. A mismatched fingerprint fails; a partition without a commit adopts the window from any committed sibling.
- Workers stop assigned partitions locally and exit when all are stopped; rebalances may extend a run.
- In bounded runs, a worker that holds no partitions continuously for TOML `worker.idle_exit` (default 30 seconds) logs it and exits; in follow mode, idle workers stay as standbys.
- On a timestamp stop, commit at the trigger record so a new owner can reevaluate it; at a tip stop, commit the next offset.
- On revocation, stop consuming that partition immediately, drain writes for up to 10 seconds, purge and close that partition's producer, commit confirmed progress, then release it.
- Rebalances pick up new source partitions while workers run; later additions need another run.
- Do not seed from another consumer group's offsets.

## Delivery and limits

- Submit bounded batches; track each record's result and commit only each partition's contiguous confirmed prefix. Batches are not atomic.
- Kafka writes use one idempotent producer per assigned source partition, with normal asynchronous calls and delivery reports; librdkafka batches internally. Size per-producer queues so their total stays within worker caps. The Go batch producer cannot preserve headers/timestamps.
- Bound uncommitted work by records and bytes: defaults 5,000/16 MiB per partition and 20,000/128 MiB per worker; configurable, with one oversized record allowed alone. Defaults are provisional until the Stage 6 baseline.
- Each worker commits all its partitions in one request, with at most one commit in flight: every second, when worker uncommitted records or bytes reach 50% of a worker cap, and on clean shutdown/revocation. Pause a partition or worker at its cap until a commit succeeds.
- Retry destination outages for 60 seconds by default, configurable; then fail.

## Errors

- Fail by default. SQL may supply either `ON_ERROR` or specific error policies; mixing them is a validation error.
- Use flat options: `ON_ERROR = FAIL|SKIP`, `ON_MISSING_TIMESTAMP = FAIL|SKIP`, `ON_REJECTED_TIMESTAMP = FAIL|SKIP`, and `ON_SOURCE_FILE_CHANGED = FAIL|CONTINUE`.
- Global `SKIP` skips any failed record with an identifiable source position and logs its position/reason. Authentication, outages, and unreadable framing retain retry/fail behavior when no record can be skipped.
- Specific policies may skip a missing timestamp required by a filter or a destination-rejected timestamp (including one past destination retention); changed source files may explicitly continue from the saved frame position and log the mismatch.

## Files

- Spell file endpoints as `FILE cbor('path')`; the format name leaves room for more handlers later.
- Use one append-order file with provisional versioned length/CBOR/checksum frames, named CBOR fields, and source position metadata.
- Use a flat record map: `key`, `value`, ordered `headers` (`name`, `value`), `timestamp_ms`, `timestamp_type` (`create_time`, `log_append_time`, `unknown`), and optional `source` (`topic`, `partition`, `offset`); byte fields retain null/empty distinction.
- Preserve original source metadata through file copies; use it for diagnostics, never Kafka destination routing.
- File header holds format marker, version, file ID, the writing query's `GROUP`, and its query fingerprint; frames are big-endian 32-bit payload length, CBOR payload, then CRC32C over length/payload. Enforce a configurable read-size limit; no file compression initially.
- Use one process per file source or output; lock its file-source checkpoint exclusively.
- A file source's EOF is its size at open; records appended later wait for another run. A partial frame at that point ends input and is not corruption.
- File-source checkpoints store file ID, next frame position, and the CRC of the last consumed frame. A different file ID, a file shorter than the checkpoint, or a CRC mismatch counts as changed for `ON_SOURCE_FILE_CHANGED`; appends never do.
- Store file-source checkpoints in `~/.kafka-fwd2/checkpoints`, overridable via TOML `worker.checkpoint_dir`; derive filenames safely from `GROUP`, validate source/query identity, and store the next frame position atomically.
- Fsync file output before acknowledging its records; truncate an incomplete final frame on restart and fail on complete corruption.
- Existing output files fail by default; append only when the header's `GROUP` and query fingerprint match the current query.

## SQL and configuration

- Use `INSERT INTO TOPIC 'topic@conn' SELECT * FROM TOPIC 'topic@conn'`; `WHERE` filters `RECORD_TIMESTAMP`, `OPTIONS (...)` holds copy semantics.
- Parse case-insensitive keywords, single-quoted strings, SQL comments, and an optional final semicolon; accept exactly one statement.
- Reject unknown, repeated, conflicting, or inapplicable options and file queries that would overwrite their source.
- Reject topic queries whose source and destination name the same connection alias and topic. Different aliases for one cluster are not detected; documentation warns about copy loops.
- Prefix statements with `WITH CONNECTION alias AS (...)` for inline librdkafka properties; TOML supplies named profiles. SQL properties override matching TOML keys.
- `ENV('NAME')` resolves at startup, fails if unset, and is redacted in logs. Reject unknown or guarantee-breaking librdkafka settings.
- TOML holds connection profiles and worker tuning; file paths use `FILE cbor('path')`.
- CLI uses `run --file/-f path`, `run --sql/-e statement`, or `run --file -` for stdin; require exactly one input. Load TOML with `--config/-c path`.
- Exit 0 on completion or a clean signal stop, 1 on runtime failure, and 2 on usage, parse, plan, or configuration errors. Skipped records do not change the exit code; logs carry their count.

## Deferred

- CBOR may change; file segmentation can come later.

## Engineering

- Use single-purpose structs behind narrow interfaces. Wire replaceable components as Fx modules at the composition root; keep core logic Fx-free.
- Use `confluent-kafka-go` for Kafka I/O.
- Test with unit, mock, component, end-to-end, load, and targeted edge-case coverage at the cheapest useful layer.
- Keep code simple; review lines of code and cyclomatic complexity as signals, not targets.
- Prioritize correctness, simplicity, throughput, CI duration, then binary size.
- Keep user documentation example-based and under 10,000 characters.
