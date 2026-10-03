# kafka-fwd2 design

## Scope

- Lightweight Go CLI; SQL-like copy command, no transformations.
- Support topic→topic, topic→file, file→topic, and file→file.

## Records

- Preserve raw key/value bytes, nulls, ordered headers (including duplicates), and Kafka record timestamp/type where possible.
- Keep payloads opaque; users ensure format and Schema Registry compatibility.

## Time

- Kafka record time governs filters; start is inclusive, end exclusive.
- Spell time literals as `TIMESTAMP '2026-10-01T00:00:00Z'`; require RFC 3339 with `Z` or an explicit UTC offset in filters and `START`.
- A Kafka group's commit wins; if absent, use explicit earliest/latest/timestamp start or a time-range start, then commit that start. Otherwise fail. Allow follow mode.
- Bounded partitions stop at the first timestamp `> end + cutoff`, or at the visible tip after wall time passes `end + cutoff` (default cutoff: one minute).
- Out-of-order or late records may be omitted by this stop rule.

## Kafka output

- Send the original key with partition unset; configure key-aware routing.
- Do not copy source partition or offset; log destination partition-count changes and continue.
- Same-key routing depends on the destination partitioner and partition count; `LogAppendTime` destinations may restamp records.
- Destination topics must already exist; do not support auto-creation.
- Read committed source records; acknowledge output before committing source offsets.
- Crashes between output and commit can duplicate records.

## Workers

- Require `GROUP` in SQL for every query: a broker consumer group for Kafka sources, a local checkpoint identity for file sources.
- `GROUP = 'name'` is stable and shared by Kafka workers; `GROUP = ONE_OFF` generates/logs a fresh ID, runs with one worker, and can resume via the named form. No `AUTO` mode.
- Kafka workers share a group without a coordinator and progress independently per assigned partition.
- Workers stop assigned partitions locally and exit when all are stopped; rebalances may extend a run.
- On a timestamp stop, commit at the trigger record so a new owner can reevaluate it; at a tip stop, commit the next offset.
- On revocation, stop consuming that partition immediately, drain writes for up to 10 seconds, commit confirmed progress, then release it.
- Rebalances pick up new source partitions while workers run; later additions need another run.
- Do not seed from another consumer group's offsets.

## Delivery and limits

- Submit bounded batches; track each record's result and commit only each partition's contiguous confirmed prefix. Batches are not atomic.
- Kafka writes use normal asynchronous producer calls and delivery reports; librdkafka batches internally. The Go batch producer cannot preserve headers/timestamps.
- Bound uncommitted work by records and bytes: defaults 250/16 MiB per partition and 2,000/128 MiB per worker; configurable, with one oversized record allowed alone.
- Commit every second or at 25% of the cap, and on clean shutdown/revocation. Pause at the cap until a commit succeeds.
- Retry destination outages for 60 seconds by default, configurable; then fail.

## Errors

- Fail by default. SQL may supply either `ON_ERROR` or specific error policies; mixing them is a validation error.
- Use flat options: `ON_ERROR = FAIL|SKIP`, `ON_MISSING_TIMESTAMP = FAIL|SKIP`, `ON_REJECTED_TIMESTAMP = FAIL|SKIP`, and `ON_SOURCE_FILE_CHANGED = FAIL|CONTINUE`.
- Global `SKIP` skips any failed record with an identifiable source position and logs its position/reason. Authentication, outages, and unreadable framing retain retry/fail behavior when no record can be skipped.
- Specific policies may skip a missing timestamp required by a filter or a destination-rejected timestamp; changed source files may explicitly continue from the saved frame position and log the mismatch.

## Files

- Spell file endpoints as `FILE cbor('path')`; the format name leaves room for more handlers later.
- Use one append-order file with provisional versioned length/CBOR/checksum frames, named CBOR fields, and source position metadata.
- Use one process per file source or output; lock its file-source checkpoint exclusively.
- Derive the file-source checkpoint filename from `GROUP` in the OS user state folder, with a TOML override; validate source/query identity and store the next frame position atomically.
- Fsync file output before acknowledging its records; truncate an incomplete final frame on restart and fail on complete corruption.
- Existing output files fail by default; append only when resuming the same validated query.

## SQL and configuration

- Use `INSERT INTO TOPIC 'topic@conn' SELECT * FROM TOPIC 'topic@conn'`; `WHERE` selects by record timestamp, `OPTIONS (...)` holds copy semantics.
- Prefix statements with `WITH CONNECTION alias AS (...)` for inline librdkafka properties; TOML supplies named profiles. SQL properties override matching TOML keys.
- `ENV('NAME')` resolves at startup, fails if unset, and is redacted in logs. Reject unknown or guarantee-breaking librdkafka settings.
- TOML holds connection profiles and worker tuning; file paths use `FILE cbor('path')`.
- CLI accepts SQL from a file, standard input, or an inline argument.

## Open

- Remaining SQL grammar, exact CBOR field schema, CLI flag names, and exact OS state-folder path. CBOR may change; file segmentation can come later.

## Engineering

- Use single-purpose structs behind narrow interfaces. Wire replaceable components as Fx modules at the composition root; keep core logic Fx-free.
- Use `confluent-kafka-go` for Kafka I/O.
- Test with unit, mock, component, end-to-end, load, and targeted edge-case coverage at the cheapest useful layer.
- Keep code simple; review lines of code and cyclomatic complexity as signals, not targets.
- Prioritize correctness, simplicity, throughput, CI duration, then binary size.
- Keep user documentation example-based and under 10,000 characters.
