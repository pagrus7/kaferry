# Avro archives

Directory-based Avro archives support capture, snapshot replay and file-to-file copying. Optional summaries are pending. [Usage](../docs/usage.md).

## Records and files

Each completed file is a Snappy-compressed Avro object container for one original source partition. Every record contains:

- Broker-reported cluster ID, topic name, 16-byte topic ID, partition and offset.
- Nullable raw key/value bytes and an ordered array of binary header names and nullable values.
- Nullable signed 64-bit millisecond timestamp and its kind: unknown, CreateTime or LogAppendTime.

Null differs from empty. Duplicate headers and offsets survive. Payloads remain opaque. Kafka destinations preserve original partition numbers by default; explicit key routing can repartition them. Topic IDs distinguish deleted and recreated topics.

The [schema](../internal/archive/avro/schema.json) defines the wire fields. OCF metadata `kaferry` contains canonical JSON, with these fields in order: `version`, `writer_id`, `file_id`, `source`, `first_offset`, `block_limit`. Version is `1`; writer/file IDs are 32 lowercase hexadecimal characters. The nested `source` fields are `cluster`, `topic`, `topic_id`, `partition`, in that order. JSON topic IDs use 32 lowercase hexadecimal characters; Avro records store 16 bytes. No whitespace, duplicate/unknown fields or alternate key spellings are accepted. Readers permit equivalent Avro schema representations.

The codec owns block cuts, counts, checksums and exact block positions. The Avro library encodes/decodes individual records. Encoding finishes before a record changes the pending block. Only format-limit rejections permit skipping; source identity changes or decreasing offsets require rotation, and caller errors stop capture.

The writer records its decoded-block byte limit (`worker.max_frame_bytes`, default 64 MiB). Readers use that limit or return `LimitError` when their configured cap is lower. The header-count limit is `max(16, block_limit/32)`; blocks contain at most one million records. Blocks normally stay within a 256 KiB target before compression; a single larger record may form a larger block within the limit. Truncated data is an error, never normal EOF. A write error stops the writer permanently. Flushing releases pending payload buffers.

## Layout and ownership

```text
archive/
  <writer>.<file>.avro
  <writer>.<file>.summary.json
  .<writer>.<file>.partial
  .kaferry/writers/<writer>/
    lock
    state.json
```

Workers own unique namespaces and files. Storage may be local or shared; completed files can be collected unchanged. The storage must support durable sync, atomic rename and exclusive ownership locks across all users of that storage. Shared filesystem support requires checking these guarantees across hosts.

Create a namespace with valid empty recovery state under a temporary name, sync it, then rename and sync its parent before creating data. Keep its stable lock while writing or recovering. Startup recovers every published namespace whose lock it can acquire; skip busy or unpublished namespaces. Keep acquired locks until recovery and one streamed scan for their unacknowledged private files finish. Recovery is independent of Kafka partition assignment. Never steal locks by age or replace lock files.

## Capture and recovery

1. Write complete compressed blocks to private files; sync the data and new directory entries.
2. Atomically save and sync required writer state containing each file's acknowledged boundary.
3. Acknowledge the batch; Kafka source offsets can then be committed.

Any write or sync failure stops capture without further acknowledgements. Required state is versioned and checked for corruption. A record-skip policy cannot bypass a storage failure.

Writer state uses canonical JSON in a SHA-256 envelope. It stores version `1`, writer ID and each private file's full identity, acknowledged byte length and record count. Both saving and loading enforce a 16 MiB limit. State replacement syncs the new file and its directory before returning success.

Recovery validates each saved prefix. Truncate only its unacknowledged private tail, sync, and publish the prefix; start a new file for further capture. A completed file must match the saved identity and length and is never truncated. Missing, shortened or corrupt acknowledged data is fatal. Discard unlisted private files only when valid initialized state and ownership prove they were never acknowledged.

Publishing renames synced private files without overwriting conflicting targets, syncs the data directory, then removes their state entries in one durable replacement per batch. Recovery state retains all boundaries until every rename is durable. A crash between output durability and Kafka commit can produce duplicates; retain them.

## Rotation

```text
INSERT INTO FILE avro('/archive/orders') WITH (ROTATE_AFTER_SIZE = 512m, ROTATE_AFTER_AGE = INTERVAL '2' HOUR)
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-archive', START = EARLIEST);
```

Only the path is required. Defaults: 1g compressed container bytes or one elapsed hour. Size suffixes k/m/g are case-insensitive powers of 1024; plain integers are bytes. The complete crossing block may exceed the size target. Check age even when no records arrive.

Also rotate when original source identity changes or offsets decrease; equal offsets are allowed. Worker completion publishes every nonempty remainder. Graceful shutdown attempts this within the shutdown deadline. Work caps and source commits flush durable blocks without forcing rotation. Create no empty data files.

The capture component caps open files at 1024 by default. TOML `worker.archive_capture_open_files` changes this budget. At capacity, publish the least recently used file before opening another; more active partitions than this budget can produce files far below the size/age targets. Check age during input and quiet flushes. `Finish` publishes remaining files; `Close` only releases resources. Calls use one event loop.

## Replay and progress

Replay selects matching completed file IDs once and saves that selection before producing. Warn about observed private files. Concurrent capture does not expand this selection; a snapshot does not establish that capture is complete.

- Same `GROUP`: resume its fixed selection and exact per-file block/record cursors. A completed group remains completed.
- New `GROUP`: select current completed files matching the partition predicate, including files another group may have replayed.

Store and exclusively lock the group-derived checkpoint outside the archive, which may be read-only. Save cursors only through destination-confirmed progress. Missing or changed selected inputs fail an incomplete replay.

Merge overlaps by original source identity, partition and offset. Preserve duplicates with deterministic file-ID/physical-record ties. No ordering across partitions. Bound open handles and payload buffers; cursor/head metadata grows with selected files. Evicting a block may require decompressing it and decoding earlier records again. The replay component defaults to 32 open files and 8 cached blocks; TOML `worker.archive_replay_open_files` and `worker.archive_replay_blocks` change these budgets. More simultaneously overlapping files than cached blocks can sharply reduce throughput. The CLI applies partition caps to the whole replay: 5,000 unsaved records / 16 MiB by default.

### Checkpoints

Checkpoints use group-derived filenames and stable `.lock` files in `~/.kaferry/checkpoints`, or `worker.checkpoint_dir`. The directory must exist. A group and checkpoint directory identify one replay; another directory starts fresh and can duplicate output. The canonical JSON envelope contains `state` and its lowercase `sha256` digest. State version `1` stores `group`, query `fingerprint`, resolved `window`, confirmed `position`, `complete`, selected `files` and optional `stop` (`reason`, `at`). Stop reason `eof` means the fixed selection was consumed; `worker_record_cap` means the requested worker threshold was reached. `at` equals the confirmed input-record count. Restart clears a previous threshold stop when new progress is saved.

Each `files` entry contains:

- `file`: `identity`, byte `size`, header-end byte `start` and content `sha256`.
- `next`: byte offset `block`, zero-based record index `record` and `previous` source offset (`-1` at the start).

`position` counts confirmed input records in merge order. EOF uses the file length as `block` and record index zero. Reads and writes are capped at 64 MiB. A [partial checkpoint fixture](../internal/archive/replay/testdata/partial-checkpoint-v1.json) fixes the wire representation.

Writer-state checksums are calculated on durability flushes and publication; replay-checkpoint checksums are calculated on progress saves. Every replay progress save rewrites and checksums the whole file list, so save time and bytes written grow with selection size. Appends do not hash JSON themselves. Frequent rotation can make these batches small. Idle flushes with no new writes do not save writer state. Selection validates every completed header and hashes only matching files. See [partition selection](decisions.md#source-partitions). An incomplete restart rechecks every selected file, including consumed files, before returning records. Explain/status read headers, sizes and saved JSON without hashing archive payloads. Completed files must remain immutable during replay.

A checkpoint failure stops new reads. A source failure still allows saving earlier confirmed output. Destination-confirmed records after the last saved checkpoint can repeat after restart.

The session returns every record and retains the resolved time window for the caller's filter. Out-of-order timestamps do not end a file. No active-file reads, replay follow mode, global completion barrier or built-in consolidation.

## Optional summaries

Each completed file may have a rebuildable versioned JSON summary: file ID, actual length, source identity, count, offset/time ranges and missing timestamp count. Trust it only with matching file ID and length; otherwise scan Avro. Pruning must preserve missing-timestamp/error behavior. Summary failure warns and does not invalidate completed data. Summaries never replace required recovery state or replay checkpoints.
