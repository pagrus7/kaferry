# Archive redesign

Accepted design for directory-based Avro archives. Only the codec is implemented. Capture, recovery, rotation, replay and summaries are pending; the CLI uses [CBOR files](../docs/usage.md).

## Records and files

Each completed file is a Snappy-compressed Avro object container for one original source partition. Every record contains:

- Broker-reported cluster ID, topic name, 16-byte topic ID, partition and offset.
- Nullable raw key/value bytes and an ordered array of binary header names and nullable values.
- Nullable signed 64-bit millisecond timestamp and its kind: unknown, CreateTime or LogAppendTime.

Null differs from empty. Duplicate headers and offsets survive. Payloads remain opaque; Kafka destinations use their own partition routing. Topic IDs distinguish deleted and recreated topics.

The [schema](../internal/archive/avro/schema.json) defines the wire fields. OCF metadata `kaferry` contains canonical JSON, with these fields in order: `version`, `writer_id`, `file_id`, `source`, `first_offset`, `block_limit`. Version is `1`; writer/file IDs are 32 lowercase hexadecimal characters. The nested `source` fields are `cluster`, `topic`, `topic_id`, `partition`, in that order. JSON topic IDs use 32 lowercase hexadecimal characters; Avro records store 16 bytes. No whitespace, duplicate/unknown fields or alternate key spellings are accepted. Readers permit equivalent Avro schema representations.

The codec owns block cuts, counts, checksums and exact block positions. The Avro library encodes/decodes individual records. Encoding finishes before a record changes the pending block. Only format-limit rejections permit skipping; source identity changes or decreasing offsets require rotation, and caller errors stop capture.

The writer records its decoded-block byte limit. Readers use that limit or return `LimitError` when their configured cap is lower. The header-count limit is `max(16, block_limit/32)`; blocks contain at most one million records. Blocks normally target 256 KiB before compression; a single larger record may form a larger block within the limit. Truncated data is an error, never normal EOF. A write error stops the writer permanently. Flushing releases pending payload buffers.

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

Create a namespace with valid empty recovery state under a temporary name, sync it, then rename and sync its parent before creating data. Keep its stable lock while writing or recovering. Startup recovers every published namespace whose lock it can acquire; skip busy or unpublished namespaces. Recovery is independent of Kafka partition assignment. Never steal locks by age or replace lock files.

## Capture and recovery

1. Write complete compressed blocks to private files; sync the data and new directory entries.
2. Atomically save and sync required writer state containing each file's acknowledged boundary.
3. Acknowledge the batch; Kafka source offsets can then be committed.

Any write or sync failure stops capture without further acknowledgements. Required state is versioned and checked for corruption. A record-skip policy cannot bypass a storage failure.

Recovery validates each saved prefix. Truncate only its unacknowledged private tail, sync, and publish the prefix; start a new file for further capture. A completed file must match the saved identity and length and is never truncated. Missing, shortened or corrupt acknowledged data is fatal. Discard unlisted private files only when valid initialized state and ownership prove they were never acknowledged.

Publishing renames a synced private file without overwriting a conflicting target, syncs the data directory, then durably removes its state entry. A crash between output durability and Kafka commit can produce duplicates; retain them.

## Rotation

```text
INSERT INTO FILE avro('/archive/orders',
    ROTATE_AFTER_SIZE = 512m,
    ROTATE_AFTER_AGE = INTERVAL '2' HOUR)
SELECT * FROM TOPIC 'orders@prod'
OPTIONS (GROUP = 'orders-archive', START = EARLIEST);
```

Only the path is required. Defaults: 1g compressed container bytes or one elapsed hour. Size suffixes k/m/g are case-insensitive powers of 1024; plain integers are bytes. The complete crossing block may exceed the size target. Check age even when no records arrive.

Also rotate when original source identity changes or offsets decrease; equal offsets are allowed. Worker completion publishes every nonempty remainder. Graceful shutdown attempts this within the shutdown deadline. Work caps and source commits flush durable blocks without forcing rotation. Create no empty data files.

## Replay and progress

Replay selects completed file IDs once and saves that selection before producing. Warn about observed private files. Concurrent capture does not expand this selection; a snapshot does not establish that capture is complete.

- Same `GROUP`: resume its fixed selection and exact per-file block/record cursors. A completed group remains completed.
- New `GROUP`: select all current completed files, including files another group may have replayed.

Store and exclusively lock the group-derived checkpoint outside the archive, which may be read-only. Save cursors only through destination-confirmed progress. Missing or changed selected inputs fail an incomplete replay.

Merge overlaps by original source identity, partition and offset. Preserve duplicates with deterministic file-ID/physical-record ties. No ordering across partitions. Bound open handles and payload buffers; cursor/head metadata grows with selected files. Evicting a block may require decompressing it again.

Time filters inspect record timestamps, including out-of-order values. Do not stop a file at its first timestamp beyond the range. No active-file reads, replay follow mode, global completion barrier or built-in consolidation.

## Optional summaries

Each completed file may have a rebuildable versioned JSON summary: file ID, actual length, source identity, count, offset/time ranges and missing timestamp count. Trust it only with matching file ID and length; otherwise scan Avro. Pruning must preserve missing-timestamp/error behavior. Summary failure warns and does not invalidate completed data. Summaries never replace required recovery state or replay checkpoints.
