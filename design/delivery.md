# Exactly-once delivery

A job produces one committed output for each selected input occurrence when that same job resumes. Equal values, repeated keys and overlapping physical archive records are separate occurrences. A new group or replay binding identifies an independent job. Retain selected input, committed output and progress throughout recovery; deletion of committed Kafka records or published archive files is not detected.

## Kafka output

Each owned source partition has a stable transactional producer ID on the destination cluster. Archive replay uses one virtual partition with a physical file cursor. The adapter initializes that producer, fencing the previous owner, **before** loading authoritative progress. It reads a complete `read_committed` prefix; startup barriers cannot pass unresolved transactions. Initialization saves resolved windows and captured ends before any advisory source-group commit.

Output records and the confirmed next cursor commit in the same destination transaction. Source-group commits verify current membership but do not determine resume positions. Ownership loss closes the partition's producer. When franz-go reports a missing broker while preparing a batch, the adapter fences that producer and retries the same records only if no earlier batch is awaiting commit and the saved cursor is unchanged. An ambiguous commit result fails the worker; the next owner fences it and reads the actual committed outcome. Consumers must use `read_committed` to exclude aborted and pending output. Transactions are independent per source partition; no whole-query atomicity or cross-partition order is promised.

The progress topic must have `cleanup.policy=compact` without `delete`. Its identity is part of the job. It needs enough partitions for source IDs; initialization grows it before opening up to eight partition producers concurrently. One producer remains open per owned source partition. Client buffers and checkpoint metadata add memory beyond the copy loop's work caps. Workload throughput, latency and resource acceptance targets must be measured separately.

## Archive output

Storage must provide durable locks, sync and atomic replacement across all its users. Capture syncs private file bytes before saving their byte boundaries with the confirmed source cursor. Archive-only copies save both in one local replacement. Kafka capture saves both in a source-cluster transaction, then replaces the local mirror. Every committed capture disk must be retained; coordination cannot reconstruct a permanently lost disk.

A replacement fences the stable producer, reads complete authority, commits its writer claim, then waits until that claim and the earlier prefix are committed-visible before changing record files. Recovery never treats a partial prefix as authority. Delayed coordinator decisions or transaction markers can prevent takeover; deadlines fail the attempt without permitting truncation.

Publication-only recovery holds the old disk's lock and requires a different committed-visible writer claim. It validates all required per-writer authority before changing any record files. It publishes only that writer's approved prefix. Its separate publication transaction may remove published boundaries from that writer's manifest, but cannot advance global source progress or fence the live capture producer. Acknowledged published files are never truncated. Missing, shortened or corrupt committed bytes stop recovery.

Rotation syncs every earlier stream before publishing a cursor that covers them. The capture file budget applies across all owned source partitions. Replay saves the fixed selected files and physical cursors, preserving legitimate overlapping occurrences.

## State format

Delivery frames use a checksummed canonical JSON envelope, explicit version `1`, and the complete group, query fingerprint, source/destination cluster and topic identities, progress-topic identity and replay-job binding when applicable. Frames contain the confirmed cursor and source commit metadata or full replay state. Incompatible versions, changed identities and incomplete state fail before accepting output. Advisory source-group metadata uses only `kaferry:4;`, with optional captured end. Earlier executables reject this prefix. Legacy source-group offsets or standalone replay checkpoints cannot establish this contract and are rejected without migration.

Kafka values use checksummed chunks and a version `1` head in the same transaction. The envelope bound is 96 MiB; full replay state remains bounded at 64 MiB and per-writer Kafka capture state at 32 MiB. Local archive envelopes use explicit version `1` with their source partition, delivery frame and capture boundaries. Local writer bindings store version `1` and their writer ID; missing bindings for an initialized namespace are rejected. The `.eos` replay binding names format `kaferry.delivery/v1`, version `1`, and the complete job identity. A local advisory replay mirror may be repaired from authority; moving a replay requires its binding and checkpoint directory. An existing ownership file without both the binding and its checkpoint mirror cannot establish a fresh job. Capture relocation requires the whole output directory, including hidden state.

Inspection reads authoritative state without group joins, producer initialization, barrier writes, recovery or execution locks. It may observe changing progress and does not verify destination payloads. [Operating procedures](../docs/usage.md#prepare-authoritative-progress) and [inspection output](../docs/reference.md#inspection).
