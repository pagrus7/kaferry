# Caveats

Read these before copying your own data. See the [usage guide](usage.md) for commands and the [copy behavior reference](../design/decisions.md) for exact contracts.

## Copy scope and permissions

Kaferry copies records only. Prepare topics, topic settings, ACLs and consumer-group offsets separately. It does not transform data or access Schema Registry. Kafka copies need [source READ and destination WRITE grants](usage.md#permissions).

## Time windows

Record timestamps need not follow source order. When timestamps determine when a copy stops, it can miss matching records, including records present before the run. See the [completion rules and their assumptions](../design/decisions.md#completion).

## Delivery and resume

Delivery is at least once: crashes and Kafka rebalances can duplicate or reorder writes. Kafka input reads committed records. Expired source group offsets or lost archive checkpoints can start a fresh copy. Keep the same named group and saved progress when [resuming](usage.md#resume-or-start-over).

## Kafka destinations

By default, records keep their original partition numbers; the destination needs every selected source partition ID. Ensure raw bytes and Schema Registry IDs work at the destination. Broker settings can replace timestamps or delete old records soon after copying. See [Kafka output](../design/decisions.md#kafka-output) for routing and timestamp rules.

## Archive storage and workers

Multiple workers can capture archives; replay uses one process per group/checkpoint directory. Storage needs durable sync, atomic rename and locks across its users. Keep archive files, recovery state and checkpoints when resuming. See [file setup](usage.md#run-a-copy) and [archive recovery and moving replay](usage.md#archive-procedures).

## Kafka input timestamps

When Kaferry reads Kafka, its client, franz-go v1.22.2, misreads record timestamps outside roughly 1677–2262. The exact limits are ±9,223,372,036,854 Unix milliseconds.

[Kafka 4+ rejects new far-future timestamps by default](https://kafka.apache.org/40/generated/topic_config.html#topicconfigs_message.timestamp.after.max.ms); existing records, extreme past timestamps and custom settings can still expose this limitation.
