# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/kaferry-logo-dark.svg">
  <img src="assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="600">
</picture>

[![CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml)
[![Go coverage](https://codecov.io/gh/pagrus7/kaferry/branch/main/graph/badge.svg?flag=go)](https://codecov.io/gh/pagrus7/kaferry)
[![Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml)

Copy Kafka records between topics and CBOR files with one SQL-like statement.

Kaferry preserves raw key and value bytes, nulls, header order and timestamps where possible. It does not transform data or access Schema Registry.

Implementation is AI-driven with Codex and Claude Code under human direction.

## Install

[Download Kaferry](https://github.com/pagrus7/kaferry/releases/latest) for Linux AMD64/ARM64 (glibc 2.34+) or macOS 14+ ARM64. Check the archive against `SHA256SUMS`, then extract and run `kaferry`.

Or build from source with Go 1.26+, a C compiler and CGO enabled. librdkafka is bundled.

```sh
git clone https://github.com/pagrus7/kaferry.git
cd kaferry
make build
bin/kaferry --version
```

Tested with Apache Kafka 3.9.0.

## Known issues

When starting a copy from Kafka, Kaferry closes its client on a group coordinator timeout so it can exit with error code `1`. Cleanup can hang; slow cleanup or other coordinator errors can still cause a [librdkafka crash](https://github.com/confluentinc/librdkafka/pull/5397). The affected worker copies nothing and leaves saved progress unchanged. Rerun after the coordinator recovers. Production crash rate is unknown. [Details](design/client-compatibility.md#known-issues).

## Quick start

Both topics must exist. Replace the addresses and add TLS and authentication as needed. On one cluster, use different topics.

**`connections.toml`:**

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9092'

[connections.backup]
'bootstrap.servers' = 'destination.example:9092'
```

**`copy.sql`:** copy records timestamped from 09:00 up to, but not including, 10:00 UTC.

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z'
OPTIONS (GROUP = 'orders-backfill');
```

`@alias` selects a connection. `GROUP` names the Kafka consumer group or file-source checkpoint.

```sh
bin/kaferry run --file copy.sql --config connections.toml
```

For downloads, use your binary's path instead of `bin/kaferry`.

`-e` takes inline SQL; `-f -` reads stdin.

## More examples

**Archive retained records and keep reading new ones.** Create the output directory first.

```sql
INSERT INTO FILE cbor('/data/orders.cbor')
SELECT * FROM TOPIC 'orders@prod'
OPTIONS (GROUP = 'orders-archive', START = EARLIEST);
```

**Restore recent records, skipping timestamps rejected before sending.**

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE cbor('/data/orders.cbor')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
OPTIONS (GROUP = 'orders-restore', ON_REJECTED_TIMESTAMP = SKIP);
```

**File to file:** [copy a range](design/grammar.md#valid-examples) with `BETWEEN` (both ends included). `GROUP = ONE_OFF` logs a new name each run; use that name as `GROUP` to resume an interrupted run.

Filters use Kafka record timestamps, not times inside payloads. Kaferry reads files to their startup size; rerun for later appends. [Full syntax](design/grammar.md).

## Connections

Use [librdkafka properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html); quote dotted TOML keys and use string values. SQL overrides TOML. Topics without `@alias` use `default`.

SASL/PLAIN authentication over TLS:

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'PLAIN'
'sasl.username' = 'copier'
'ssl.ca.location' = '/etc/kafka/ca.pem'
```

Set `PROD_PASSWORD` in the environment, then prefix the SQL statement with:

```sql
WITH CONNECTION prod AS ('sasl.password' = ENV('PROD_PASSWORD'))
```

`ENV()` works only in SQL and fails if the variable is unset. Logs redact connection settings; keep passwords out of SQL literals and shell arguments. Kaferry rejects settings that would break its copy guarantees.

TOML's `[worker]` table sets [limits and timeouts](design/decisions.md#worker-settings).

## Before you run

- **Resume:** rerun the same query and `GROUP`. Use a new group to change the time filter or `CUTOFF`. On restart, Kaferry reuses each partition's or file's saved `NOW()` time range. Expired offsets or lost checkpoints can start a fresh copy.
- **Kafka input:** without an end time, Kaferry keeps reading new records. With one, each partition stops at its first timestamp after `end + CUTOFF` (default one minute), or after catching up once that time passes. Records at later offsets can have in-window timestamps and be missed, even if present before the run. [Details](design/decisions.md#completion).
- **Duplicates and order:** delivery is at least once. A crash after writing but before saving progress, or Kafka moving partitions between workers, can repeat or reorder writes. Kafka sources read only committed records.
- **Kafka output:** Kaferry leaves the partition unset and sends the original key bytes. With the default Java-compatible partitioner and a fixed partition count, equal non-null keys stay together. A `LogAppendTime` destination replaces timestamps. [Ordering details](design/decisions.md#kafka-output).
- **Payloads:** make sure the bytes and Schema Registry IDs work at the destination. Its retention rules may delete old records soon after copying.
- **Workers:** topic-to-topic workers use the same query and named group. Workers can calculate `NOW()` differently; use fixed timestamps for one shared time range. [Details](design/decisions.md#groups-and-workers).
- **Files:** use one process per query on one machine, with a local filesystem supporting locks and hard links. For a new file-source job, run `mkdir -p ~/.kaferry/checkpoints`. Keep that directory, the output and saved progress to resume. If output or progress is lost, start over with a new group and output path.
- **Permissions:** Kafka sources need READ on the source topic and group; Kafka output needs destination WRITE. For Kafka-source `ONE_OFF`, allow the `kaferry-` group prefix. [Details](design/client-compatibility.md#permissions).
- **Errors:** errors stop the copy by default. Use `ON_ERROR = SKIP` or a timestamp `SKIP` policy only for records Kaferry rejects before sending. Broker rejections (including timestamp rejections), authentication errors and corruption stop it. [Skippable errors](design/decisions.md#errors).

On SIGINT or SIGTERM, Kaferry stops reading, waits up to ten seconds for writes and saves confirmed progress. Kafka or filesystem calls can still hang shutdown. JSON logs (including `copy summary`) and final errors go to stderr.

- `0`: this worker finished its remaining assigned work, or a signal cleanly stopped a copy from Kafka with no end time.
- `1`: the copy failed, or a signal stopped startup, any copy from a file, or a copy from Kafka with an end time.
- `2`: invalid command/configuration, or a conflict detected before reading records.
- `3`: for a copy from Kafka with an end time, Kafka assigned this worker no partitions, and none arrived within `worker.idle_exit` (default 30 seconds). This does not say whether other workers finished.

A librdkafka crash can end the process with a signal instead of these codes.

[Copy rules](design/decisions.md) · [File format](docs/file-format.md)

## Development

```sh
make check
make broker integration
make broker-down
```

Broker tests need Docker. [Tests](design/components.md#tests).

[MIT license](LICENSE); dependencies retain their licenses. [Security reporting](SECURITY.md).
