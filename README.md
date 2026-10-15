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

When starting a copy from Kafka, Kaferry closes its client on a group coordinator timeout so it can exit with error code `1`. Cleanup can hang; slow cleanup or other coordinator errors can still cause a [librdkafka crash](https://github.com/confluentinc/librdkafka/pull/5397). The affected process copies nothing and leaves saved progress unchanged. Rerun after the coordinator recovers. Production crash rate is unknown. [Details](design/client-compatibility.md#known-issues).

## Quick start

Both topics must exist. Replace the addresses; add [TLS and authentication](#connections) as needed. On one cluster, use different topics.

**`connections.toml`:**

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9092'

[connections.backup]
'bootstrap.servers' = 'destination.example:9092'
```

**`copy.sql`:** filter Kafka record timestamps, not payload times, from 09:00 inclusive to 10:00 exclusive (UTC).

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z'
OPTIONS (GROUP = 'orders-backfill');
```

`@alias` selects a connection; `GROUP` names the consumer group or file checkpoint. Rerun unchanged to resume; use a new group to change the window or `CUTOFF`.

```sh
bin/kaferry run --file copy.sql --config connections.toml
```

For downloads, use your binary's path instead of `bin/kaferry`.

## Files

**Archive retained and new records.** Create the output directory first.

```sql
INSERT INTO FILE cbor('/data/orders.cbor')
SELECT * FROM TOPIC 'orders@prod'
OPTIONS (GROUP = 'orders-archive', START = EARLIEST);
```

**Restore:** for a new file-source job, run `mkdir -p ~/.kaferry/checkpoints` first.

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE cbor('/data/orders.cbor')
OPTIONS (GROUP = 'orders-restore');
```

Files are read to their startup size; rerun for later appends. [File-to-file and other SQL examples](design/grammar.md#valid-examples).

## Connections

Use [librdkafka properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html) as strings; quote dotted TOML keys. SASL/PLAIN over TLS:

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

Keep passwords out of SQL literals and shell arguments. [Configuration rules](docs/usage.md#configuration).

## Before you run

- **Time windows:** Kafka input without an end time runs until stopped. With one, the [cutoff rule](design/decisions.md#completion) can miss records within the range, including those present before the run.
- **Delivery:** at least once; crashes and rebalances can duplicate or reorder writes. Expired offsets or lost checkpoints can start fresh. Kafka input reads committed records.
- **Destination:** Kafka output chooses a destination partition using the original key bytes; null keys can spread. Source partitions are not copied. Ensure bytes and Schema Registry IDs work there. `LogAppendTime` replaces timestamps; retention may delete old records. [Routing](design/decisions.md#kafka-output).
- **Files:** one process per query on one machine, using a local filesystem with locks and hard links. [Resume](docs/usage.md#resume-or-start-over).
- **Permissions:** Kafka sources need topic and group READ; Kafka destinations need topic WRITE. [Grants](docs/usage.md#permissions).

[Usage guide](docs/usage.md) · [File format](docs/file-format.md)

## Development

[Tests](design/components.md#test-targets) · [MIT license](LICENSE); dependencies retain their licenses · [Security](SECURITY.md)
