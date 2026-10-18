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

[Download](https://github.com/pagrus7/kaferry/releases/latest), verify `SHA256SUMS`, extract and run `kaferry`.

From source: `make build` with Go 1.26+, then `bin/kaferry --version`. No C compiler is needed.

## Known issues

When Kaferry reads Kafka, franz-go v1.22.2 misreads record timestamps outside ±9,223,372,036,854 Unix milliseconds. [Kafka 4+ defaults and scope](design/client-compatibility.md#known-issues).

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
bin/kaferry explain --file copy.sql --config connections.toml
bin/kaferry run --file copy.sql --config connections.toml
```

For downloads, use your binary's path instead of `bin/kaferry`.

## Connections

Quote dotted TOML keys and string values. Only the [listed properties](design/decisions.md#connection-properties) are accepted. SASL/PLAIN over TLS:

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

- **Time windows:** Kafka input without an end time or `LIMIT` runs until stopped. With an end time, the [cutoff rule](design/decisions.md#completion) can miss records within the range, including those present before the run.
- **Delivery:** at least once; crashes and rebalances can duplicate or reorder writes. Expired offsets or lost checkpoints can start fresh. Kafka input reads committed records.
- **Destination:** Kafka output chooses a destination partition using the original key bytes; null keys can spread. Source partitions are not copied. Ensure bytes and Schema Registry IDs work there. `LogAppendTime` replaces timestamps; retention may delete old records. [Routing](design/decisions.md#kafka-output).
- **Files:** one process per query on one machine, on a local filesystem that supports file locks and hard links. [File setup and resume](docs/usage.md#run-a-copy).
- **Permissions:** [Source READ and destination WRITE grants](docs/usage.md#permissions).

[Usage guide](docs/usage.md)

## Development

[Tests](design/components.md#test-targets) · [MIT license](LICENSE); dependencies retain their licenses · [Security](SECURITY.md)
