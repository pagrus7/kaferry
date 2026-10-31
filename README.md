# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/kaferry-logo-dark.svg">
  <img src="assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="600">
</picture>

A lightweight command-line tool for copying Kafka records between topics and files using SQL-like statements.

[![CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml)
[![Go coverage](https://codecov.io/gh/pagrus7/kaferry/branch/main/graph/badge.svg?flag=go)](https://codecov.io/gh/pagrus7/kaferry)
[![Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml)

## Examples

Prepare your [Kafka connections and destination topics](docs/usage.md#copy-between-kafka-topics). Save one statement as `copy.sql` and run:

```sh
kaferry run -c connections.toml -f copy.sql
```

**Topic to topic** — copy current records and stop.

```sql
INSERT INTO TOPIC 'orders-copy@backup'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_OFFSET < LATEST;
```

**Topic to file** — save current records to disk and stop.

```sql
INSERT INTO ARCHIVE 'orders-archive'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_OFFSET < LATEST;
```

**Time range** — copy records from 09:00 to 10:00 UTC, inclusive, within the current backlog and stop.

```sql
INSERT INTO TOPIC 'orders-copy@backup'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_OFFSET < LATEST
  AND RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z'
                           AND TIMESTAMP '2026-10-01T10:00:00Z';
```

**File to topic** — replay the archive into Kafka and stop.

```sql
INSERT INTO TOPIC 'orders-restored@backup'
SELECT * FROM ARCHIVE 'orders-archive';
```

> **Warning:** Kaferry is pre-1.0. Releases may introduce breaking changes.

## Choosing a tool

| Tool | Use it for |
| --- | --- |
| Kaferry | Copy a time range to another topic or cluster, tail new records, capture records to disk, or replay an archive into Kafka. |
| MirrorMaker 2 | Maintain continuous replication between Kafka clusters, including topic configurations, ACLs and consumer-group offsets. |
| kcat | Inspect records and cluster metadata, or produce and consume records through shell pipelines. |

## Install

Homebrew:

```sh
brew install pagrus7/tap/kaferry
```

Go 1.26+:

```sh
CGO_ENABLED=0 go install github.com/pagrus7/kaferry/cmd/kaferry@latest
```

[Prebuilt binaries](https://github.com/pagrus7/kaferry/releases/latest) · [Installation details](docs/install.md).

## Quick start

```sh
docker run --rm --mount source=kaferry-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest run -c tutorial.toml -f copy.sql
```

```text
{"time":"2026-10-25T22:39:06.595190641Z","level":"INFO","msg":"partition assigned","partition":0,"position":0,"window":{"lower":null,"upper":null}}
{"time":"2026-10-25T22:39:06.602132061Z","level":"INFO","msg":"copy summary","submitted":3,"confirmed":3,"filtered":0,"skipped":0,"timestamp_stops":0,"tip_stops":0,"peak_uncommitted_records":3,"peak_uncommitted_bytes":534}
```

With a fresh `kaferry-demo` volume, this copies the three bundled sample records into `output` and saves progress. The files remain after the container exits. [Inspect results, rerun and reset](docs/usage.md#first-copy-with-docker) · [Run without Docker](docs/usage.md#first-copy-without-kafka) · [Try Kafka locally](docs/usage.md#local-kafka-playground).

## More examples

[Current records](docs/usage.md#copy-between-kafka-topics) · [Offset range](docs/usage.md#copy-an-offset-range) · [Selected partitions](docs/usage.md#copy-selected-partitions) · [Time range](docs/usage.md#copy-records-in-a-time-range) · [Tail new records](docs/usage.md#keep-copying-new-records) · [Archive and replay](docs/usage.md#archive-your-own-topic).

[Documentation index](docs/README.md) · [Settings and results](docs/reference.md) · [Tests and contributing](design/components.md) · [Security](SECURITY.md) · [MIT license](LICENSE); dependencies retain their licenses.
