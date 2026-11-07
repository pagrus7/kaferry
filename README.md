# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/kaferry-logo-dark.svg">
  <img src="docs/assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="400">
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

**Topic to topic**

Copy current records and stop.

```sql
INSERT INTO TOPIC 'orders-copy@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-copy-oct31')
WHERE RECORD_OFFSET < LATEST;
```

**Topic to file**

Save current records to disk and stop.

```sql
INSERT INTO ARCHIVE 'orders-archive'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-archive-oct31')
WHERE RECORD_OFFSET < LATEST;
```

**Time range**

Copy records from 09:00 to 10:00 UTC, inclusive, within the current backlog and stop.

```sql
INSERT INTO TOPIC 'orders-copy@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-hour-oct01')
WHERE RECORD_OFFSET < LATEST
  AND RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z'
                           AND TIMESTAMP '2026-10-01T10:00:00Z';
```

**File to topic**

Replay the archive into Kafka and stop.

```sql
INSERT INTO TOPIC 'orders-restored@backup'
SELECT * FROM ARCHIVE 'orders-archive' WITH (GROUP = 'orders-restore-oct31');
```

More examples: [Current records](docs/usage.md#copy-between-kafka-topics) · [Offset range](docs/usage.md#copy-an-offset-range) · [Selected partitions](docs/usage.md#copy-selected-partitions) · [Time range](docs/usage.md#copy-records-in-a-time-range) · [Tail new records](docs/usage.md#keep-copying-new-records) · [Archive and replay](docs/usage.md#archive-your-own-topic).

## Install

> **Warning:** Kaferry is pre-1.0. Releases may introduce breaking changes.

Homebrew:

```sh
brew install pagrus7/tap/kaferry
```

Go 1.26+:

```sh
CGO_ENABLED=0 go install github.com/pagrus7/kaferry/cmd/kaferry@latest
```

[Prebuilt binaries](https://github.com/pagrus7/kaferry/releases/latest) · [Installation details](docs/install.md).

## Docker quick start

**Try a Kafka copy.** Start Docker, then run:

```sh
docker run --rm ghcr.io/pagrus7/kaferry-demo:latest
```

Docker downloads the demo image on first use. It starts a temporary Kafka broker, seeds three records in `orders`, prints and runs the SQL to copy them into `orders-copy`, then reads and verifies the destination:

```text
order-1
order-2
order-3

Demo passed: verified all 3 records in orders-copy.
```

No local files, Kafka setup, credentials or host ports are needed. Each run starts fresh and exits after verification; `--rm` removes its container and data. The downloaded image stays cached. Failure or a three-minute timeout exits nonzero. [Image details](docs/install.md#container-images).

**Run your own SQL and configuration.** [Prepare your topics and connections](docs/usage.md#copy-between-kafka-topics), then run from the directory containing `connections.toml` and `copy.sql` (macOS/Linux):

```sh
docker run --rm --stop-timeout 35 --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$PWD,dst=/work" ghcr.io/pagrus7/kaferry:latest \
  run -c /work/connections.toml -f /work/copy.sql
```

Use broker addresses reachable from the container. For archive replay, create `checkpoints` locally and set `worker.checkpoint_dir = '/work/checkpoints'` in TOML; keep archive output under `/work` too. [Container paths and stopping](docs/usage.md#containers) · [Archive demo and resume](docs/usage.md#first-copy-with-docker).

## Choosing a tool

| Tool | Use it for |
| --- | --- |
| Kaferry | Copy a time range to another topic or cluster, tail new records, capture records to disk, or replay an archive into Kafka. |
| MirrorMaker 2 | Maintain continuous replication between Kafka clusters, including topic configurations, ACLs and consumer-group offsets. |
| kcat | Inspect records and cluster metadata, or produce and consume records through shell pipelines. |

**Documentation:** [Index](docs/README.md) · [Settings and results](docs/reference.md).

**[Endurance test results](https://pagrus7.github.io/kaferry/)** — published runs, measurements and evidence.

**Project:** [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [MIT license](LICENSE); dependencies retain their licenses.
