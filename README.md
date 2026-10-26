# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/kaferry-logo-dark.svg">
  <img src="assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="600">
</picture>

[![CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml)
[![Go coverage](https://codecov.io/gh/pagrus7/kaferry/branch/main/graph/badge.svg?flag=go)](https://codecov.io/gh/pagrus7/kaferry)
[![Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml)

Copy Kafka records between topics and disk archives with one SQL-like statement.

A Kaferry archive is a directory of Avro files containing Kafka records with their original topic, partition and offset. Copies preserve raw bytes, nulls and ordered headers.

## Choosing a tool

| Tool | Use it for |
| --- | --- |
| Kaferry | Copy a time range to another topic or cluster, capture records to disk, or replay an archive into Kafka. A [group name](docs/usage.md#resume-or-start-over) identifies the copy's saved progress; reuse it to resume. |
| [MirrorMaker 2](https://kafka.apache.org/43/operations/geo-replication-cross-cluster-data-mirroring/) | Maintain continuous replication between Kafka clusters, including topic configurations, ACLs and consumer-group offsets. |
| [kcat](https://github.com/edenhill/kcat) | Inspect records and cluster metadata, or produce and consume records through shell pipelines. |

## Install

[Download a release](https://github.com/pagrus7/kaferry/releases/latest), verify `SHA256SUMS`, extract and put `kaferry` on `PATH`.

Homebrew: `brew install pagrus7/tap/kaferry`. [Source builds, containers and redistribution](docs/install.md).

## Quick start

```sh
docker run --rm --mount source=kaferry-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest run -c tutorial.toml -f copy.sql
```

```text
{"time":"2026-10-25T22:39:06.595190641Z","level":"INFO","msg":"partition assigned","partition":0,"position":0,"window":{"lower":null,"upper":null}}
{"time":"2026-10-25T22:39:06.602132061Z","level":"INFO","msg":"copy summary","submitted":3,"confirmed":3,"filtered":0,"skipped":0,"timestamp_stops":0,"tip_stops":0,"peak_uncommitted_records":3,"peak_uncommitted_bytes":534}
```

With a fresh `kaferry-demo` volume, this copies the three bundled sample records into `output` and saves progress. The files remain after the container exits. [Inspect results, rerun and reset](docs/usage.md#first-copy-with-docker) · [Run without Docker](docs/usage.md#first-copy-without-kafka) · [Try Kafka locally](docs/usage.md#local-kafka-playground).

## Examples

[Current records](docs/usage.md#copy-between-kafka-topics) · [Offset range](docs/usage.md#copy-an-offset-range) · [Selected partitions](docs/usage.md#copy-selected-partitions) · [Time range](docs/usage.md#copy-records-in-a-time-range) · [Archive and replay](docs/usage.md#archive-your-own-topic).

[Documentation index](docs/README.md) · [Settings and results](docs/reference.md) · [Tests and contributing](design/components.md) · [Security](SECURITY.md) · [MIT license](LICENSE); dependencies retain their licenses.
