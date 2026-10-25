# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/kaferry-logo-dark.svg">
  <img src="assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="600">
</picture>

[![CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml)
[![Go coverage](https://codecov.io/gh/pagrus7/kaferry/branch/main/graph/badge.svg?flag=go)](https://codecov.io/gh/pagrus7/kaferry)
[![Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml)

Copy Kafka records between topics and Kaferry archives with one SQL-like statement.

Preserves raw bytes, nulls, ordered headers and timestamps where possible. No transformations or Schema Registry access. Copies records; topics, topic settings, ACLs and consumer-group offsets must be prepared separately.

## Install

[Download a release](https://github.com/pagrus7/kaferry/releases/latest), verify `SHA256SUMS`, extract and put `kaferry` on `PATH`.

Homebrew: `brew install pagrus7/tap/kaferry`. [Source builds, containers and redistribution](docs/install.md).

## Known issues

When Kaferry reads Kafka, franz-go v1.22.2 misreads record timestamps outside ±9,223,372,036,854 Unix milliseconds. [Kafka 4+ defaults and scope](design/client-compatibility.md#known-issues).

## Quick start

```sh
docker run --rm --mount source=kaferry-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest run -c tutorial.toml -f copy.sql
```

```text
{"time":"2026-10-25T22:39:06.595190641Z","level":"INFO","msg":"partition assigned","partition":0,"position":0,"window":{"lower":null,"upper":null}}
{"time":"2026-10-25T22:39:06.602132061Z","level":"INFO","msg":"copy summary","submitted":3,"confirmed":3,"filtered":0,"skipped":0,"timestamp_stops":0,"tip_stops":0,"peak_uncommitted_records":3,"peak_uncommitted_bytes":534}
```

This copies the three bundled sample records into `output` and saves progress in the `kaferry-demo` volume. The files remain after the container exits. [Inspect results, rerun and reset](docs/usage.md#first-copy-with-docker) · [Run without Docker](docs/usage.md#first-copy-without-kafka) · [Copy Kafka topics](docs/usage.md#copy-between-kafka-topics).

## Before you run

- **Time windows:** The [staleness rule](design/decisions.md#completion) can miss records within the range, including those present before the run.
- **Delivery:** at least once; crashes and rebalances can duplicate or reorder writes. Expired offsets or lost checkpoints can start fresh. Kafka input reads committed records.
- **Destination:** original partition numbers are preserved. Ensure bytes and Schema Registry IDs work there. Broker settings can replace timestamps or delete old records. [Routing](design/decisions.md#kafka-output).
- **Archives:** multiple capture workers; one replay process per group/checkpoint directory. Storage needs durable sync, atomic rename and locks. [File setup and resume](docs/usage.md#run-a-copy).
- **Permissions:** [Source READ and destination WRITE grants](docs/usage.md#permissions).

[Documentation index](docs/README.md) · [Settings and results](docs/reference.md) · [Tests and contributing](design/components.md) · [Security](SECURITY.md) · [MIT license](LICENSE); dependencies retain their licenses.
