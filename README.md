# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/kaferry-logo-dark.svg">
  <img src="assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="600">
</picture>

[![CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml)
[![Go coverage](https://codecov.io/gh/pagrus7/kaferry/branch/main/graph/badge.svg?flag=go)](https://codecov.io/gh/pagrus7/kaferry)
[![Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml)

Copy Kafka records between topics and Avro archives with one SQL-like statement.

Preserves raw bytes, nulls, ordered headers and timestamps where possible. No transformations or Schema Registry access. Copies records; topics, topic settings, ACLs and consumer-group offsets must be prepared separately.

## Install

[Download a release](https://github.com/pagrus7/kaferry/releases/latest), verify `SHA256SUMS`, extract and put `kaferry` on `PATH`.

Homebrew: `brew install pagrus7/tap/kaferry`. [Source builds, containers and redistribution](docs/install.md).

## Known issues

When Kaferry reads Kafka, franz-go v1.22.2 misreads record timestamps outside ±9,223,372,036,854 Unix milliseconds. [Kafka 4+ defaults and scope](design/client-compatibility.md#known-issues).

## Quick start

[Download and unpack the tutorial](docs/usage.md#first-copy-without-kafka) for three known records and no broker. From its `archive-demo` directory:

```sh
mkdir -p checkpoints
kaferry run -c tutorial.toml -f copy.sql
kaferry status -c tutorial.toml -f copy.sql --json
```

The first run confirms three records; rerunning the same named group confirms zero. [Kafka setup and copying](docs/usage.md#copy-between-kafka-topics) · [Authentication](docs/usage.md#connections).

## Before you run

- **Time windows:** The [staleness rule](design/decisions.md#completion) can miss records within the range, including those present before the run.
- **Delivery:** at least once; crashes and rebalances can duplicate or reorder writes. Expired offsets or lost checkpoints can start fresh. Kafka input reads committed records.
- **Destination:** original partition numbers are preserved. Ensure bytes and Schema Registry IDs work there. Broker settings can replace timestamps or delete old records. [Routing](design/decisions.md#kafka-output).
- **Archives:** multiple capture workers; one replay process per group/checkpoint directory. Storage needs durable sync, atomic rename and locks. [File setup and resume](docs/usage.md#run-a-copy).
- **Permissions:** [Source READ and destination WRITE grants](docs/usage.md#permissions).

[Documentation index](docs/README.md) · [Settings and results](docs/reference.md) · [Tests and contributing](design/components.md) · [Security](SECURITY.md) · [MIT license](LICENSE); dependencies retain their licenses.
