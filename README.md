# Kaferry

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/kaferry-logo-dark.svg">
  <img src="assets/kaferry-logo.svg" alt="Kaferry logo: a ferry carrying record blocks" width="600">
</picture>

[![CI](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/ci.yml)
[![Go coverage](https://codecov.io/gh/pagrus7/kaferry/branch/main/graph/badge.svg?flag=go)](https://codecov.io/gh/pagrus7/kaferry)
[![Extended tests](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml/badge.svg?branch=main)](https://github.com/pagrus7/kaferry/actions/workflows/qualification.yml)

Copy Kafka records between topics and Kaferry archives with one SQL-like statement.

Preserves raw bytes, nulls and ordered headers.

## Install

[Download a release](https://github.com/pagrus7/kaferry/releases/latest), verify `SHA256SUMS`, extract and put `kaferry` on `PATH`.

Homebrew: `brew install pagrus7/tap/kaferry`. [Source builds, containers and redistribution](docs/install.md).

## Quick start

[Download and unpack the tutorial](docs/usage.md#first-copy-without-kafka) for three known records and no broker. From its `archive-demo` directory:

```sh
mkdir -p checkpoints
kaferry run -c tutorial.toml -f copy.sql
kaferry status -c tutorial.toml -f copy.sql --json
```

The first run confirms three records; rerunning the same named group confirms zero. [Kafka setup and copying](docs/usage.md#copy-between-kafka-topics) · [Authentication](docs/usage.md#connections).

## Caveats

Before copying your own data, read the [caveats](docs/caveats.md) for time windows, delivery and resume, Kafka destinations, archive storage and timestamp limits.

[Documentation index](docs/README.md) · [Settings and results](docs/reference.md) · [Tests and contributing](design/components.md) · [Security](SECURITY.md) · [MIT license](LICENSE); dependencies retain their licenses.
