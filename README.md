# kafka-fwd2

Copy Kafka records between topics and CBOR files with one SQL-like statement.

Preserves raw keys, values, nulls, ordered headers and timestamps where possible. No transformations or Schema Registry access.

Implementation is AI-driven, using Codex and Claude Code under human direction.

**Status:** v0.1.0 is not production-qualified: it can crash during startup when the Kafka group coordinator is slow or recovering, and shutdown can hang indefinitely. See [client limitations and permissions](design/client-compatibility.md).

## Install

[Download v0.1.0](https://github.com/pagrus7/kafka-fwd2/releases/tag/v0.1.0): Linux AMD64/ARM64 (glibc 2.34+) or macOS ARM64, with `SHA256SUMS`. Source builds need Go 1.26+, a C compiler and CGO; librdkafka is bundled.

```sh
git clone https://github.com/pagrus7/kafka-fwd2.git
cd kafka-fwd2
make build
```

Verified broker baseline: Apache Kafka 3.9.0. See [compatibility](design/client-compatibility.md) for other deployment limits.

## Quick start

Both topics must exist. Replace the example addresses below and add security settings as needed; they use no TLS or authentication. On one cluster, use a different destination topic.

**`connections.toml`:**

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9092'

[connections.backup]
'bootstrap.servers' = 'destination.example:9092'
```

**`copy.sql`:** copy the given hour of Kafka record timestamps.

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z'
OPTIONS (GROUP = 'orders-backfill');
```

`@prod` and `@backup` select connections. `GROUP` names the consumer group, or a local checkpoint for file sources.

Run:

```sh
bin/kafka-fwd2 run --file copy.sql --config connections.toml
```

On completion, the run logs a JSON `copy summary` with counts to stderr and exits `0`. The cutoff rule below can omit records.

Use your downloaded binary's path, or `bin/kafka-fwd2` after building. `-e` takes inline SQL; `-f -` reads stdin.

## More examples

**Archive from the earliest retained record, then keep following.** Create the output directory first.

```sql
INSERT INTO FILE cbor('/data/orders.cbor')
SELECT * FROM TOPIC 'orders@prod'
OPTIONS (GROUP = 'orders-archive', START = EARLIEST);
```

**Restore the last seven days.** Skip timestamps known to be outside the destination's accepted bounds.

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE cbor('/data/orders.cbor')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
OPTIONS (GROUP = 'orders-restore', ON_REJECTED_TIMESTAMP = SKIP);
```

**File to file:** [extract an inclusive time range](design/grammar.md#valid-examples) with `BETWEEN` and `GROUP = ONE_OFF`, which generates and logs a group name.

Filters use Kafka record time, not payload event time. Files read up to their size at startup; later appends wait for another run. [Full syntax](design/grammar.md) covers timestamps, intervals and `SELECT *`.

## Connections

Use [librdkafka properties](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html); quote dotted TOML keys and use string values. SQL overrides TOML. Topics without `@alias` use `default`.

Secure connection example, using the tested PLAIN over TLS mechanism:

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'PLAIN'
'sasl.username' = 'copier'
'ssl.ca.location' = '/etc/kafka/ca.pem'
```

Prefix the statement to read its password from the environment:

```sql
WITH CONNECTION prod AS ('sasl.password' = ENV('PROD_PASSWORD'))
```

`ENV` works only in SQL and must be set. SQL can define a complete connection. Logs redact properties; keep passwords out of literals and shell arguments. Settings that break copy guarantees are rejected.

<details>
<summary>Optional worker settings and defaults</summary>

Limits count uncommitted work, including delivered records. [Details](design/decisions.md#delivery-and-progress).

```toml
[worker]
partition_record_cap = 5000
partition_byte_cap = 16777216
record_cap = 20000
byte_cap = 134217728
max_frame_bytes = 67108864
delivery_timeout = '60s'
idle_exit = '30s'
tip_wait = '15m'
# checkpoint_dir = '/var/lib/kafka-fwd2/checkpoints'
```

Use an absolute path when overriding `checkpoint_dir`.

</details>

## Before you run

- **Resume:** rerun the same query and `GROUP` to continue from saved progress. New windows or `CUTOFF` need a new group. `NOW()` is saved per partition or file. Expired offsets or deleted checkpoints can cause a fresh copy.
- **Window end:** Kafka follows without an upper bound. Otherwise each partition stops at its first timestamp after `end + CUTOFF` (default one minute), or after catching up once that time passes. Records past the stop point can still be within the window and be missed, even if already present before the run. [Details](design/decisions.md#completion).
- **Delivery:** at least once. Crashing after writing but before saving progress, or rebalancing, can duplicate writes. Kafka sources read committed records.
- **Kafka output:** keep original keys; let the destination choose partitions. Default Java-compatible partitioning keeps equal non-null keys together with a fixed partition count. `LogAppendTime` replaces timestamps. [Ordering details](design/decisions.md#kafka-output).
- **Payloads:** ensure bytes and Schema Registry IDs work at the destination. Old records may expire under destination retention.
- **Workers:** topic-to-topic processes may share a group; use literal times for one window. All exiting 0 does not prove completion. After they leave, rerun alone with the same query/group and check there is no `local idle exit`.
- **Files:** one process per query on one machine, using a local filesystem with locks and hard links. File-source progress lives in `~/.kafka-fwd2/checkpoints`. Keep output and progress; losing either prevents resuming file output. Use a new group and output path to start over.
- **Errors:** fail by default. Use `ON_ERROR = SKIP` or specific timestamp policies. Broker rejections, authentication errors and corruption still fail. [Skippable errors](design/decisions.md#errors).

Signals stop reading, drain writes for up to ten seconds and save progress; native calls can extend shutdown. Structured logs are JSON on stderr. Exit codes: `0` finished or stopped while following; `1` failed or interrupted before finishing; `2` invalid usage/configuration or a conflict before copying.

[Detailed behavior](design/decisions.md) · [File format and recovery](docs/file-format.md) · [Client limits and permissions](design/client-compatibility.md)

## Development

```sh
make check
make broker integration
make broker-down
```

Kafka tests need Docker. [CI checks](https://github.com/pagrus7/kafka-fwd2/actions/workflows/ci.yml) run on changes; [qualification](https://github.com/pagrus7/kafka-fwd2/actions/workflows/qualification.yml) retains results for 30 days. More: [test targets](design/components.md#test-targets), [component design](design/components.md).
