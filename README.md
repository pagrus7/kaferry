# kafka-fwd2

Copy opaque Kafka records between topics and CBOR files. No transformations or Schema Registry access.

## Build and run

Requires Go 1.26+, C compiler, CGO; bundled librdkafka. Verified: macOS ARM64, Linux ARM64/AMD64 (AMD64 emulated), Kafka 3.9.0. Earlier brokers are unverified.

```sh
make build
bin/kafka-fwd2 run -f copy.sql -c connections.toml
cat copy.sql | bin/kafka-fwd2 run -f - -c connections.toml
bin/kafka-fwd2 run -e "INSERT INTO FILE cbor('/data/copy.cbor') SELECT * FROM FILE cbor('/data/input.cbor') OPTIONS(GROUP='copy')"
```

One `--sql/-e` or `--file/-f`; optional `--config/-c`. Create destination directories/topics first.

## Connections

Keys/string values follow [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html). SQL overrides TOML; conflicting protected settings fail.

```toml
[connections.prod]
'bootstrap.servers' = 'prod-a.example:9093,prod-b.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'SCRAM-SHA-512'
'sasl.username' = 'copier'
'ssl.ca.location' = '/etc/kafka/ca.pem'

[connections.dr]
'bootstrap.servers' = 'dr-a.example:9093,dr-b.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'SCRAM-SHA-512'
'sasl.username' = 'copier'
'ssl.ca.location' = '/etc/kafka/dr-ca.pem'
'compression.type' = 'zstd'
'linger.ms' = '10'

[worker]
partition_record_cap = 5000
partition_byte_cap = 16777216
record_cap = 20000
byte_cap = 134217728
max_frame_bytes = 67108864
delivery_timeout = '60s'
idle_exit = '30s'
tip_wait = '15m'
```

```sql
WITH CONNECTION prod AS ('sasl.password'=ENV('PROD_PASSWORD')),
     CONNECTION dr AS ('sasl.password'=ENV('DR_PASSWORD'))
INSERT INTO TOPIC 'orders@dr'
SELECT * FROM TOPIC 'orders@prod'
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T00:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-02T00:00:00Z'
OPTIONS (GROUP='orders-dr-oct1');
```

`ENV` must be set. Logs redact connection values; avoid credential literals/arguments. Unqualified topics use `default`.

## Examples

Archive from the earliest retained position:

```sql
INSERT INTO FILE cbor('/data/audit.cbor')
SELECT * FROM TOPIC 'audit@prod'
OPTIONS (GROUP='audit-archive', START=EARLIEST);
```

Restore recent records, skipping timestamps outside readable destination bounds:

```sql
INSERT INTO TOPIC 'orders@dr'
SELECT * FROM FILE cbor('/data/orders.cbor')
WHERE RECORD_TIMESTAMP >= NOW() - INTERVAL '7' DAY
OPTIONS (GROUP='restore', ON_REJECTED_TIMESTAMP=SKIP);
```

File extract with inclusive endpoints:

```sql
INSERT INTO FILE cbor('/data/incident.cbor')
SELECT * FROM FILE cbor('/data/orders.cbor')
WHERE RECORD_TIMESTAMP BETWEEN TIMESTAMP '2026-10-01T09:00:00Z'
                           AND TIMESTAMP '2026-10-01T09:30:00Z'
OPTIONS (GROUP=ONE_OFF);
```

Supports `>`, `<=`, lower/upper bounds joined by `AND`, `NOW()` ± integer SECOND/MINUTE/HOUR/DAY intervals. Times: RFC 3339 with zone, millisecond precision. Only `SELECT *`. Quotes escape as `''`; comments: `--`, `/* ... */`.

## Progress and completion

- Required `GROUP` dedicates one identity/query. `ONE_OFF` logs an ID usable for resume; file-source success deletes its checkpoint, not its lock.
- Kafka needs `WHERE` or `START=EARLIEST|LATEST`. Commits win; otherwise lower-bound timestamp lookup, upper-only earliest, or `START`. With group progress, new commitless partitions start earliest. No-match/`LATEST` use the read-committed end before open transactions.
- `NOW()` persists per Kafka partition/file checkpoint. New partitions adopt the lowest-numbered saved window or resolve locally; concurrent workers may differ. Literal times give a shared window. New window needs a new group; expired offsets make it fresh again.
- Stop on timestamp **> end + cutoff**, or read-committed tip after wall time passes it. Default one minute; override `CUTOFF=INTERVAL '5' MINUTE`. Tip completion samples H once and waits through transactions/control records up to `tip_wait`.
- Late records and everything after one future-dated trigger can be omitted. Timestamp stops replay the trigger; tip reruns may copy late arrivals. Without an upper bound, Kafka follows.
- Kafka→Kafka workers share a group, finishing independently. Empty bounded workers exit after `idle_exit`; follow workers stand by. Collective zero exits don't prove coverage: confirm by a sole-worker bounded rerun after others leave.
- Expired commits resume at log start with a logged gap. Active out-of-range/beyond-end commits fail. Only retained data is available.

## Delivery and errors

Null/empty bytes, ordered duplicate headers, nullable values, and timestamps survive. Files retain provenance. Kafka sends the original key, partition unset; default `murmur2_random` matches Java. Same non-null key routes consistently with stable partitioner/count; null keys spread. Source partitions/offsets never route. `LogAppendTime` restamps; missing timestamps without filters get producer timestamps, counted/warned.

Stable routing preserves source-partition→destination-partition submission order within one producer session. Files keep observed order; Kafka has no global order. Crash replay repeats records; old-owner writes may land after replay.

At least once: acknowledgment precedes progress; a crash between them duplicates. Revocation releases immediately, possibly duplicating bounded uncommitted work. Later failures retain earlier output. Idempotence covers producer retries, not replay.

Caps include acknowledged/uncommitted work; one oversized record proceeds alone. Lookahead/native buffers add memory. Commit each second/half-cap; native queues are bounded. `delivery_timeout` is 1–2147483647 ms for delivery/queue-full. `message.timeout.ms`/`delivery.timeout.ms` override delivery (0: infinite); conflicting aliases fail. Source outages retry indefinitely. Signals stop intake, drain writes ten seconds and commit the prefix. Native commit/filesystem/close can extend shutdown.

Default FAIL. Either `ON_ERROR=SKIP` or specific `ON_MISSING_TIMESTAMP=SKIP` / `ON_REJECTED_TIMESTAMP=SKIP`. Only attributable unsent validation errors skip. Batch rejection, authentication, exhausted recovery, corruption, changed source identity always fail. Change policy and resume the same group to revisit failures.

Ensure payload bytes/Schema Registry IDs are destination-compatible. No remapping. Retention is unchecked; old output may expire immediately. Saved topic IDs detect source recreation; destination recreation is undetected.

Limits: Kafka drops NUL header-name suffixes; file→Kafka rejects NUL names. Invalid CreateTime timestamps (zero/negative/outside nanosecond range) use timestamp-error policy. Pre-join checks cannot see unassigned subscriptions. Native coordinator errors can abort/hang at startup: release blocked. [Compatibility](design/client-compatibility.md).

## Files and exit codes

Files require one process/query/machine, local locks/hard links/atomic rename/fsync. Input snapshots size at open; appends wait for rerun; partial final frames end input.

Checkpoints: `~/.kafka-fwd2/checkpoints`, safe group-derived names. Keep progress/output together. Validate source ID/last-frame CRC, destination ID/fsynced extent. Changed input needs a new group. Missing/short output or damage before acknowledged extent fails. Broken unacknowledged tails truncate; complete frames remain and may duplicate, even above a lowered frame limit.

Header publication before ID persistence can leave an unowned output: verify header-only, delete manually, rerun. No adoption without saved ownership. [File format](docs/file-format.md).

Exit `0`: completion/skips/clean follow interruption; `1`: runtime/busy lock/drain/bounded interruption/later conflict; `2`: usage/configuration/pre-copy identity conflict. JSON logs carry windows/stops/skips/counts.

## Verification

```sh
make check
make broker integration
make fault load soak qualification fuzz benchmark
make broker-down
```

Unit/mock/component tests need no Docker. CI schedules fault/load/soak/security tests. Qualification starts and removes its TLS/SASL cluster. [Measurements](design/implementation-results.md).
