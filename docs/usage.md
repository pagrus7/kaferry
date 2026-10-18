# Using Kaferry

## Configuration

Use `-c connections.toml`; SQL overrides TOML. [Connections](../README.md#connections) · [Settings](../design/decisions.md#configuration-and-cli).

## Permissions

Kafka needs source topic/group READ and destination WRITE (including DESCRIBE); some brokers also need cluster IDEMPOTENT_WRITE. For `ONE_OFF`, grant group READ on the `kaferry-` prefix. File-only copies need no Kafka grants. [Optional DESCRIBE_CONFIGS](../design/client-compatibility.md#optional-destination-configuration).

## Preview a copy

```sh
kaferry explain -c connections.toml -f copy.sql
kaferry explain -c connections.toml -f copy.sql --json
```

Shows endpoints, progress, windows and policies without joining, writing or locking. Records, bytes, duration and write access are not checked. [Output contract](../design/decisions.md#inspection).

## Run a copy

Pass SQL with `-f file`, `-f -` (stdin) or `-e "SQL"`.

**Archive retained records, then follow new ones.** Create the output directory:

```sh
mkdir -p archive
kaferry run -c connections.toml -f - <<'SQL'
INSERT INTO FILE cbor('archive/orders.cbor')
SELECT * FROM TOPIC 'orders@prod'
OPTIONS (GROUP = 'orders-archive', START = EARLIEST);
SQL
```

Stop archiving with Ctrl+C before restoring. Create the default checkpoint directory:

```sh
mkdir -p ~/.kaferry/checkpoints
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE cbor('archive/orders.cbor') OPTIONS (GROUP = 'orders-restore');"
```

Kafka needs `WHERE` or `START`: `EARLIEST` for retained records, `LATEST` for new ones. Saved progress takes precedence. Files read to their opening size. [Examples](../design/grammar.md#valid-examples).

Add `WORKER_STOP_AFTER_RECORDS = 100` to `OPTIONS` to stop each process after accepting 100 records for writing. Each restart gets a fresh allowance. [Counting rules](../design/decisions.md#worker-record-stop).

## Stop and check results

SIGINT/SIGTERM stop reading, allow 10 s for writes, then save progress. File flushes can block saving. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. A second signal forces exit. See `copy summary` on stderr; check saved stops with:

```sh
kaferry status -c connections.toml -f copy.sql --json
```

`stopped` means every current partition has a saved stop reason, including worker record thresholds. More input may remain; time cutoffs can miss records within the range. Kafka activity is checked twice, without an atomic snapshot; file activity is not checked. Ctrl+C on a Kafka follow run leaves status `incomplete`. [Status and exit codes](../design/decisions.md#saved-status).

## Resume or start over

Rerun with the same `GROUP`, endpoints, filter, `CUTOFF` and checkpoint directory. Saved `NOW()` bounds are reused. For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep output, checkpoints and locks; losing them requires a new group/output. [Recovery rules](../design/decisions.md#output-recovery).

## Errors

Errors stop the copy. `ON_ERROR = SKIP` drops identified records rejected before sending; broker rejections cannot be skipped. Counts appear in `copy summary`. [Policies](../design/decisions.md#errors) · [SQL errors](../design/grammar.md#invalid-examples).

## Workers

For topic-to-topic copies, run identical SQL and a dedicated `GROUP` on each machine. Use fixed timestamps; `NOW()` can differ. Stop workers before upgrading. File queries use one process. Rerun unassigned workers to pick up unfinished work.

## Monitoring

With an OTLP/HTTP receiver, append to `connections.toml`:

```toml
[metrics]
enabled = true
```

```sh
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT='https://collector.example/v1/metrics'
kaferry run -c connections.toml -f copy.sql
```

Progress logs default to 30 s; `worker.progress_interval='0'` disables them. [Metric meanings and settings](../design/decisions.md#observability).
