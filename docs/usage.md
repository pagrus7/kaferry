# Using Kaferry

## Configuration

Use `-c connections.toml`; SQL overrides TOML. [Connections](../README.md#connections) · [Settings](../design/decisions.md#configuration-and-cli).

## SQL settings

Use `WITH` on either endpoint: source `GROUP`, `START`, `CUTOFF`; destination `PARTITIONING` or Avro rotation. Final `OPTIONS` holds worker stops and error policies. [Full syntax](../design/grammar.md).

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
INSERT INTO FILE avro('archive/orders')
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-archive', START = EARLIEST);
SQL
```

Replay completed files while capture continues; stop capture first to include its remaining records. Create the checkpoint directory:

```sh
mkdir -p ~/.kaferry/checkpoints
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE avro('archive/orders') WITH (GROUP = 'orders-restore');"
```

Kafka needs `WHERE` or `START`: `EARLIEST` for retained records, `LATEST` for new ones. Saved progress takes precedence. Avro pins completed files: the same group resumes them; a new group selects current files. [Examples](../design/grammar.md#valid-examples).

Add `OPTIONS (WORKER_STOP_AFTER_RECORDS = 100)` before the final `;` to stop each process after accepting 100 records for writing. Each restart gets a fresh allowance. [Counting rules](../design/decisions.md#worker-record-stop).

## Stop and check results

SIGINT/SIGTERM stop reading, allow 10 s for writes, then save progress. File flushes can block saving. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. A second signal forces exit. See `copy summary` on stderr; check saved stops with:

```sh
kaferry status -c connections.toml -f copy.sql --json
```

`stopped` means every current partition has a saved stop reason, including worker record thresholds. More input may remain; time cutoffs can miss records within the range. Kafka activity is checked twice, without an atomic snapshot; file activity is not checked. Ctrl+C on a Kafka follow run leaves status `incomplete`. [Status and exit codes](../design/decisions.md#saved-status).

## Resume or start over

Rerun with the same `GROUP`, endpoints, filter, `CUTOFF`, partitioning mode and checkpoint directory. Saved `NOW()` bounds are reused. For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep output, checkpoints and locks. Archive files must stay unchanged until replay completes. [Recovery rules](../design/archive.md#capture-and-recovery).

## Errors

Errors stop the copy. `ON_ERROR = SKIP` drops identified records rejected before sending; broker rejections cannot be skipped. Counts appear in `copy summary`. [Policies](../design/decisions.md#errors) · [SQL errors](../design/grammar.md#invalid-examples).

## Workers

Kafka-source workers use identical SQL and a dedicated `GROUP`. Use fixed timestamps; `NOW()` can differ. Stop workers before upgrading. Avro captures can use local or shared storage. Replay uses one process per group/checkpoint directory. Rerun unassigned workers to pick up unfinished work.

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
