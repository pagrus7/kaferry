# Using Kaferry

## First copy without Kafka

Install `kaferry` on `PATH` ([installation](install.md)). This tutorial uses a shell, `curl` and `tar`, three synthetic records, and local storage that supports sync, atomic rename and locks. It needs no broker or credentials. Choose a [release](https://github.com/pagrus7/kaferry/releases) that includes `archive-demo.tar.gz` and matches your binary. Set `version` below to that release tag, then run the block in a directory that does not already contain `archive-demo`:

```sh
version=vX.Y.Z
curl -fLO "https://github.com/pagrus7/kaferry/releases/download/$version/archive-demo.tar.gz"
tar -xzf archive-demo.tar.gz
cd archive-demo
```

Checkout users can build the package from the repository root with `go run ./scripts/package_examples .cache/archive-demo.tar.gz`, then extract that file in a fresh directory. The input is a Kaferry archive: Avro containers with a fixed Kafka record schema and raw key/value bytes. Its values are `order-1`, `order-2`, `order-3`, at original offsets 0–2 in one partition. `copy.sql` copies them to `output` with the named group `archive-demo`; `tutorial.toml` stores replay progress in `checkpoints`.

<!-- archive-tutorial-commands -->
```sh
mkdir -p checkpoints
kaferry explain -c tutorial.toml -f copy.sql > explain.txt
kaferry run -c tutorial.toml -f copy.sql 2> first-run.log
cat first-run.log
kaferry status -c tutorial.toml -f copy.sql --json > status.json
cat status.json
kaferry run -c tutorial.toml -f copy.sql 2> rerun.log
cat rerun.log
```

The first `copy summary` reports `submitted: 3` and `confirmed: 3`. The status includes these fields (other fields are omitted here):

```json
{
  "verdict": "stopped",
  "reason": "file selection completed",
  "partitions": [{"id": 0, "position": 3, "stop": {"reason": "eof", "at": 3}}]
}
```

This group consumed its fixed file selection and saved progress after output confirmation. Status reads saved progress; it does not compare destination contents. The rerun summary reports `submitted: 0` and `confirmed: 0`, and the output still contains three records. Keep `output` and `checkpoints` together: a completed checkpoint does not recreate deleted output. To repeat the tutorial from scratch, use another fresh copy of the example directory.

## What a copy job means

A named `GROUP` identifies saved progress. Kafka stores it as source consumer-group offsets and metadata; archive replay stores it in `worker.checkpoint_dir`. The query and endpoint identities must match when resuming. Omitting `GROUP` generates a new independent group every run.

| Input mode | What stops this worker? | Saved state and rerun |
| --- | --- | --- |
| Kafka without an enabled completion rule | A signal or worker record threshold | Confirmed offsets; the same group resumes and follows new records, including when both timestamp rules are OFF |
| Kafka with timestamp completion enabled | Per-partition [completion rules](../design/decisions.md#completion), or worker record threshold | Confirmed offsets, resolved window, ends and stops; the same group resumes under those rules |
| Kafka with a fixed offset end | Reaching each saved exclusive end, or worker record threshold | Ends remain fixed; rerunning the same group excludes later appends |
| Completed Avro files | Completing selected streams under EOF or explicit criteria, or worker record threshold | Fixed file selection and confirmed cursors; the same group retains its selection |

A worker record threshold applies afresh to each invocation. A new archive group selects current completed files, including files already copied by other groups. See [identity rules](../design/decisions.md#identity) before changing a saved job.

## Configuration

Use `-c connections.toml`; SQL overrides TOML. [Settings](reference.md#configuration-and-cli).

## Copy between Kafka topics

Both topics must exist; the destination needs at least as many partitions as the source. Replace the addresses; add [authentication](#connections) as needed.

**`connections.toml`:**

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9092'

[connections.backup]
'bootstrap.servers' = 'destination.example:9092'
```

**`copy.sql`:** filter Kafka record timestamps, not payload times, from 09:00 inclusive to 10:00 exclusive (UTC).

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-backfill')
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z';
```

`@alias` selects a connection; named `GROUP` saves progress for reruns. Omitting `GROUP` starts a fresh copy each run. Changing the window, effective timestamp allowances, offset criteria or partitioning mode needs a new group. Kafka timestamp completion defaults to five minutes for arrival delay and disorder; `MAX_OUT_OF_ORDER` can override disorder independently. Copying starts immediately. [Completion and assumptions](../design/decisions.md#completion).

```sh
kaferry explain --file copy.sql --config connections.toml
kaferry run --file copy.sql --config connections.toml
```

## Connections

Quote dotted TOML keys. [Supported properties](reference.md#connection-properties); SASL/PLAIN over TLS:

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'PLAIN'
'sasl.username' = 'copier'
'ssl.ca.location' = '/etc/kafka/ca.pem'
```

Set `PROD_PASSWORD` in the environment, then prefix the SQL statement with:

```sql
WITH CONNECTION prod AS ('sasl.password' = ENV('PROD_PASSWORD'))
```

Keep passwords out of SQL literals and shell arguments. [Configuration rules](#configuration).

### OAuth bearer tokens

For an OAuth issuer that accepts the client credentials grant, configure its HTTPS token endpoint:

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'OAUTHBEARER'
'sasl.oauthbearer.token.endpoint.url' = 'https://identity.example/token'
'sasl.oauthbearer.client.id' = 'kaferry'
'sasl.oauthbearer.scope' = 'kafka.read kafka.write'
```

Set `KAFKA_OAUTH_SECRET` in the environment and prefix the copy statement with:

```sql
WITH CONNECTION prod AS ('sasl.oauthbearer.client.secret' = ENV('KAFKA_OAUTH_SECRET'))
```

Kaferry obtains and renews tokens as brokers authenticate. If another credential agent supplies tokens, remove every other `sasl.oauthbearer.*` setting from TOML and SQL, then set `'sasl.oauthbearer.token.file' = '/run/secrets/kafka-token'`. That agent must replace the file before the token expires; Kaferry reads it for each authentication.

### AWS MSK IAM

Prepare AWS credentials through the standard AWS SDK chain: environment variables, a shared profile selected by `AWS_PROFILE`, or the workload's IAM role. Configure an IAM-enabled MSK bootstrap address:

```toml
[connections.prod]
'bootstrap.servers' = 'b-1.cluster.kafka.us-east-1.amazonaws.com:9098'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'AWS_MSK_IAM'
```

The SDK renews temporary credentials when the selected provider supports renewal; environment credentials and static profile entries are loaded once. Do not set `sasl.username` or `sasl.password`. In containers, pass the required AWS environment or mount profile files at paths selected by `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE`.

### Kerberos

Obtain a client keytab and a `krb5.conf` that reaches your realm's key distribution center. The broker hostname must match its Kerberos service principal:

```toml
[connections.prod]
'bootstrap.servers' = 'broker.example:9093'
'security.protocol' = 'SASL_SSL'
'sasl.mechanism' = 'GSSAPI'
'sasl.kerberos.principal' = 'copier@EXAMPLE.COM'
'sasl.kerberos.keytab' = '/run/secrets/copier.keytab'
'sasl.kerberos.config' = '/etc/krb5.conf'
```

Kaferry acquires tickets with the keytab and renews them while the client runs. Mount both files when running in Docker. [Authentication settings](reference.md#connection-properties).

## SQL settings

`WITH` and `OPTIONS` are optional. Kafka source `WITH` sets `GROUP`, `START`, `MAX_STALENESS` and `MAX_OUT_OF_ORDER`; archive source `WITH` sets `GROUP` and optional `MAX_OUT_OF_ORDER`. Sink properties configure partitioning or archive rotation. [Defaults by endpoint](reference.md#endpoint-with-properties). Final `OPTIONS` holds worker stops and error policies. [Full syntax](../design/grammar.md).

## Permissions

Kafka needs source topic/group READ and destination WRITE (including DESCRIBE); some brokers also need cluster IDEMPOTENT_WRITE. Without a named `GROUP`, grant group READ on the `kaferry-` prefix. File-only copies need no Kafka grants. [Optional DESCRIBE_CONFIGS](../design/client-compatibility.md#optional-destination-configuration).

## Preview a copy

```sh
kaferry explain -c connections.toml -f copy.sql
kaferry explain -c connections.toml -f copy.sql --json
```

Shows endpoints, progress, windows and policies without joining, writing or locking. Records, bytes, duration and write access are not checked. [Output contract](reference.md#inspection).

## Run a copy

Pass SQL with `-f file`, `-f -` (stdin) or `-e "SQL"`.

**Archive retained records, then follow new ones.** Create the parent directory:

```sh
mkdir -p archive
kaferry run -c connections.toml -f - <<'SQL'
INSERT INTO ARCHIVE 'archive/orders'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-archive');
SQL
```

Replay completed files while capture continues; stop capture first to include its remaining records. Create the checkpoint directory:

```sh
mkdir -p ~/.kaferry/checkpoints
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM ARCHIVE 'archive/orders' WITH (GROUP = 'orders-restore');"
```

Without timestamp bounds, Kafka starts at the earliest retained record; `START = LATEST` selects new ones. Saved progress takes precedence. Without an enabled completion rule, Kafka follows. Archive replay selects current completed files and exits; the same group retains its selection. [Examples](../design/grammar.md#valid-examples).

To copy the current readable Kafka backlog and exit, add `WHERE RECORD_OFFSET < LATEST`. Each partition saves its own exclusive end; rerunning the same group keeps those ends. Timestamp filters can restrict records within that snapshot without a lateness assumption. Use a new group for a fresh snapshot. [Offset examples](../design/grammar.md#valid-examples).

Use `WHERE PARTITION IN (0, 2)` for fixed source IDs, or `NOT IN (0, 2)` for all others. Kafka discovers additions while workers run; archive replay pins matching files once and keeps that selection on resume. All workers share the same predicate. Listed IDs must exist in Kafka metadata or completed archive headers. Excluded Kafka partitions are not fetched or committed; excluded archive payloads are not read. [Selection rules](../design/decisions.md#source-partitions).

Omitting `GROUP` means `ONE_OFF`: each run logs a fresh group. Reruns and concurrent unnamed runs copy independently. Use a shared named group for Kafka workers.

Add `OPTIONS (WORKER_STOP_AFTER_RECORDS = 100)` before the final `;` to stop each process after accepting 100 records for writing. Each restart gets a fresh allowance. [Counting rules](../design/decisions.md#worker-record-stop).

## Stop and check results

SIGINT/SIGTERM stop reading, allow 10 s for writes, then save progress. File flushes can block saving. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. A second signal forces exit. See `copy summary` on stderr; check saved stops with:

```sh
kaferry status -c connections.toml -f copy.sql --json
```

`stopped` means every currently selected partition has a saved stop reason, including worker record thresholds. More input may remain; staleness stops can miss records within the range. Kafka activity is checked twice, without an atomic snapshot; file activity is not checked. Ctrl+C on a Kafka follow run leaves status `incomplete`. [Status and exit codes](reference.md#saved-status).

## Resume or start over

Rerun with the same `GROUP`, endpoints, filter, `MAX_STALENESS`, partitioning mode and checkpoint directory. Saved `NOW()` bounds are reused. For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep output, checkpoints and locks. Archive files must stay unchanged until replay completes. [Archive recovery and moving replay](#archive-procedures).

## Archive procedures

Keep archive data, capture recovery state, and replay progress separate:

```text
work/
  archive/orders/
    <writer>.<file>.avro       completed data
    .<writer>.<file>.partial   private capture data
    .kaferry/writers/          capture state and locks
  checkpoints/                replay progress and locks
```

Set `[worker] checkpoint_dir = '/absolute/path/to/work/checkpoints'` when using this layout. The checkpoint directory must already exist and be outside the source archive.

### Capture crashed

Keep the whole archive directory, including hidden files, and retain the source Kafka group. Restart the same capture query. Startup recovers available writer namespaces under their locks, publishes acknowledged data, and then resumes copying. Do not delete private files, recovery state or lock files. Missing or corrupt acknowledged data stops recovery; preserve it for investigation rather than starting over in place. Records written before a crash but not saved in source progress can repeat. [Storage contract](../design/archive.md#capture-and-recovery).

### Move replay to another machine

1. Stop replay and all capture writers for this archive, and wait for successful exits. Recover a failed capture before moving its archive. Keep the destination’s existing output.
2. Copy the entire source archive and checkpoint directory, including hidden files and lock files, to storage with the required sync, rename and lock guarantees. Selected archive files must remain byte-for-byte unchanged.
3. Create the destination checkpoint directory if needed and set `worker.checkpoint_dir` to its new path. Update archive paths and connection details while keeping the named group, query and destination identity. For a file destination, move its existing output as well; paths alone do not prove that output is present.
4. Run `explain` to check that saved progress is found, then rerun the same copy. Destination-confirmed records after the last saved checkpoint may repeat.

Use only one replay process for a group/checkpoint directory. A different checkpoint directory with no saved state starts fresh and can duplicate output. [Checkpoint contract](../design/archive.md#checkpoints).

### Capture is still running

Replay selects completed files once. It does not read private files or add files published later to the same group. To include capture’s remaining records, stop every intended capture worker, wait for successful exits. If a capture failed, recover it before selecting replay input. Preview a fresh selection to check for private-file warnings:

```sh
kaferry explain -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM ARCHIVE 'archive/orders' WITH (GROUP = ONE_OFF);"
```

This preview creates no group or checkpoint and does not expand an existing replay selection. These checks establish which archive files are available; they do not verify the intended Kafka source set.

A new replay group selects current completed files and can copy previously replayed records again. Plan a fresh destination or account for those duplicates before using another group.

## Troubleshooting

Keep the SQL, named group, configuration, `explain` output, stderr logs and `status --json` output together when investigating. Avoid recording secrets in SQL or shell arguments.

| Symptom | Evidence and interpretation | Next action and consequence |
| --- | --- | --- |
| Running, but destination does not grow | Read stderr `progress` events and `explain`: the worker may be following, have no assignment, filter records, or resume past available input. `run` writes no stdout. | Check source availability, selected partitions, saved start positions and filter counts. Correct the input or query; use a new group for a changed filter, which starts an independent copy. |
| Copy fails on a record | Read the error and `copy summary`. Only identified synchronous validation failures support SKIP; delivery and storage failures stop the copy. | Fix the cause and resume the same group. Use SKIP only when deliberately dropping that rejected record; async broker failures cannot be skipped. [Policies](../design/decisions.md#errors). |
| Rerun copies records again | Compare the logged group and saved progress. An unnamed run, missing checkpoint, expired offsets, or writes after the last save can repeat data. | Restore the intended progress and destination together, or resume the same named group. Previously repeated output remains and may need downstream handling. |
| Replay omits recent capture output | Compare its fixed selection with completed files; private or later-published files are outside that selection. | Follow [capture completion](#capture-is-still-running). A new group can replay older files again. |
| Status is `stopped`, but input remains | Read each partition’s saved `stop.reason`; a worker record threshold can stop before source end. | For a threshold stop, rerun the same job for another allowance. Interpret other reasons using the [completion contract](../design/decisions.md#completion). |

## Errors

Errors stop the copy. `ON_ERROR = SKIP` drops identified records rejected before sending; broker rejections cannot be skipped. Counts appear in `copy summary`. [Policies](../design/decisions.md#errors) · [SQL errors](../design/grammar.md#invalid-examples).

## Workers

Kafka-source workers use identical SQL and a dedicated `GROUP`. Use fixed timestamps; `NOW()` can differ. Stop workers before upgrading. Archive captures can use local or shared storage. Replay uses one process per group/checkpoint directory. Rerun unassigned workers to pick up unfinished work.

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

Progress logs default to 30 s; `worker.progress_interval='0'` disables them. [Metric meanings and settings](reference.md#observability).

## Containers

For released images (Linux AMD64/ARM64):

```sh
docker run --rm --stop-timeout 35 --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$PWD,dst=/work" ghcr.io/pagrus7/kaferry:vX.Y.Z \
  run --file /work/copy.sql --config /work/connections.toml
```

Use container paths and reachable broker addresses. Set `worker.checkpoint_dir` to `/work/checkpoints` for resume; `--rm` deletes container-only files. The stop timeout must exceed `worker.shutdown_timeout` (30s by default). [Image verification](install.md#container-images).
