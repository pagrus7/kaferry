# Using Kaferry

For existing Kafka topics, start with the [copy examples](#copy-between-kafka-topics).

## Local Kafka playground

Install `kaferry` on PATH ([installation](install.md)) and start Docker. Use a fresh working directory, a free local port 9092 and an unused `kaferry-kafka-demo` container name. This example starts one disposable Kafka broker on loopback, creates two single-partition topics and produces three acknowledged records. It needs no Kafka credentials, Go, Compose or tutorial download. Without Docker, use the [archive demo](#first-copy-without-kafka).

<!-- kafka-quickstart-setup -->
```sh
docker run -d --name kaferry-kafka-demo -p 127.0.0.1:9092:9092 apache/kafka:3.9.2
docker exec kaferry-kafka-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 1 --replication-factor 1
docker exec kaferry-kafka-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders-copy --partitions 1 --replication-factor 1
printf 'order-1\norder-2\norder-3\n' | docker exec -i kaferry-kafka-demo /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --sync --producer-property acks=all
```

Copy the current readable records and exit. `RECORD_OFFSET < LATEST` saves each partition's readable end on its first assignment; the same group keeps that end on reruns and excludes later appends.

<!-- kafka-quickstart-copy -->
```sh
cat > copy.sql <<'SQL'
WITH CONNECTION default AS ('bootstrap.servers' = 'localhost:9092')
INSERT INTO TOPIC 'orders-copy'
SELECT * FROM TOPIC 'orders' WITH (GROUP = 'demo')
WHERE RECORD_OFFSET < LATEST;
SQL
kaferry run -f copy.sql
```

The copy summary reports `submitted: 3` and `confirmed: 3`. Read the destination:

<!-- kafka-quickstart-read -->
```sh
docker exec kaferry-kafka-demo /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders-copy --from-beginning --max-messages 3 --timeout-ms 10000
```

```text
order-1
order-2
order-3
```

Rerun the same query and group. It reports `submitted: 0` and `confirmed: 0`; the destination still contains three records:

<!-- kafka-quickstart-rerun -->
```sh
kaferry run -f copy.sql
```

Remove the demo container and its data when finished:

<!-- kafka-quickstart-cleanup -->
```sh
docker rm -fv kaferry-kafka-demo
```

[Copy your own Kafka topics](#copy-between-kafka-topics) · [Inspect saved progress](#stop-and-check-results).

## Copy between Kafka topics

Install `kaferry` on `PATH` ([installation](install.md)). Create both topics first; topic settings, ACLs and consumer-group offsets are not copied. These examples keep original partition numbers, so the destination needs every selected source partition ID; add `WITH (PARTITIONING = KEY)` after the destination topic to route by key instead ([routing](../design/decisions.md#kafka-output)).

Replace the addresses in **`connections.toml`**; add [authentication](#connections) and the required [permissions](#permissions). Kaferry copies raw bytes: ensure their formats and Schema Registry IDs work at the destination. Destination broker settings can replace timestamps or delete old records soon after copying; see [Kafka output](../design/decisions.md#kafka-output) and [client limits](../design/client-compatibility.md#kafka-input-timestamps).

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9092'

[connections.backup]
'bootstrap.servers' = 'destination.example:9092'
```

Choose one example below and save it as **`copy.sql`**. `@prod` and `@backup` select the connection profiles above; a topic without `@alias`, as in the playground, uses the connection named `default`. Preview, run and check saved progress with:

```sh
kaferry explain -c connections.toml -f copy.sql
kaferry run -c connections.toml -f copy.sql
kaferry status -c connections.toml -f copy.sql --json
```

For a following copy, press Ctrl+C before checking status. `explain` checks the query and current metadata without writing; it does not check write access. [Inspection output](reference.md#inspection).

### Copy the current backlog and stop

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-snapshot')
WHERE RECORD_OFFSET < LATEST;
```

An offset is a position within one partition. `LATEST` is its readable end, excluded from the copy: an end of 10 means `< LATEST` includes offset 9. Kaferry saves each partition's end when first assigned; an open transaction can hold it below records already written, and partitions capture their ends separately.

Only committed records still retained by Kafka can be copied. The named `GROUP` saves progress in the source Kafka group: rerunning it keeps the saved ends and excludes later appends. A new group, or omitting `GROUP`, starts an independent copy and can duplicate existing output; see [resuming](#resume-or-start-over).

### Try a small copy first

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-preview')
WHERE RECORD_OFFSET < LATEST
OPTIONS (WORKER_STOP_AFTER_RECORDS = 100);
```

Each invocation accepts at most 100 records for writing in this process, or stops sooner at the saved ends. A saved `worker_record_cap` stop means the allowance was reached; rerun without `OPTIONS` to continue to those same ends, or keep it for another allowance of 100. [Counting rules](../design/decisions.md#worker-record-stop).

### Copy an offset range

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-offsets-0')
WHERE PARTITION IN (0)
  AND RECORD_OFFSET BETWEEN 100 AND 199;
```

`BETWEEN 100 AND 199` includes both offsets, from source partition 0. The partition must exist and retain start position 100 and end position 200 when the copy starts, or the copy fails. Offset gaps mean this need not produce 100 records.

### Copy selected partitions

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-partition-range')
WHERE PARTITION BETWEEN 0 AND 2
  AND RECORD_OFFSET < LATEST;
```

Copies source partitions 0, 1 and 2 to their saved readable ends; all three IDs must exist. Use `IN (0, 2)` for only those two IDs, or `NOT BETWEEN 0 AND 2` for every other partition, including eligible additions while the worker runs. [Partition selection](../design/decisions.md#source-partitions).

### Use different offsets per partition

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-offset-repair')
WHERE (PARTITION IN (0) AND RECORD_OFFSET BETWEEN 100 AND 199)
   OR (PARTITION IN (2) AND RECORD_OFFSET >= 900 AND RECORD_OFFSET < LATEST);
```

Copies offsets 100–199 from partition 0 and offsets from 900 up to the saved readable end of partition 2. Each branch needs its own partition selection and upper offset bound; selections must not overlap, and unlisted partitions are excluded. The same [available-position rules](../design/decisions.md#offset-ends) apply to numeric bounds.

## Copy records in a time range

Use the connection profiles and commands from [Kafka setup](#copy-between-kafka-topics).

### Within the current backlog

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-hour')
WHERE RECORD_OFFSET < LATEST
  AND RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z';
```

Copies Kafka record timestamps from 09:00 inclusive to 10:00 exclusive (UTC), not times inside the payload. Reading can continue from the first matching timestamp through each saved readable end, including records newer than the requested range. Matching records appended beyond those ends are excluded; timestamps only filter records in this example, without deciding when reading stops.

### Allow late arrivals

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WITH (GROUP = 'orders-hour-live',
      MAX_STALENESS = INTERVAL '5' MINUTE,
      MAX_OUT_OF_ORDER = INTERVAL '5' MINUTE)
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z';
```

Use this when records for the range are still arriving, or to finish an old range without reading to today's end. Both settings show their five-minute defaults; copying starts immediately:

- `MAX_STALENESS`: once Kaferry's clock passes 10:05, it captures each partition's current end, including unfinished transactions, saves it and reads up to it. Records appended beyond that saved end are excluded; if the deadline is already past, capture happens at startup.
- `MAX_OUT_OF_ORDER`: reading a record stamped strictly after 10:05 stops reading that partition, even before reaching its saved end. A later record stamped 09:59 can be missed, even if already present.

Increase the arrival-delay or disorder allowance to match your input, or disable the corresponding rule with `OFF`. Either rule can finish a partition independently, so increasing one does not protect against the other stopping early; neither setting limits total run time. [Exact rules and assumptions](../design/decisions.md#completion) · [Relative windows with `NOW()`](../design/grammar.md#valid-examples).

## Keep copying new records

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-follow');
```

Copies the retained backlog, then follows new records until you press Ctrl+C. Add `START = LATEST` inside source `WITH` to skip the backlog for a fresh group; saved progress takes precedence on resume. A partition filter without an offset end also keeps following.

## Archive a topic and replay it

### First copy with Docker

```sh
docker run --rm --mount source=kaferry-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest run -c tutorial.toml -f copy.sql
```

```text
{"time":"2026-10-25T22:39:06.595190641Z","level":"INFO","msg":"partition assigned","partition":0,"position":0,"window":{"lower":null,"upper":null}}
{"time":"2026-10-25T22:39:06.602132061Z","level":"INFO","msg":"copy summary","submitted":3,"confirmed":3,"filtered":0,"skipped":0,"timestamp_stops":0,"tip_stops":0,"peak_uncommitted_records":3,"peak_uncommitted_bytes":534}
```

This output is from a fresh `kaferry-demo` volume. If you ran the README example, continue with the status command below.

The image includes [copy.sql](../examples/archive-demo/copy.sql), its configuration and three sample Kafka records stored in Avro files. The command copies these records from `input` to `output` and saves progress for the named group `archive-demo`. A checkpoint records replay progress in a file. Docker keeps it with the input and output in the `kaferry-demo` volume after the container exits.

```sh
docker run --rm --mount source=kaferry-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest status -c tutorial.toml -f copy.sql
```

```text
Status: stopped
Group: "archive-demo"
Reason: file selection completed
  Source: ARCHIVE "input"
  Destination: ARCHIVE "output"

Saved progress
  Replay: input records 3; file selection completed at 3
  Snapshot: 1 completed files, 1498 encoded bytes.

Stopped means this group saved a stop reason for its fixed selection of archive files. A new group selects current completed files.
Activity: not checked for files.
```

This reads the group's saved progress from the volume.

```sh
docker run --rm --mount source=kaferry-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest run -c tutorial.toml -f copy.sql
```

```text
{"time":"2026-10-25T22:39:07.066648988Z","level":"INFO","msg":"partition assigned","partition":0,"position":3,"window":{"lower":null,"upper":null}}
{"time":"2026-10-25T22:39:07.066686447Z","level":"INFO","msg":"copy summary","submitted":0,"confirmed":0,"filtered":0,"skipped":0,"timestamp_stops":0,"tip_stops":0,"peak_uncommitted_records":0,"peak_uncommitted_bytes":0}
```

This resumes the same group from its saved position. The output remains in the volume across containers.

```sh
docker volume rm kaferry-demo
```

```text
kaferry-demo
```

This deletes the demo's input, output and checkpoints. Running the first command again starts a fresh copy. [Copy between Kafka topics](#copy-between-kafka-topics) · [Run your own files in a container](#containers).

### First copy without Kafka

Install `kaferry` on `PATH` ([installation](install.md)). This tutorial uses a shell, `curl` and `tar`, three synthetic records, and a fresh directory on local disk ([archive storage requirements](#archive-your-own-topic)). It needs no broker or credentials. Choose a [release](https://github.com/pagrus7/kaferry/releases) that includes `archive-demo.tar.gz` and matches your binary. Set `version` below to that release tag, then run the block in a directory that does not already contain `archive-demo`:

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

### Archive your own topic

Prepare the connection profiles, topics and permissions from [Kafka setup](#copy-between-kafka-topics) before these commands.

Use storage where successful saves survive a crash, file replacement happens as one operation, and locks prevent competing processes from updating the same state. Verify these guarantees across hosts for shared storage. [Storage contract](../design/archive.md#layout-and-ownership).

**Archive retained records, then follow new ones.** Create the parent directory:

```sh
mkdir -p archive
kaferry run -c connections.toml -f - <<'SQL'
INSERT INTO ARCHIVE 'archive/orders'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-archive');
SQL
```

Multiple workers can capture into one archive. Stop capture with Ctrl+C and wait for a successful exit before replaying its remaining records; [recover a failed capture](#capture-crashed) first.

**Replay completed files to Kafka.** A checkpoint is a file recording replay progress, stored outside the input archive. Kaferry creates the default directory, `~/.kaferry/checkpoints`, when replay starts. Run one replay process for this group:

```sh
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM ARCHIVE 'archive/orders' WITH (GROUP = 'orders-restore');"
```

Replay selects current completed files and exits after reading that selection; the same group keeps its selection and progress. Keep the checkpoint directory and existing output to resume; new archive files require a new group and can cause older records to be copied again. [Archive recovery and moving replay](#archive-procedures).

Pass SQL with `-f file`, `-f -` (stdin), or `-e "SQL"`, as above. [Archive rotation, routing and more SQL examples](../design/grammar.md#valid-examples).

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

Keep passwords out of SQL literals and shell arguments. SQL connection properties override TOML; use `-c connections.toml` to load profiles. [Configuration](reference.md#configuration-and-cli).

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

## Permissions

Kafka needs source topic/group READ and destination WRITE (including DESCRIBE); some brokers also need cluster IDEMPOTENT_WRITE. Without a named `GROUP`, grant group READ on the `kaferry-` prefix. File-only copies need no Kafka grants. [Optional DESCRIBE_CONFIGS](../design/client-compatibility.md#optional-destination-configuration).

## Stop and check results

SIGINT/SIGTERM stop reading, allow 10 s for writes, then save progress. File flushes can block saving. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. A second signal forces exit. See `copy summary` on stderr; check saved stops with:

```sh
kaferry status -c connections.toml -f copy.sql --json
```

`stopped` means every currently selected partition has a saved stop reason; it does not verify destination contents. Record limits can leave input unread, and [timestamp stops](#allow-late-arrivals) depend on their assumptions. Ctrl+C on a Kafka follow run leaves status `incomplete`. [Status and exit codes](reference.md#saved-status).

## Resume or start over

Rerun the same query with the same named `GROUP`, endpoints and checkpoint directory. Saved time windows and offset ends are reused; changing filters, completion allowances or routing needs a new group ([identity rules](../design/decisions.md#identity)). For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep existing output and saved progress. Kafka stores progress in source consumer-group offsets; archive replay uses checkpoint files. Expired offsets or missing checkpoints can start a fresh copy; crashes and Kafka rebalances can repeat or reorder writes. Keep selected completed archive files unchanged and retain lock files until replay completes. [Archive recovery and moving replay](#archive-procedures).

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
2. Copy the entire source archive and checkpoint directory, including hidden files and lock files, to storage meeting the [archive requirements](#archive-your-own-topic). Selected archive files must remain byte-for-byte unchanged.
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
  --mount "type=bind,src=$PWD,dst=/work" ghcr.io/pagrus7/kaferry:latest \
  run --file /work/copy.sql --config /work/connections.toml
```

Use container paths and reachable broker addresses. Set `worker.checkpoint_dir` to `/work/checkpoints` for resume; `--rm` deletes container-only files. The stop timeout must exceed `worker.shutdown_timeout` (30s by default). [Image verification](install.md#container-images).
