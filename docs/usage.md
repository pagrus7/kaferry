# Using Kaferry

Try the [one-command Kafka demo](../README.md#docker-quick-start), or run `kaferry help` for a first copy with an installed CLI. To experiment on a broker you can keep running, use the [local playground](#local-kafka-playground).

## Copy between Kafka topics

Install `kaferry` on `PATH` ([installation](install.md)). Create both topics first; topic settings, ACLs and consumer-group offsets are not copied. These examples keep original partition numbers, so the destination needs every selected source partition ID; add `WITH (PARTITIONING = KEY)` after the destination topic to route by key instead ([routing](reference.md#kafka-sink)).

Replace the addresses in **`connections.toml`**; add [authentication](#connections) and the required [permissions](#permissions). Kaferry copies raw bytes: ensure their formats and Schema Registry IDs work at the destination. Destination broker settings can replace timestamps or delete old records soon after copying; see [Kafka output](reference.md#kafka-sink) and [client limits](design/client-compatibility.md#kafka-input-timestamps).

```toml
[connections.prod]
'bootstrap.servers' = 'source.example:9092'

[connections.backup]
'bootstrap.servers' = 'destination.example:9092'
```

Save this query as **`copy.sql`**. It copies the current backlog and stops at each partition's [saved readable end](reference.md#how-a-copy-ends). The source and destination use the `prod` and `backup` profiles above.

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-snapshot');
```

Preview the query, run it, then inspect the group's saved progress:

```sh
kaferry explain -c connections.toml -f copy.sql
kaferry run -c connections.toml -f copy.sql
kaferry status -c connections.toml -f copy.sql --json
```

The preview checks current metadata without testing write access. Rerunning with the same group reuses the saved ends. A new or omitted group can copy records again. Use the same commands for the recipes below. For a following copy, press Ctrl+C before checking status.

### Try a small copy first

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-preview')
OPTIONS (WORKER_STOP_AFTER_RECORDS = 100);
```

This worker accepts up to 100 records for writing. A saved `worker_record_cap` stop means it reached that allowance. Rerun without `OPTIONS` to continue to the same ends, or keep it for another 100. [What counts toward the cap](reference.md#final-options).

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
WHERE PARTITION BETWEEN 0 AND 2;
```

Copies source partitions 0, 1 and 2 to their saved readable ends; all three IDs must exist. Use `IN (0, 2)` for only those two IDs, or `NOT BETWEEN 0 AND 2` for every other partition, including eligible additions while the worker runs. Exited workers do not discover new partitions.

### Use different offsets per partition

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-offset-repair')
WHERE (PARTITION IN (0) AND RECORD_OFFSET BETWEEN 100 AND 199)
   OR (PARTITION IN (2) AND RECORD_OFFSET >= 900 AND RECORD_OFFSET < LATEST);
```

Copies offsets 100–199 from partition 0 and offsets from 900 up to the saved readable end of partition 2. Each branch needs its own partition selection and upper offset bound; selections must not overlap, and unlisted partitions are excluded. Numeric starts and exclusive ends must be within retained positions when the copy starts, as in the [single-partition example](#copy-an-offset-range).

## Copy records in a time range

Use the [Kafka setup](#copy-between-kafka-topics) to copy one hour of the current backlog.

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-hour')
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z';
```

Copies records with Kafka timestamps from 09:00 inclusive to 10:00 exclusive (UTC). The scan continues to each partition's [saved end](reference.md#how-a-copy-ends).

### Shorten a historical scan

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod'
WITH (GROUP = 'orders-hour-historical',
      MAX_OUT_OF_ORDER = INTERVAL '5' MINUTE)
WHERE RECORD_TIMESTAMP >= TIMESTAMP '2026-10-01T09:00:00Z'
  AND RECORD_TIMESTAMP <  TIMESTAMP '2026-10-01T10:00:00Z';
```

With this allowance, the first visible record stamped after 10:05 UTC can stop that partition's scan. A later record stamped 09:59 can be missed. Schedule the copy after the window closes if late arrivals should be eligible. [Timestamp stop rule](reference.md#how-a-copy-ends).

## Keep copying new records

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-follow', FOLLOW = ON);
```

Copies the retained backlog, then follows new records until you press Ctrl+C. Add `START = LATEST` inside source `WITH` to skip the backlog for a fresh group; saved progress takes precedence on resume. A partition filter can restrict which partitions to follow.

## Archive a topic and replay it

### First copy with Docker

The CLI image bundles the same three-record archive used in the [local tutorial](#first-copy-without-kafka). With a fresh volume, this command confirms three writes:

```sh
docker run --rm --mount source=kaferry-archive-demo,target=/demo --workdir /demo ghcr.io/pagrus7/kaferry:latest run -c tutorial.toml -f copy.sql
```

Repeat the command to resume. It confirms zero new writes because the volume retains both output and checkpoint. To inspect the saved progress, replace `run -c` with `status -c`. Remove the volume with `docker volume rm kaferry-archive-demo` when you are finished with its data.

### First copy without Kafka

Install [Kaferry](install.md) and choose a [matching release](https://github.com/pagrus7/kaferry/releases) with `archive-demo.tar.gz`. In a fresh local directory, set the tag and unpack three sample records:

```sh
version=vX.Y.Z
curl -fLO "https://github.com/pagrus7/kaferry/releases/download/$version/archive-demo.tar.gz"
tar -xzf archive-demo.tar.gz
cd archive-demo
```

The bundled SQL copies `input` to `output` and saves progress for group `archive-demo` in `checkpoints`. Preview, copy, inspect and rerun:

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

The first `copy summary` reports `submitted: 3` and `confirmed: 3`. This status JSON excerpt shows the verdict and saved progress.

```json
{
  "version": 3,
  "verdict": "complete",
  "reason": "file selection completed",
  "meaning": "Source processing finished for the selected finite scope: saved offset ends or the saved archive selection.",
  "partitions": [{"id": 0, "position": 3, "stop": {"reason": "eof", "at": 3}}]
}
```

The rerun confirms zero writes. Keep `output` and `checkpoints` together. Saved status cannot verify or recreate deleted output. To start fresh, unpack the bundle in a new directory.

### Archive your own topic

Prepare the connection profiles, topics and permissions from [Kafka setup](#copy-between-kafka-topics) before these commands.

Use storage where successful saves survive a crash, file replacement happens as one operation, and locks prevent competing processes from updating the same state. Verify these guarantees across hosts for shared storage. [Storage contract](design/archive.md#layout-and-ownership).

**Archive retained records, then follow new ones.** Create the parent directory:

```sh
mkdir -p archive
kaferry run -c connections.toml -f - <<'SQL'
INSERT INTO ARCHIVE 'archive/orders'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-archive', FOLLOW = ON);
SQL
```

Multiple workers can capture into one archive. Stop capture with Ctrl+C and wait for a successful exit before replaying its remaining records; [recover a failed capture](#capture-crashed) first.

**Replay completed files to Kafka.** A checkpoint is a file recording replay progress, stored outside the input archive. Kaferry creates the default directory, `~/.kaferry/checkpoints`, when replay starts. Run one replay process for this group:

```sh
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM ARCHIVE 'archive/orders' WITH (GROUP = 'orders-restore');"
```

Keep the checkpoint directory and existing output to resume. See [replay selection and progress](design/archive.md#replay-and-progress) and [archive recovery and moving replay](#archive-procedures).

Pass SQL with `-f file`, `-f -` (stdin), or `-e "SQL"`, as above. [SQL forms and endpoint settings](reference.md#sql-statements).

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

TOML does not expand environment variables. Set `PROD_PASSWORD` in the environment, then add a `WITH CONNECTION` prefix using `ENV()` to each SQL file that uses `prod`. For example, `copy.sql` becomes:

```sql
WITH CONNECTION prod AS ('sasl.password' = ENV('PROD_PASSWORD'))
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-snapshot');
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

Set `KAFKA_OAUTH_SECRET` in the environment and prefix each SQL file that uses `prod` with:

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

Release checks do not contact MSK; IAM signing and credential refresh have component tests only.

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

Grant the source principal topic/group READ and the destination principal topic WRITE (including DESCRIBE). The destination principal also needs cluster IDEMPOTENT_WRITE on brokers that require it for idempotent producers. Without a named `GROUP`, grant the source principal group READ on the `kaferry-` prefix. File-only copies need no Kafka grants. [Optional DESCRIBE_CONFIGS](design/client-compatibility.md#optional-destination-configuration).

## Stop and check results

SIGINT/SIGTERM stop reading, allow 10 s for writes, then save progress. File flushes can block saving. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. A second signal forces exit. See `copy summary` on stderr; check saved stops with:

```sh
kaferry status -c connections.toml -f copy.sql --json
```

A clean Ctrl+C can leave a following copy at `incomplete`. Use [saved status](reference.md#saved-status) to check progress across the group and [exit codes](reference.md#exit-codes) to check how this worker stopped.

## Resume or start over

Rerun the same query with the same named `GROUP`, endpoints and checkpoint directory. Saved time windows and offset ends are reused; changing filters, completion allowances or routing needs a new group. For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep existing output and saved progress. Kafka stores progress in source consumer-group offsets; archive replay uses checkpoint files. Expired offsets or missing checkpoints can start a fresh copy; crashes and Kafka rebalances can repeat or reorder writes. Keep selected completed archive files unchanged and retain lock files until replay completes. [Archive recovery and moving replay](#archive-procedures).

### Recover after destination topic recreation

A replacement topic has a new UUID. Old workers can write to it before detecting the change. Disable automatic restarts, stop every worker and inspect the original query and group:

```sh
kaferry status -c connections.toml -f copy.sql --json > progress.json
```

A fingerprint conflict returns `unknown` (exit 5). For Kafka, confirm the group is idle in `activity` and use only [validated saved positions](reference.md#saved-status).

**Start fresh.** Use a new group to copy the retained source. It captures new ends, resolves `NOW()` again and can duplicate records already in the replacement. Use this option for archive replay, whose positions count examined records.

**Continue from Kafka offsets.** This skips records lost with the old topic before each saved position. Start at each partition's exact `position` and stop at its saved exclusive `captured_end`. For example:

```sql
INSERT INTO TOPIC 'orders@backup'
SELECT * FROM TOPIC 'orders@prod' WITH (GROUP = 'orders-recovery')
WHERE (PARTITION IN (0) AND RECORD_OFFSET >= 1001 AND RECORD_OFFSET < 2000)
   OR (PARTITION IN (1) AND RECORD_OFFSET >= 801 AND RECORD_OFFSET < 1500);
```

Use only partitions selected by the original query:

- Omit partitions stopped at `tip` or `timestamp`, or whose `position` equals `captured_end`.
- For saved time windows, replace entire timestamp predicates with `RECORD_TIMESTAMP >= window.lower` and `RECORD_TIMESTAMP < window.upper`, including their operators. Convert Unix milliseconds to RFC 3339, omit null bounds, and use separate queries and groups for different windows.
- A position below the current retained start fails. Choose a start for selected partitions without saved positions.

To resume following, run one query per original partition with its own new group, `FOLLOW = ON`, and `RECORD_OFFSET >= position`. These queries exclude later partitions. Writes after the last save can repeat.

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

Keep the whole archive directory, including hidden files, and retain the source Kafka group. Restart the same capture query. Startup recovers available writer namespaces under their locks, publishes acknowledged data, and then resumes copying. Do not delete private files, recovery state or lock files. Missing or corrupt acknowledged data stops recovery; preserve it for investigation rather than starting over in place. Records written before a crash but not saved in source progress can repeat. [Storage contract](design/archive.md#capture-and-recovery).

### Move replay to another machine

1. Stop replay and all capture writers for this archive, and wait for successful exits. Recover a failed capture before moving its archive. Keep the destination’s existing output.
2. Copy the entire source archive and checkpoint directory, including hidden files and lock files, to storage meeting the [archive requirements](#archive-your-own-topic). Selected archive files must remain byte-for-byte unchanged.
3. Create the destination checkpoint directory if needed and set `worker.checkpoint_dir` to its new path. Update archive paths and connection details while keeping the named group, query and destination identity. For a file destination, move its existing output as well; paths alone do not prove that output is present.
4. Run `explain` to check that saved progress is found, then rerun the same copy. Destination-confirmed records after the last saved checkpoint may repeat.

Use only one replay process for a group/checkpoint directory. A different checkpoint directory with no saved state starts fresh and can duplicate output. [Checkpoint contract](design/archive.md#checkpoints).

### Capture is still running

To include capture’s remaining records in a [fresh replay selection](design/archive.md#replay-and-progress), stop every intended capture worker and wait for successful exits. If a capture failed, recover it before selecting replay input. Preview a fresh selection to check for private-file warnings:

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
| Copy fails on a record | Read the error and `copy summary`. Only identified synchronous validation failures support SKIP; delivery and storage failures stop the copy. | Fix the cause and resume the same group. Use SKIP only when deliberately dropping that rejected record; async broker failures cannot be skipped. [Policies](reference.md#record-error-policies). |
| Rerun copies records again | Compare the logged group and saved progress. An unnamed run, missing checkpoint, expired offsets, or writes after the last save can repeat data. | Restore the intended progress and destination together, or resume the same named group. Previously repeated output remains and may need downstream handling. |
| Replay omits recent capture output | Compare its fixed selection with completed files; private or later-published files are outside that selection. | Follow [capture completion](#capture-is-still-running). A new group can replay older files again. |
| Status is `limit_reached` | A worker record cap stopped source processing. | Rerun the same job for another allowance. |

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

## Local Kafka playground

To try your own queries on a disposable broker, install `kaferry` on PATH and start Docker. Use a fresh directory, free port 9092 and an unused container name `kaferry-kafka-demo`. The setup creates two topics and seeds three records.

<!-- kafka-quickstart-setup -->
```sh
docker run -d --name kaferry-kafka-demo -p 127.0.0.1:9092:9092 apache/kafka:3.9.2
docker exec kaferry-kafka-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders --partitions 1 --replication-factor 1
docker exec kaferry-kafka-demo /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders-copy --partitions 1 --replication-factor 1
printf 'order-1\norder-2\norder-3\n' | docker exec -i kaferry-kafka-demo /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders --sync --producer-property acks=all
```

Copy the current readable records and exit. [How a copy ends](reference.md#how-a-copy-ends).

<!-- kafka-quickstart-copy -->
```sh
cat > copy.sql <<'SQL'
WITH CONNECTION default AS ('bootstrap.servers' = 'localhost:9092')
INSERT INTO TOPIC 'orders-copy'
SELECT * FROM TOPIC 'orders' WITH (GROUP = 'demo');
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

## Containers

For released images (Linux AMD64/ARM64), run the [README command](../README.md#docker-quick-start) from the directory containing `connections.toml` and `copy.sql`. It mounts that directory at `/work` and runs as your host user. Use `/work/...` paths in SQL and TOML; your user must be able to read inputs and write output directories.

For the running [local Kafka playground](#local-kafka-playground), insert `--network container:kaferry-kafka-demo` before the image in the README command. Sharing the broker's network makes its advertised `localhost:9092` reachable from Kaferry. This was runtime-tested on Linux Docker Engine (AMD64/ARM64) and macOS arm64 with OrbStack; Docker Desktop running Linux containers on macOS follows [Docker's shared-network contract](https://docs.docker.com/engine/network/#container-networks), but was not runtime-tested. Keep the broker running until the copy finishes. Replacing only the bootstrap address with `host.docker.internal` does not fix the broker's advertised `localhost` address.

For example, keep the playground's `copy.sql` and create `connections.toml` in the same directory:

```toml
[connections.default]
'bootstrap.servers' = 'localhost:9092'
```

For other brokers, both bootstrap and advertised addresses must be reachable from the container. A broker running directly on a Linux host and advertising `localhost` can use `--network host`; this is a different network mode from sharing the playground container.

For archive replay, run `mkdir -p checkpoints` on the host and set `worker.checkpoint_dir = '/work/checkpoints'` in TOML. Keep archive output under `/work` too. The bind mount preserves these files after `--rm` removes the container. Stop a following copy with `docker stop CONTAINER` (find its ID with `docker ps`) and wait for a successful exit before resuming. The command's 35-second stop timeout exceeds `worker.shutdown_timeout` (30s by default); increase it if you raise that setting. [Image verification](install.md#container-images).
