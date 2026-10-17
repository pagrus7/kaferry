# Using Kaferry

## Configuration

Create `connections.toml` from [quick start](../README.md#quick-start), including [authentication](../README.md#connections). `-c` loads it; without `-c`, no TOML is loaded. SQL overrides TOML; topics without `@alias` use `default`. SQL `ENV()` fails if unset. [Worker settings](../design/decisions.md#configuration-and-cli).

## Permissions

For existing topics on Kafka 3.9 StandardAuthorizer, Kafka input needs only source topic/group READ; Kafka output needs only destination WRITE. These imply DESCRIBE. File-only copies need no Kafka grants. Other brokers/authorizers may require cluster IDEMPOTENT_WRITE.

For Kafka-source `GROUP = ONE_OFF`, grant group READ on the `kaferry-` prefix.

Destination DESCRIBE_CONFIGS is optional; without it, Kaferry warns and skips checks that need those settings. [Fallbacks](../design/client-compatibility.md#optional-destination-configuration).

## Run a copy

Examples assume `kaferry` is on `PATH`; otherwise use its path. Pass SQL with `-f file`, `-f -` (stdin) or `-e "SQL"`. Help: `kaferry run -h`.

**Archive retained records, then follow new ones.** Create the output directory:

```sh
mkdir -p archive
kaferry run -c connections.toml -f - <<'SQL'
INSERT INTO FILE cbor('archive/orders.cbor')
SELECT * FROM TOPIC 'orders@prod'
OPTIONS (GROUP = 'orders-archive', START = EARLIEST);
SQL
```

Press Ctrl+C to stop archiving before restoring. For a new file-input job, create the checkpoint directory (default shown):

```sh
mkdir -p ~/.kaferry/checkpoints
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE cbor('archive/orders.cbor') OPTIONS (GROUP = 'orders-restore');"
```

Kafka input needs `WHERE` or `START`, not both. `EARLIEST` starts at the oldest retained record; `LATEST` copies records that become readable after startup. Saved progress overrides the start position. Use a dedicated `GROUP`. File input takes no `START` and reads up to its size at startup; later appends wait for the next run. [More examples](../design/grammar.md#valid-examples).

## Stop and check results

SIGINT/SIGTERM stop reading, allow up to ten seconds for writes and save confirmed progress. The CLI exits `1` if stopping takes longer than `worker.shutdown_timeout` (default 30 s); unsaved work may replay. Check stderr; `copy summary` reports `confirmed`, `filtered` and `skipped` counts.

[Exit codes](../design/decisions.md#cli-and-exit-codes) describe only this worker’s outcome. A second signal forces termination.

## Resume or start over

Resume with the same `GROUP`, endpoints, time filter and `CUTOFF`; use a new group for a new query. Saved `NOW()` bounds are reused.

`GROUP = ONE_OFF` logs a new `group` in `one-off group generated`. To resume an interrupted run, quote that name as `GROUP`.

Keep output, checkpoints and locks. Resume with the same `worker.checkpoint_dir` (default `~/.kaferry/checkpoints`). Lost output or progress requires a new group and, for file output, a new path. [Recovery rules](../design/decisions.md#output-recovery).

## Errors

Errors stop the copy by default. `ON_ERROR = SKIP` drops only identified records rejected before sending; broker rejections cannot be skipped. For a skippable error, add `ON_ERROR = SKIP` and rerun the same query. Skips appear in `copy summary`. [Policies](../design/decisions.md#errors), [SQL errors](../design/grammar.md#invalid-examples).

## Workers

For topic-to-topic copies, run the same command on each machine with identical SQL and named `GROUP`. Each process is a worker. Use fixed timestamps; `NOW()` can differ. Stop workers before upgrading Kaferry. File queries use one process.

If a worker exits without any assigned partitions, rerun the same query and group to pick up unfinished work.
