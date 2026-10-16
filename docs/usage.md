# Using Kaferry

## Run a copy

Use [quick start](../README.md#quick-start) SQL and profiles; add its authentication prefix where needed. Use your binary path. Choose one of `-f file`, `-f -` (stdin) or `-e "SQL"`; `-c` loads TOML. Flags: `kaferry run -h`.

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

Kafka input needs `WHERE` or `START` (`EARLIEST`: retained; `LATEST`: readable end), not both. Saved commits win; `WHERE` filters record timestamps. Use a dedicated `GROUP`. File input has no `START` and ends at startup size. [File-to-file and other examples](../design/grammar.md#valid-examples).

## Resume or start over

Resume with the same `GROUP`, endpoints, time filter and `CUTOFF`; use a new group for a new query. Error policies may change. Saved `NOW()` bounds are reused.

`GROUP = ONE_OFF` logs a new `group` in `one-off group generated`. To resume an interrupted run, quote that name as `GROUP`.

Keep output, checkpoints and locks. Resume with the same `worker.checkpoint_dir` (default `~/.kaferry/checkpoints`). Lost output or progress requires a new group and, for file output, a new path. [Recovery rules](../design/decisions.md#output-recovery).

## Workers

For topic-to-topic copies, run the same command on each machine with identical SQL and named `GROUP`. Each process is a worker. Use fixed timestamps; `NOW()` can differ. Stop workers before moving progress or upgrading Kaferry. File queries use one process.

## Configuration

Without `-c`, no TOML is loaded. SQL overrides TOML; topics without `@alias` use `default`. `ENV()` is SQL-only and fails if unset. [Worker limits and protected settings](../design/decisions.md#configuration-and-cli).

## Permissions

For existing topics on Kafka 3.9 StandardAuthorizer, Kafka input needs only source topic/group READ; Kafka output needs only destination WRITE. These imply DESCRIBE. File-only copies need no Kafka grants. Other brokers/authorizers may require cluster IDEMPOTENT_WRITE.

Kafka-source `ONE_OFF` needs the `kaferry-` group prefix.

Destination DESCRIBE_CONFIGS is optional; without it, Kaferry warns and skips checks that need those settings. [Fallbacks](../design/client-compatibility.md#optional-destination-configuration).

## Errors

Errors stop the copy by default. `ON_ERROR = SKIP` drops only identified records rejected before sending; broker rejections cannot be skipped. [Policies](../design/decisions.md#errors), [SQL errors](../design/grammar.md#invalid-examples).

## Stopping and exit codes

SIGINT/SIGTERM stop reading, allow up to ten seconds for writes and save confirmed progress. The CLI exits `1` if stopping takes longer than `worker.shutdown_timeout` (default 30 s); unsaved work may replay. Read stderr for errors and JSON logs; `copy summary` reports `confirmed`, `filtered` and `skipped` counts.

[Exit codes](../design/decisions.md#cli-and-exit-codes) describe only the exiting worker's outcome, not whether other workers finished. A second signal forces termination.
