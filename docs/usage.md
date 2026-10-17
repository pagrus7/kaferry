# Using Kaferry

## Configuration

Pass `-c connections.toml` to load TOML; it is not loaded otherwise. SQL overrides TOML; topics without `@alias` use `default`. [Connection setup](../README.md#connections) · [Worker settings](../design/decisions.md#configuration-and-cli).

## Permissions

Kafka 3.9 StandardAuthorizer: source topic/group READ; destination WRITE. These imply DESCRIBE. Other brokers may require cluster IDEMPOTENT_WRITE. File-only copies need no Kafka grants. For Kafka-source `GROUP = ONE_OFF`, grant group READ on the `kaferry-` prefix. Destination DESCRIBE_CONFIGS is optional; if denied, [dependent checks are skipped](../design/client-compatibility.md#optional-destination-configuration).

## Preview a copy

```sh
kaferry explain -c connections.toml -f copy.sql
kaferry explain -c connections.toml -f copy.sql --json
```

Shows endpoints, saved progress and starting positions. It never joins a group, commits, creates files or takes locks. Kafka spans count offsets, not records, and do not predict completion. Without saved progress, `NOW()` resolves again when you run. Listed run permissions are not verified. [Output contract](../design/decisions.md#inspection).

## Run a copy

Pass SQL with `-f file`, `-f -` (stdin) or `-e "SQL"`. Both commands accept `-h`.

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

Kafka input needs `WHERE` or `START`, not both. `EARLIEST` means oldest retained; `LATEST` means records readable after startup. Saved progress determines the start. File input takes no `START` and reads up to its size when the run starts. [Examples](../design/grammar.md#valid-examples).

## Stop and check results

SIGINT/SIGTERM stop reading, allow ten seconds for writes, then save confirmed progress. A second signal forces termination. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. Check `copy summary` on stderr for `confirmed`, `filtered` and `skipped` counts. [Exit codes](../design/decisions.md#cli-and-exit-codes) describe only this worker's outcome.

## Resume or start over

Rerun with the same `GROUP`, endpoints, filter, `CUTOFF` and checkpoint directory (default `~/.kaferry/checkpoints`). Saved `NOW()` bounds are reused. For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep output, checkpoints and locks. Lost output or progress requires a new group and, for file output, a new path. [Recovery rules](../design/decisions.md#output-recovery).

## Errors

Errors stop the copy by default. `ON_ERROR = SKIP` drops identified records rejected before sending; broker rejections cannot be skipped. Add it and rerun for skippable errors; skips appear in `copy summary`. [Policies](../design/decisions.md#errors) · [SQL errors](../design/grammar.md#invalid-examples).

## Workers

For topic-to-topic copies, run identical SQL and a dedicated named `GROUP` on each machine. Each process is a worker. Use fixed timestamps; `NOW()` can differ. Stop workers before upgrading. File queries use one process. Rerun workers that received no partition assignment to pick up unfinished work.
