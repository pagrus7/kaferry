# Using Kaferry

## Configuration

Load TOML with `-c connections.toml`. SQL overrides TOML; topics without `@alias` use `default`. [Connection setup](../README.md#connections) · [Worker settings](../design/decisions.md#configuration-and-cli).

## Permissions

Kafka needs source topic/group READ and destination WRITE (including DESCRIBE); some brokers also need cluster IDEMPOTENT_WRITE. For `ONE_OFF`, grant group READ on the `kaferry-` prefix. File-only copies need no Kafka grants. [Optional DESCRIBE_CONFIGS](../design/client-compatibility.md#optional-destination-configuration).

## Preview a copy

```sh
kaferry explain -c connections.toml -f copy.sql
kaferry explain -c connections.toml -f copy.sql --json
```

Shows endpoints, progress, windows and policies without joining, writing or locking. JSON lists every partition. Records, bytes, duration and write access are not checked. Fresh `NOW()` resolves again when run. [Output contract](../design/decisions.md#inspection).

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

Press Ctrl+C to stop archiving before restoring. Before file input, create its checkpoint directory:

```sh
mkdir -p ~/.kaferry/checkpoints
kaferry run -c connections.toml -e "INSERT INTO TOPIC 'orders@backup'
SELECT * FROM FILE cbor('archive/orders.cbor') OPTIONS (GROUP = 'orders-restore');"
```

Kafka input needs `WHERE` or `START`: `EARLIEST` for retained records or `LATEST` for new ones. Saved progress takes precedence. File input reads up to its opening size. [Examples](../design/grammar.md#valid-examples).

## Stop and check results

SIGINT/SIGTERM stop reading, drain writes for up to 10 s, then save progress. A second signal forces termination. At `worker.shutdown_timeout` (default 30 s), the CLI exits `1`; unsaved work may replay. Read counts in `copy summary` on stderr; check saved stops with:

```sh
kaferry status -c connections.toml -f copy.sql --json
```

`stopped` means a stop was saved for every current partition; the cutoff rule can still miss records within the time range. Kafka activity is checked twice, without an atomic snapshot; file activity is not checked. Ctrl+C on a Kafka follow run leaves status `incomplete`. [Status and exit codes](../design/decisions.md#saved-status).

## Resume or start over

Rerun with the same `GROUP`, endpoints, filter, `CUTOFF` and checkpoint directory (default `~/.kaferry/checkpoints`). Saved `NOW()` bounds are reused. For `GROUP = ONE_OFF`, resume with the logged group name quoted as `GROUP`.

Keep output, checkpoints and locks; losing them requires a new group/output. [Recovery rules](../design/decisions.md#output-recovery).

## Errors

Errors stop the copy by default. `ON_ERROR = SKIP` drops identified records rejected before sending; broker rejections cannot be skipped. Add it and rerun for skippable errors; skips appear in `copy summary`. [Policies](../design/decisions.md#errors) · [SQL errors](../design/grammar.md#invalid-examples).

## Workers

For topic-to-topic copies, run identical SQL and a dedicated `GROUP` on each machine. Use fixed timestamps; `NOW()` can differ. Stop workers before upgrading. File queries use one process. Rerun unassigned workers to pick up unfinished work.

## Monitoring

Progress logs every 30 s. [Configure logs and optional metrics](../design/decisions.md#observability).
