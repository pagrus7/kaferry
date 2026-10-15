# Using Kaferry

## Resume or start over

Rerun the same SQL and `GROUP`; use a new group to change the time filter or `CUTOFF`. Saved progress overrides `START` and reuses each partition's or file's `NOW()` bounds; expired offsets or lost checkpoints can start fresh.

`GROUP = ONE_OFF` logs a new name each run. To resume an interrupted run, use that name as a quoted `GROUP`.

Keep file output, checkpoints and lock files. Reuse `worker.checkpoint_dir` (default `~/.kaferry/checkpoints`). If output or saved progress is lost, start over with a new group; if the destination is a file, also use a new path. Never recreate missing output under the old group. [Recovery rules](../design/decisions.md#output-recovery).

## Workers

Each Kaferry process is a worker. Topic-to-topic workers share SQL and a named group. Use fixed timestamps; workers can resolve `NOW()` differently. Stop all of a group's workers before moving progress or upgrading Kaferry. File queries use one worker.

## Configuration

SQL overrides TOML; topics without `@alias` use `default`. `ENV()` works only in SQL; its value is hidden in diagnostics, and an unset variable fails. Conflicting [protected settings](../design/decisions.md#protected-client-properties) fail. `[worker]` sets [limits, timeouts and checkpoints](../design/decisions.md#worker-settings).

## Permissions

For existing topics on Kafka 3.9 StandardAuthorizer, Kafka input needs only source topic/group READ; Kafka output needs only destination WRITE. These imply DESCRIBE. File-only copies need no Kafka grants. Other brokers/authorizers may require cluster IDEMPOTENT_WRITE.

Named groups permit exact grants; Kafka-source `ONE_OFF` needs the `kaferry-` group prefix.

Destination DESCRIBE_CONFIGS is optional; without it, Kaferry warns and skips checks that need those settings. [Fallbacks](../design/client-compatibility.md#optional-destination-configuration).

## Errors

Errors stop the copy by default (`FAIL`). `SKIP` drops only identified records rejected before sending; the broker can still reject records that pass. `ON_MISSING_TIMESTAMP` and `ON_REJECTED_TIMESTAMP` can be used together, but not with `ON_ERROR`. Under every policy, broker rejections (including timestamps), authentication, authorization and corruption errors stop the run. [Policies](../design/decisions.md#errors).

## Stopping and exit codes

SIGINT/SIGTERM stop reading, allow up to ten seconds for writes and save confirmed progress. Kafka or filesystem calls can hang shutdown; use a supervisor timeout if needed. JSON logs, including `copy summary`, and final errors go to stderr.

[Exit codes](../design/decisions.md#cli-and-exit-codes) describe only the exiting worker's outcome, not whether other workers finished. A native crash may end by signal.
