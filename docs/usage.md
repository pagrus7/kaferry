# Using Kaferry

## Resume or start over

Rerun the same SQL and `GROUP`; use a new group to change the time filter or `CUTOFF`. Saved progress overrides `START` and reuses each partition's or file's `NOW()` bounds; expired offsets or lost checkpoints can start fresh.

`GROUP = ONE_OFF` logs a new name each run. To resume an interrupted run, use that name as a quoted `GROUP`.

Keep file output, checkpoints and lock files. Reuse `worker.checkpoint_dir` (default `~/.kaferry/checkpoints`). Lost output or progress needs a new group and, for file output, a new path. [Recovery rules](../design/decisions.md#output-recovery).

## Workers

Each Kaferry process is a worker. Topic-to-topic workers share SQL and a named group. Use fixed timestamps; workers can resolve `NOW()` differently. Stop workers before upgrading a group. File queries use one worker.

## Configuration

SQL overrides TOML; topics without `@alias` use `default`. SQL-only `ENV()` fails if unset. Logs redact connection settings. Conflicting protected settings fail. `[worker]` sets [limits, timeouts and checkpoints](../design/decisions.md#worker-settings).

## Permissions

For existing topics on Kafka 3.9 StandardAuthorizer, Kafka input needs source topic/group READ; Kafka output needs destination WRITE. These imply DESCRIBE. File-only copies need no Kafka grants. No other grants are required on this tested setup. Other brokers/authorizers may require cluster IDEMPOTENT_WRITE.

Named groups permit exact grants; Kafka-source `ONE_OFF` needs the `kaferry-` group prefix.

Destination DESCRIBE_CONFIGS is optional; without it, Kaferry warns and skips checks that need those settings. [Fallbacks](../design/client-compatibility.md#optional-destination-configuration).

## Errors

Errors stop the copy by default. `ON_ERROR = SKIP` drops only identified records rejected before sending. Specific timestamp policies are alternatives to `ON_ERROR`. Broker rejections (including timestamps), authentication/authorization errors and corruption cannot be skipped. [Policies](../design/decisions.md#errors).

## Stopping and exit codes

SIGINT/SIGTERM stop reading, allow up to ten seconds for writes and save confirmed progress. Native Kafka or filesystem calls can still hang shutdown; use a supervisor timeout if needed. JSON logs, including `copy summary`, and final errors go to stderr.

[Exit codes](../design/decisions.md#cli-and-exit-codes) describe this worker's outcome, not completion by other workers. Native crashes can terminate by signal.
