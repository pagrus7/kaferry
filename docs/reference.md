# Settings and results reference

Exact settings, inspection output, exit codes and metrics. For operating steps, see [usage](usage.md); for delivery and restart rules, see [copy behavior](../design/decisions.md).

## Inspection

`explain` previews the same statement, configuration, identity and start-position rules as `run`. It never joins a group, consumes records, commits, creates/truncates files or takes locks. Observations can change during inspection; this is not an atomic snapshot or a guarantee that a later run succeeds. Write access is not checked. Records, output bytes and duration are not estimated.

Text sections show endpoints, progress, time windows, behavior and any warnings. They summarize saved/fresh partitions, resolved bounds, start/stop rules, destination routing and record-error policies. Identical bounds and origins share a window summary. At most eight window groups, eight partition IDs per group and eight warnings are printed; omitted counts point to `--json`. Cluster IDs are shown separately from connection aliases; fingerprints and topic IDs stay in JSON.

Kafka JSON rows show the selected start and retained/log-end offsets; log end includes open transactions. Offset differences are not record counts or completion evidence. Fresh `NOW()` values are preview-only.

Archive inspection reads completed-file headers, sizes and saved checkpoint data without scanning record payloads, taking locks or recovering private files. Source positions count confirmed examined input records. `ONE_OFF` never generates a group or reads an existing checkpoint; the preview says a fresh group would be used.

`--json` emits version 2 with all partitions and warnings. Time bounds use signed Unix milliseconds; byte and offset fields are counts. Connection properties and SQL text are excluded.

| Fields | Meaning |
| --- | --- |
| `version`, `fingerprint`, `group`, `one_off`, `max_staleness_ms`, `max_out_of_order_ms` | Report format and query identity; one-off group is `ONE_OFF`. `max_staleness_ms=-1` disables capture; `max_out_of_order_ms=null` disables timestamp stopping |
| `start_rule` | Kafka fallback for partitions without progress: `WINDOW`, `OFFSET`, `EARLIEST` or `LATEST`; absent for file sources |
| `record_errors` | Effective `FAIL`/`SKIP` policies: `missing_timestamp`, `rejected_timestamp`, `other_record`; SKIP applies before writing, not to delivery failures |
| `source`, `destination` | `kind`, `name`, optional connection alias, cluster/topic IDs, partition count, destination timestamp type and `partitioning` (`PRESERVE` or `KEY` for a Kafka destination) |
| Endpoint `file` | `exists`; source adds `size_bytes`, `file_count`, `position_records`, `window`, `window_origin`, `checkpoint`, `checkpoint_exists`. Destination `size_bytes` is zero; destination size and file count are not inspected |
| `source.partition_filter` | Optional `{exclude, ids}` predicate; `exclude=true` means NOT IN. Kafka discovers eligible added partitions while workers run; archives retain their saved file selection |
| `partitions` | Sorted `partition`, `start`, `log_start`, `log_end`, `saved`, `window`, `window_origin`; optional `captured_end` is the exclusive end offset; `end_origin` distinguishes `saved`, `literal` and unsaved `preview` |
| `offset_range`, `partition_offset_ranges` | Optional symbolic offset criteria; per-partition entries include original partition IDs |
| `window` | Inclusive `lower`, exclusive `upper`; null means unbounded. `window_origin`: `literal`, `saved`, `adopted` or `resolved` for preview-only NOW |
| `warnings` | Detected retention gaps and unavailable/changed destination timestamp behavior; may be empty |

### Saved status

`kaferry status` accepts the same query and configuration as `run`; a named `GROUP` is required. It reads saved progress without joining, consuming, producing, committing, creating files or taking locks. It needs no permissions beyond existing source/group DESCRIBE and endpoint metadata access. `ONE_OFF` requires the actual logged group name; a deleted file checkpoint has no saved status.

Verdicts: invalid/incompatible progress → `unknown`; incompatible active members → `unknown`; active members, unsettled group state or changed membership → `running`; no saved progress → `unknown`; any current partition without a saved stop → `incomplete`; otherwise → `stopped`. Added eligible partitions count. Kafka membership is read before and after offsets; observations are not atomic and cannot detect every concurrent start. File activity is not checked. New archive files do not expand a saved replay selection. Status reports `unknown` (5) when a checkpoint or saved selection cannot be validated; other operational read failures exit 1 without a verdict.

`stopped` means every currently selected partition has a saved stop reason. More source records may remain; timestamp completion depends on the configured arrival-delay and disorder assumptions. Workers remain independent: they never wait for other workers' stops.

Kafka text lists up to eight partitions; file text reports replay progress. `--json` includes all. JSON version `1` contains `verdict`, optional human-readable `reason`, `meaning`, `group`, `fingerprint`, source/destination endpoints, `activity`, and `partitions` (`id`, optional saved `position`, `stop`, `window`, `captured_end`). Kafka positions are offsets; file positions count confirmed examined input records. File `size_bytes` and `file_count` describe the saved selection, or current completed files before the first run. Inspection checks selected headers and sizes for incomplete groups, not payload checksums. Status does not scan records or verify destination writes.

## Configuration and CLI

Connections use TOML profiles or `WITH CONNECTION`; SQL keys override TOML. Endpoint `WITH` configures source progress/start rules or destination rotation/partitioning; optional final `OPTIONS` holds worker stops and all record-error policies. `ENV('NAME')` resolves at startup, fails if unset and is redacted. Worker settings are in TOML.

Profiles can be shared by sources and destinations. Supported connection keys are mapped to franz-go options; other keys fail before endpoints are opened, without displaying property values. A property name does not imply full librdkafka compatibility.

### Endpoint WITH properties

Source properties follow `FROM`; sink properties follow `INSERT INTO`. Unknown or inapplicable properties fail. Final `OPTIONS` contains worker stops and record-error policies. [Syntax and examples](../design/grammar.md).

#### Kafka source

| Property | Default | Meaning |
| --- | --- | --- |
| `GROUP` | `ONE_OFF` | Named consumer-group progress, or a generated independent group per invocation |
| `START` | `EARLIEST` | Initial position without timestamp bounds; `LATEST` starts at the readable committed end. Saved progress takes precedence; new partitions in an established group start at earliest retained data |
| `MAX_STALENESS` | `INTERVAL '5' MINUTE` with a timestamp upper bound | Capture replicated ends after the interval end plus this allowance; `OFF` disables wall-clock capture |
| `MAX_OUT_OF_ORDER` | Inherits `MAX_STALENESS` | Allowance before timestamp progress can finish a partition; `OFF` disables timestamp stopping |

Both allowances require an upper timestamp bound and reject an explicit offset end. Either enabled rule can establish completion. Raising only one allowance does not protect against an invalid assumption used by the other. Both `OFF` means following filtered records until another stop or a signal. Neither allowance limits total run time. [Completion guarantees](../design/decisions.md#completion).

#### File source

| Property | Default | Meaning |
| --- | --- | --- |
| `GROUP` | `ONE_OFF` | Checkpoint containing the pinned completed-file selection and physical replay cursors |
| `MAX_OUT_OF_ORDER` | `OFF` | Optional timestamp-disorder allowance per original source identity and partition; requires an upper timestamp bound and rejects an explicit offset end |

`START` and `MAX_STALENESS` are Kafka-only. New files do not expand a saved selection. Timestamp and original-offset criteria filter archived records; filesystem modification times and byte positions are not record criteria. [File replay](../design/archive.md#replay-and-progress).

#### Kafka sink

| Property | Default | Meaning |
| --- | --- | --- |
| `PARTITIONING` | `PRESERVE` | Preserve original partition numbers, or use `KEY` for key-based routing |

#### File sink

| Property | Default | Meaning |
| --- | --- | --- |
| `ROTATE_AFTER_SIZE` | `1g` | Compressed container size target; binary k/m/g suffixes |
| `ROTATE_AFTER_AGE` | `INTERVAL '1' HOUR` | Elapsed file age before rotation |

[Rotation and publication](../design/archive.md#rotation).

### Worker settings

Optional keys in TOML's `[worker]` table; byte values are integers:

| Key | Default | Meaning |
| --- | --- | --- |
| `partition_record_cap`, `partition_byte_cap` | 5000, 16777216 (16 MiB) | Unsaved work per partition |
| `record_cap`, `byte_cap` | 20000, 134217728 (128 MiB) | Unsaved work per worker; also sizes Kafka queues |
| `max_frame_bytes` | 67108864 (64 MiB) | Maximum decoded Avro block |
| `archive_capture_open_files` | 1024 | Open capture files; reaching this budget rotates the least recently used file |
| `archive_replay_open_files` | 32 | Open replay handles |
| `archive_replay_blocks` | 8 | Cached replay blocks; overlapping files beyond this budget repeat decoding |
| `delivery_timeout` | `'60s'` | Delivery deadline and queue-full wait |
| `shutdown_timeout` | `'30s'` | Whole-process stop deadline |
| `idle_exit` | `'30s'` | Wait after an empty assignment before exit 3 |
| `tip_wait` | `'15m'` | Continuous wait at the readable tip below the saved captured end; ordinary backlog processing resets it |
| `progress_interval` | '30s' | Progress logs; '0' disables |
| `checkpoint_dir` | `~/.kaferry/checkpoints` | File-source checkpoint directory |

Caps must be positive; `max_frame_bytes` is at most 4,294,967,295. Durations are positive Go duration strings, except `progress_interval` permits zero; `delivery_timeout` is 1 ms–2,147,483,647 ms. `checkpoint_dir` must not be empty. Unknown keys fail.

Caps include delivered records whose source progress is not yet saved. [Delivery and progress](../design/decisions.md#delivery-and-progress).

### Connection properties

Values are strings in TOML and SQL. Unknown keys, unsupported values and contradictory aliases fail; they are never silently ignored. Authentication and TLS paths are validated before output opens.

| Keys | Meaning / limits |
| --- | --- |
| `bootstrap.servers` | Required comma-separated host[:port] entries, no protocol prefix |
| `security.protocol` | `PLAINTEXT` (default), `SSL`, `SASL_PLAINTEXT`, `SASL_SSL`. `ssl.*` keys require SSL or SASL_SSL; `sasl.*` keys require SASL_PLAINTEXT or SASL_SSL |
| `sasl.mechanism`, `sasl.username`, `sasl.password` | PLAIN (default), SCRAM-SHA-256 or SCRAM-SHA-512; credentials required for SASL |
| `ssl.ca.location` | PEM CA file; otherwise system roots |
| `ssl.certificate.location`, `ssl.key.location` | PEM client certificate and unencrypted private key; supply both |
| `ssl.endpoint.identification.algorithm` | `https` (default) or `none`; `none` disables hostname checks but retains CA-chain checks |
| `client.id` | Optional suffix; source ID is `kaferry-<group>-<suffix>`, other clients `kaferry-<suffix>` |
| `compression.type` | `none` (default), gzip, snappy, lz4, zstd |
| `message.max.bytes` | Client batch ceiling including 4–5 bytes of outer framing, 1,000–1,000,000,000 bytes; default 1,000,000, adjusted by readable destination config |
| `message.timeout.ms`, `delivery.timeout.ms` | Submission deadline; positive milliseconds, aliases must agree; default worker delivery timeout |
| `queue.buffering.max.messages`, `queue.buffering.max.kbytes` | Sink admission bounds: at most worker records / the larger of worker bytes or the message limit (rounded up to KiB) |
| `queued.max.messages.kbytes`, `fetch.max.bytes`, `max.partition.fetch.bytes` | Soft per-fetch ceilings, capped by worker bytes; defaults worker bytes / 50 MiB / 1 MiB |
| `session.timeout.ms`, `heartbeat.interval.ms` | Defaults 45,000 / 3,000; heartbeat must be smaller than session |
| `max.poll.interval.ms` | Group rebalance timeout, default 300,000; at least session timeout and 30,000. Not an application poll watchdog |
| `request.timeout.ms` | Produce request timeout, default 30,000; not the record-delivery deadline |
| `socket.timeout.ms` | Request timeout overhead, default 2,000; 100–900,000; not a total-operation deadline |
| `linger.ms` | Batch wait, default 5; 0–60,000 |
| `metadata.max.age.ms`, `topic.metadata.refresh.interval.ms` | Metadata refresh age, default 300,000; aliases must agree; 10–3,600,000 |

Queue/fetch overrides must be positive. Byte queue settings use KiB. Large or compressed batches can exceed fetch ceilings; these do not cap total client memory. The ordinary consumer runs one fetch request at a time; separate requests checking offset gaps can overlap it. Kaferry's uncommitted-work caps remain independent.

`partitioner` is unsupported; select the copy's placement with destination `WITH (PARTITIONING = PRESERVE|KEY)`. Auto-commit/reset, group ID/protocol, read isolation, cooperative assignment, auto topic creation, idempotence, acknowledgements and delivery handling are owned by Kaferry. Kaferry sets these; the corresponding property names are rejected as unsupported. Sources use the SQL group, classic protocol, read-committed and no automatic offset reset. Producers use idempotence and all-ISR acknowledgements. Topic auto-creation is disabled.

### CLI and exit codes

`run`, `explain` and `status` accept exactly one input: `--file/-f path`, `--sql/-e statement`, or `--file -`. `--config/-c path` loads TOML. `explain` and `status` write text or `--json` to stdout; `run` keeps stdout empty. Events are JSON on stderr; errors also print a plain-text message after cleanup. Status verdicts use stdout, including nonzero verdicts. Shutdown timeout diagnostics name the operation still running.

| Normal exit | Meaning |
| --- | --- |
| 0 | Status is `stopped`, or the preview printed, or this worker finished its file input or remaining Kafka partitions, or reached its worker record threshold. A signal cleanly stopped a Kafka copy without an enabled completion rule or worker record threshold. SKIP counts do not change the code |
| 1 | Copy, inspection, output, cleanup or the shutdown deadline failed, or a signal arrived before completion of startup, a file copy, or a Kafka copy with an enabled completion rule or worker record threshold. Conflicts after the first consumed record also exit 1 |
| 2 | Usage/parse/plan/configuration error, or a validation conflict before the first consumed record (Kafka topic validation, query/source identity, replay checkpoint/lock or self-copy) |
| 3 | For a Kafka copy with an enabled completion rule or worker record threshold, Kafka assigned this worker no partitions, and none arrived within `worker.idle_exit` (default 30 seconds). This does not say whether other workers finished |
| 4 | Status is `incomplete`: a partition lacks a saved stop |
| 5 | Status is `unknown`: progress is absent, invalid or incompatible, or the group has an incompatible active member |
| 6 | Status is `running`: group activity or a change was observed |

A busy replay-group lock exits 2. Errors while writing, publishing or recovering archive data exit 1. Startup Kafka topic-validation conflicts (for example, topic authorization denied) exit 2. The first failure determines the code; cleanup cannot replace it. A second signal or external forced termination can use system signal status.

A cleanup error can occur after all records are copied and progress is saved; the error names the close operation. Resume using the same named group to avoid starting over. `ONE_OFF` always creates a new group, so rerunning it can copy records again.

## Observability

JSON `progress` logs on stderr report local counters and buffer usage every `worker.progress_interval` (default 30 s; `0` disables). The final `copy summary` is independent of this setting.

Metric export is off by default. [Setup](usage.md#monitoring).

A Collector or compatible backend receives metrics; no scrape listener or Kafka requests are added. `explain` and `status` start no telemetry.

Supported exporter environment suffixes: `ENDPOINT`, `PROTOCOL` (`http/protobuf` only), `HEADERS`, `TIMEOUT`, `INSECURE`, `CERTIFICATE` (CA PEM), `CLIENT_CERTIFICATE` and `CLIENT_KEY` (PEM pair). Prefix them with `OTEL_EXPORTER_OTLP_` or `OTEL_EXPORTER_OTLP_METRICS_`; metric-specific settings take precedence. `INSECURE`, if set, must match the endpoint scheme: `true` for `http`, `false` for `https`. Certificate settings require an HTTPS endpoint. Client certificate/key settings must form a pair under the same prefix. A generic endpoint is a base URL with `/v1/metrics` appended; a metric-specific endpoint is the complete URL. Endpoint credentials, queries and fragments are rejected. Headers use comma-separated `name=value` entries with percent-encoded values.

`OTEL_METRIC_EXPORT_INTERVAL` defaults to 60000 ms; `OTEL_METRIC_EXPORT_TIMEOUT` to 30000 ms; exporter `TIMEOUT` to 10000 ms. Durations use positive integer milliseconds. `[metrics].enabled` is the only enable switch; `OTEL_METRICS_EXPORTER` does not control it. Counters use cumulative totals without exporter retries.

Resources include `service.name=kaferry`, version, a generated `service.instance.id` and `messaging.consumer.group.name`. `OTEL_RESOURCE_ATTRIBUTES` adds deployment/query attributes; built-in identity takes precedence. Automatic attributes exclude SQL, record contents, file paths and credentials. Attributes increase the number of series; use stable deployment values.

Metric names are experimental. All describe this worker's invocation; restarts and rebalances can replay records. Source partitions and topics are not metric attributes.

| Metric (`kaferry.` prefix) | Meaning |
| --- | --- |
| `records.submitted`, `records.confirmed` | Separate cumulative counts accepted and acknowledged by the destination; not unique records |
| `records.filtered`, `records.skipped` | Separate cumulative counts of examined records excluded by timestamp or offset filters, and dropped by error policy |
| `buffer.entries`, `buffer.entry_limit` | Unsaved tracked progress entries and their worker cap; entries can include cursor gaps and exclude a record deferred before tracking |
| `buffer.estimated_size`, `buffer.estimated_size_limit` | Tracked cap-accounting bytes and their cap; not payload/wire bytes or total memory |
| `partitions.held`, `partitions.stopped` | Current held partitions and held partitions at their stop rule, split by reason: `timestamp`, `tip`, `eof`, or `worker_record_cap`; not group completion |
| `progress.saves` | Nonempty save operations, including startup and metadata-only writes; `outcome=success|partial|failure` reflects requested partitions |
| `progress.save.duration` | Save duration in seconds |
| `progress.last_success` | Unix seconds of the last nonempty save where every requested partition succeeded; absent until then |
| `loop.iterations` | Completed loop steps, including quiet polls; not successful copying or group health |

Snapshots refresh at most once per second and once at exit. Gauges may lag current state; metric delivery is best effort. Invalid transport settings fail before output opens. Receiver failures produce a redacted, rate-limited warning and copying continues. Cleanup attempts final export for at most one second within the remaining process deadline. Lost exports, short runs, crashes and receiver outages can leave missing metrics; saved source progress is unaffected. Lag sampling and an HTTP health endpoint are not provided.
